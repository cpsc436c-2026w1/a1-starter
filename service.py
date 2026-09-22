"""CPSC 436C A1 — rainfall summary service.

One stats function, two deployments:
  * FastAPI app  -> `app`            (always-on instance)
  * Lambda entry -> `lambda_handler` (event-driven)

Both call summarise(), so the answers are identical by construction.
"""
import os, io, json, time
import pandas as pd

DATA_PATH = os.environ.get("DATA_PATH", "rainfall_daily.parquet")
DATA_URI = os.environ.get("DATA_URI")           # s3://bucket/key, used on Lambda

_df = None
_load_ms = None


def load_data():
    """Read the Parquet into memory. On Lambda this runs once per cold start."""
    global _df, _load_ms
    if _df is not None:
        return _df
    t0 = time.perf_counter()
    if DATA_URI:
        import boto3
        bucket, key = DATA_URI[5:].split("/", 1)
        body = boto3.client("s3").get_object(Bucket=bucket, Key=key)["Body"].read()
        _df = pd.read_parquet(io.BytesIO(body))
    else:
        _df = pd.read_parquet(DATA_PATH)
    _load_ms = (time.perf_counter() - t0) * 1000
    print(f"data loaded: {len(_df):,} rows in {_load_ms:.0f} ms")
    return _df


def summarise(start, end):
    """Rainfall statistics per climate model, over the dates start to end."""
    d = load_data()
    window = d[(d["time"] >= pd.Timestamp(start)) & (d["time"] <= pd.Timestamp(end))]
    g = window.groupby("model", observed=True)["rain_mm"]
    out = pd.DataFrame({
        "mean_mm": g.mean().round(4),
        "max_mm": g.max().round(4),
        "pct_days_over_10mm": (window.assign(wet=window["rain_mm"] > 10)
                               .groupby("model", observed=True)["wet"].mean() * 100).round(2),
        "n_rows": g.size(),
    }).sort_values("mean_mm").reset_index()
    return {"start": start, "end": end, "n_models": len(out),
            "models": out.to_dict("records")}


# --------------------------------------------------------------- FastAPI
try:
    from fastapi import FastAPI, Query
    app = FastAPI(title="436C A1 rainfall summary")

    @app.on_event("startup")
    def _warm():
        load_data()

    @app.get("/summary")
    def summary(start: str = Query(...), end: str = Query(...)):
        return summarise(start, end)

    @app.get("/health")
    def health():
        return {"ok": True, "rows": len(load_data()), "load_ms": round(_load_ms)}
except ImportError:
    app = None


# --------------------------------------------------------------- Lambda
def _http(event):
    q = (event or {}).get("queryStringParameters") or {}
    return q.get("start", "1990-01-01"), q.get("end", "1999-12-31")


def _s3(event):
    import boto3
    rec = event["Records"][0]["s3"]
    bucket, key = rec["bucket"]["name"], rec["object"]["key"]
    s3 = boto3.client("s3")
    req = json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read())
    return req.get("start", "1990-01-01"), req.get("end", "1999-12-31"), bucket, key, s3


def lambda_handler(event, context):
    cold = _df is None

    # S3 events and API Gateway events look nothing alike, so branch on shape.
    if isinstance(event, dict) and "Records" in event:
        start, end, bucket, key, s3 = _s3(event)
        body = summarise(start, end)
        body["cold_start"] = cold
        body["source_key"] = key
        out_key = "results/" + key.split("/", 1)[-1]
        s3.put_object(Bucket=bucket, Key=out_key,
                      Body=json.dumps(body, indent=1).encode(),
                      ContentType="application/json")
        print(f"wrote s3://{bucket}/{out_key}")
        return {"written": out_key, "cold_start": cold}

    start, end = _http(event)
    body = summarise(start, end)
    body["cold_start"] = cold
    return {"statusCode": 200,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps(body)}
