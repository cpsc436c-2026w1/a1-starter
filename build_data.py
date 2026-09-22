"""Build rainfall_daily.parquet for CPSC 436C A1.

Downloads the partitioned climate-model data from figshare (the same source
DSCI 525 uses), keeps 10 models, and writes one Parquet file of about 118 MB.

    uv run --with pandas --with pyarrow --with requests python build_data.py

Use --source to point at a local copy of combined_model_data_parti.parquet and
skip the download.
"""
import argparse, os, zipfile
import pandas as pd
import pyarrow.dataset as ds

ARTICLE = 14226968                      # figshare: combined model data, partitioned
WANT = "combined_model_data_parti.parquet.zip"
MODELS = ["BCC-ESM1", "CanESM5", "NorESM2-LM", "AWI-ESM-1-1-LR", "MPI-ESM-1-2-HAM",
          "MPI-ESM1-2-LR", "NESM3", "FGOALS-g3", "KIOST-ESM", "INM-CM4-8"]


def download(dest="raw"):
    import requests
    os.makedirs(dest, exist_ok=True)
    meta = requests.get(f"https://api.figshare.com/v2/articles/{ARTICLE}",
                        headers={"Content-Type": "application/json"}).json()
    url = next(f["download_url"] for f in meta["files"] if f["name"] == WANT)
    zpath = os.path.join(dest, WANT)
    if not os.path.exists(zpath):
        print(f"downloading {WANT} ...")
        with requests.get(url, stream=True) as r, open(zpath, "wb") as fh:
            for chunk in r.iter_content(1 << 20):
                fh.write(chunk)
    with zipfile.ZipFile(zpath) as z:
        z.extractall(dest)
    return os.path.join(dest, "combined_model_data_parti.parquet")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", help="local combined_model_data_parti.parquet directory")
    ap.add_argument("--out", default="rainfall_daily.parquet")
    a = ap.parse_args()

    src = a.source or download()
    dataset = ds.dataset(src, format="parquet", partitioning="hive")
    tbl = dataset.to_table(
        columns=["time", "lat_min", "lon_min", "rain (mm/day)", "model"],
        filter=ds.field("model").isin(MODELS))
    df = tbl.to_pandas()
    df = df.rename(columns={"rain (mm/day)": "rain_mm"})
    df["time"] = pd.to_datetime(df["time"]).dt.normalize()   # datetime64, not date
    df["model"] = df["model"].astype("category")
    df = df[["time", "lat_min", "lon_min", "rain_mm", "model"]]
    df.to_parquet(a.out, index=False, compression="snappy")
    print(f"{a.out}: {len(df):,} rows, {os.path.getsize(a.out)/1e6:.1f} MB")


if __name__ == "__main__":
    main()
