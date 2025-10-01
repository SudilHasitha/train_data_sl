import sys, glob, pandas as pd

PATTERN = sys.argv[1] if len(sys.argv) > 1 else "data/shard_*/slr_trains_by_route.*.partial.csv"
out_csv = sys.argv[2] if len(sys.argv) > 2 else "slr_trains_by_route.ALL.csv"
out_xlsx = sys.argv[3] if len(sys.argv) > 3 else "slr_trains_by_route.ALL.xlsx"

files = sorted(glob.glob(PATTERN))
if not files:
    print("No shard files found"); raise SystemExit(1)

dfs = [pd.read_csv(f) for f in files]
df = pd.concat(dfs, ignore_index=True).drop_duplicates(
    subset=["train_no","start_station_id","end_station_id"]
)
df.to_csv(out_csv, index=False)
try:
    df.to_excel(out_xlsx, index=False)
except Exception:
    pass
print(f"Merged {len(files)} files → {out_csv} ({len(df)} rows)")
