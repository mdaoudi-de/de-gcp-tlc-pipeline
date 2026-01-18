from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path

import yaml

from de_pipeline.ingestion.downloader import download_file
from de_pipeline.ingestion.source import TLCSources, month_start_n_months_back


def main() -> None:
    with open("config/dataset.yml", encoding="utf-8") as f:
        dataset_cfg = yaml.safe_load(f)
    with open("config/runtime.yml", encoding="utf-8") as f:
        runtime_cfg = yaml.safe_load(f)

    src_cfg = dataset_cfg["dataset"]["source"]
    months_back = int(dataset_cfg["dataset"]["default_range"]["months_back"])
    local_prefix = dataset_cfg["raw_conventions"]["local_prefix"]
    local_raw_dir = Path(runtime_cfg["runtime"]["local_raw_dir"])
    timeout_sec = int(runtime_cfg["runtime"]["request_timeout_sec"])
    chunk_size = int(runtime_cfg["runtime"]["chunk_size_bytes"])

    ingestion_date = datetime.now(UTC).strftime(runtime_cfg["runtime"]["ingestion_date_format"])

    src = TLCSources(
        base_url=src_cfg["base_url"],
        trips_path_template=src_cfg["trips_path_template"],
        zones_path=src_cfg["zones_path"],
    )

    # 1) zones lookup (small csv)
    zones_url = src.zones_url()
    zones_dest = local_raw_dir / local_prefix / f"ingestion_date={ingestion_date}" / "taxi_zone_lookup.csv"
    download_file(zones_url, zones_dest, timeout_sec=timeout_sec, chunk_size=chunk_size)

    # 2) trips parquet (monthly)
    for y, m in month_start_n_months_back(date.today(), months_back):
        url = src.trip_url(y, m)
        filename = f"green_tripdata_{y:04d}-{m:02d}.parquet"
        dest = local_raw_dir / local_prefix / f"ingestion_date={ingestion_date}" / filename
        try:
            download_file(url, dest, timeout_sec=timeout_sec, chunk_size=chunk_size)
        except Exception as e:
            print(f"⚠️  Skipped {y:04d}-{m:02d}: {e}")


if __name__ == "__main__":
    main()
