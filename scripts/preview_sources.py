from datetime import date

import yaml

from de_pipeline.ingestion.source import TLCSources, month_start_n_months_back


def main() -> None:
    with open("config/dataset.yml", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    src_cfg = cfg["dataset"]["source"]
    months_back = int(cfg["dataset"]["default_range"]["months_back"])

    src = TLCSources(
        base_url=src_cfg["base_url"],
        trips_path_template=src_cfg["trips_path_template"],
        zones_path=src_cfg["zones_path"],
    )

    print("Zones lookup URL:")
    print(src.zones_url())
    print("\nTrip parquet URLs:")
    for y, m in month_start_n_months_back(date.today(), months_back):
        print(src.trip_url(y, m))


if __name__ == "__main__":
    main()
