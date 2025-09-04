import argparse
import os
from datetime import datetime

from ingest_ragflow.dspace_api.items import generate_item_stats

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--output", required=True, help="Output csv path")

    args = vars(ap.parse_args())

    # Configuration
    BASE_URL = "https://repositorioinstitucional.uaslp.mx/"
    BASE_URL_REST = "https://repositorioinstitucional.uaslp.mx/rest"
    OUTPUT_CSV_PATH = args["output"]

    os.makedirs(OUTPUT_CSV_PATH, exist_ok=True)

    df = generate_item_stats(BASE_URL_REST)

    # Get current date in YYYYMMDD format
    date_str = datetime.now().strftime("%Y%m%d")
    output_path = os.path.join(
        OUTPUT_CSV_PATH,
        f"/items_stats_{date_str}.csv",
    )

    df.to_csv(output_path, index=False)

    print(f"Statistics have been saved to {output_path}")
    print(df)
