import argparse
import os
from datetime import datetime

import requests

from ingest_ragflow.dspace_api.autentification import authenticate_user
from ingest_ragflow.dspace_api.collections import generate_collection_stats

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--email", required=True, help="Email for authentication")
    ap.add_argument("--password", required=True, help="Password for authentication")
    ap.add_argument("--output", required=True, help="Output csv path")

    args = vars(ap.parse_args())

    # Configuration
    BASE_URL = "https://repositorioinstitucional.uaslp.mx/"
    BASE_URL_REST = "https://repositorioinstitucional.uaslp.mx/rest"
    EMAIL = args["email"]
    PASSWORD = args["password"]
    OUTPUT_CSV_PATH = args["output"]

    os.makedirs(OUTPUT_CSV_PATH, exist_ok=True)

    session = requests.Session()
    authenticate_user(session, EMAIL, PASSWORD, BASE_URL_REST)

    df = generate_collection_stats(session, BASE_URL_REST)

    # Get current date in YYYYMMDD format
    date_str = datetime.now().strftime("%Y%m%d")
    output_path = os.path.join(
        OUTPUT_CSV_PATH,
        f"\
                               collection_stats_{date_str}.csv",
    )

    df.to_csv(output_path, index=False)

    print(f"Statistics have been saved to {output_path}")
    print(df)
