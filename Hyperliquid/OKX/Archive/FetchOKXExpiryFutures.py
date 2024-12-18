import requests
import json
from datetime import datetime


def fetch_okx_futures():
    url = "https://www.okx.com/api/v5/public/instruments"
    params = {
        "instType": "FUTURES",
    }

    response = requests.get(url, params=params)
    data = response.json()

    if data['code'] == '0':
        instruments = data['data']

        # Sort by expiry time
        instruments.sort(key=lambda x: x['expTime'])

        print("\nOKX Futures Instruments:")
        print(f"{'Symbol':<15} {'InstID':<20} {'Expiry':<20} {'Status'}")
        print("-" * 70)

        for inst in instruments:
            # Convert expiry timestamp to readable date
            expiry_date = datetime.fromtimestamp(int(inst['expTime']) / 1000).strftime('%Y-%m-%d %H:%M:%S')

            print(f"{inst['uly']:<15} {inst['instId']:<20} {expiry_date:<20} {inst['state']}")


if __name__ == "__main__":
    fetch_okx_futures()