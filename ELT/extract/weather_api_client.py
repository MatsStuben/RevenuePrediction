import requests
import pandas as pd
from datetime import datetime, date, timedelta
from typing import List, Optional, Tuple
from pathlib import Path


def _chunk_into_5y_periods(start_date: str, end_date: str) -> List[Tuple[str, str]]:
    """
    Split [start_date, end_date] (YYYY-MM-DD) into contiguous periods,
    each at most 5 years long (inclusive).
    """
    start = datetime.strptime(start_date, "%Y-%m-%d").date()
    end = datetime.strptime(end_date, "%Y-%m-%d").date()

    if end < start:
        raise ValueError("end_date must be on or after start_date")

    periods: List[Tuple[str, str]] = []
    cursor = start

    while cursor <= end:
        chunk_upper = cursor.replace(year=cursor.year + 5) - timedelta(days=1)
        chunk_end = min(chunk_upper, end)
        periods.append((cursor.isoformat(), chunk_end.isoformat()))
        cursor = chunk_end + timedelta(days=1)

    return periods

def fetch_weather_data(
    client_id: str,
    endpoint: str,
    station_id: str,
    elements: List[str],
    start_date: str,
    end_date: str,
    output_csv: Optional[str] = None,
) -> Optional[pd.DataFrame]:
    """
    Fetch weather data from Frost API for a station within [start_date, end_date],
    automatically chunked into ≤5-year periods to avoid large responses.
    """
    periods = _chunk_into_5y_periods(start_date, end_date)
    all_data: List[pd.DataFrame] = []

    for start, end in periods:
        print(f"Fetching data from {start} to {end}...")
        params = {
            "sources": station_id,
            "elements": ",".join(elements),
            "referencetime": f"{start}/{end}",
        }

        response = requests.get(endpoint, params=params, auth=(client_id, ""))
        if response.status_code == 200:
            data = response.json().get("data", [])
            if data:
                df = pd.json_normalize(data, "observations", ["referenceTime", "sourceId"])
                df.rename(columns={"referenceTime": "date"}, inplace=True)
                df["date"] = pd.to_datetime(df["date"]).dt.date
                all_data.append(df)
            else:
                print(f"No data found for {start}–{end}")
        else:
            print(f"Error {response.status_code}: {response.text}")

    if not all_data:
        print("No data collected.")
        return None

    df_all = pd.concat(all_data, ignore_index=True)

    df_all = df_all.pivot_table(
        index=["date", "sourceId"],
        columns="elementId",
        values="value",
        aggfunc="first"
    ).reset_index()

    if output_csv:
        data_dir = Path(__file__).resolve().parents[1].parent / "Data"
        data_dir.mkdir(parents=True, exist_ok=True) 
        output_path = data_dir / output_csv

        df_all.to_csv(output_path, index=False)
        print(f"Saved full dataset to {output_path}")

    return df_all

if __name__ == "__main__":
    client_id = "c522a0ba-ceb8-4df7-8f04-5e19f0a12eff"
    endpoint = "https://frost.met.no/observations/v0.jsonld"
    station_id = "SN18950"
    elements = [
        "surface_snow_thickness",
        "sum(precipitation_amount P1D)",
        "min(air_temperature P1D)",
        "max(air_temperature P1D)",
        "mean(air_temperature P1D)",
    ]

    df = fetch_weather_data(
        client_id=client_id,
        endpoint=endpoint,
        station_id=station_id,
        elements=elements,
        start_date="2011-01-01",
        end_date="2025-08-18",
        output_csv="tryvann_daily_weather.csv"
    )

    if df is not None:
        print(df.head())