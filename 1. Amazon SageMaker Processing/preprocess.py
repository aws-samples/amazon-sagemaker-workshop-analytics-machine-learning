
import glob
import logging
import os
import subprocess
import sys
subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'sagemaker'])

from zipfile import ZipFile
# from time import gmtime, strftime
import socket
import shutil
import json
import time
import argparse
import boto3
import uuid

n_cores = os.cpu_count()
# host_name = socket.gethostname()
# print(host_name)
# print(os.environ)

# Install geopandas dependency before including pandas
subprocess.check_call([sys.executable, "-m", "pip", "install", "geopandas==0.9.0"])

import pandas as pd  # noqa: E402
import geopandas as gpd  # noqa: E402
from sklearn.model_selection import train_test_split  # noqa: E402

import sagemaker
from sagemaker.feature_store.feature_group import FeatureGroup

def get_session(region, default_bucket):
    """Gets the sagemaker session based on the region.
    """

    boto_session = boto3.Session(region_name=region)

    sagemaker_client = boto_session.client("sagemaker")
    return sagemaker.session.Session(
        boto_session=boto_session,
        sagemaker_client=sagemaker_client,
        default_bucket=default_bucket,
    )


logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())

def parse_args() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('--ingest_featuregroup_name', type=str, default=None)
    parser.add_argument('--region', type=str)
    parser.add_argument('--bucket', type=str)
    parser.add_argument('--base_dir', type=str, default="/opt/ml/processing")
    args, _ = parser.parse_known_args()
    return args

def extract_zones(zones_file: str, zones_dir: str):
    logger.info(f"Extracting zone file: {zones_file}")
    with ZipFile(zones_file, "r") as zip:
        zip.extractall(zones_dir)


def load_zones(zones_dir: str):
    logging.info(f"Loading zones from {zones_dir}")
    # Load the shape file and get the geometry and lat/lon
    zone_df = gpd.read_file(os.path.join(zones_dir, "taxi_zones.shp"))
    # Get centroids as EPSG code of 3310 to measure distance
    zone_df["centroid"] = zone_df.geometry.centroid.to_crs(epsg=3310)
    # Convert cordinates to the WSG84 lat/long CRS has a EPSG code of 4326.
    zone_df["latitude"] = zone_df.centroid.to_crs(epsg=4326).x
    zone_df["longitude"] = zone_df.centroid.to_crs(epsg=4326).y
    return zone_df


def load_data(file_list: list):
    # Define dates, and columns to use
    use_cols = [
        "fare_amount",
        "lpep_pickup_datetime",
        "lpep_dropoff_datetime",
        "passenger_count",
        "PULocationID",
        "DOLocationID",
    ]
    # Concat input files with select columns
    dfs = []
    for file in file_list:
        dfs.append(pd.read_csv(file, usecols=use_cols))
    return pd.concat(dfs, ignore_index=True)


def enrich_data(trip_df: pd.DataFrame, zone_df: pd.DataFrame):
    # Join trip DF to zones for poth pickup and drop off locations
    trip_df = gpd.GeoDataFrame(
        trip_df.join(zone_df, on="PULocationID").join(
            zone_df, on="DOLocationID", rsuffix="_DO", lsuffix="_PU"
        )
    )
    trip_df["geo_distance"] = (
        trip_df["centroid_PU"].distance(trip_df["centroid_DO"]) / 1000
    )

    # Add date parts
    trip_df["lpep_pickup_datetime"] = pd.to_datetime(trip_df["lpep_pickup_datetime"])
    trip_df["hour"] = trip_df["lpep_pickup_datetime"].dt.hour
    trip_df["weekday"] = trip_df["lpep_pickup_datetime"].dt.weekday
    trip_df["month"] = trip_df["lpep_pickup_datetime"].dt.month

    # Get calculated duration in minutes
    trip_df["lpep_dropoff_datetime"] = pd.to_datetime(trip_df["lpep_dropoff_datetime"])
    trip_df["duration_minutes"] = (
        trip_df["lpep_dropoff_datetime"] - trip_df["lpep_pickup_datetime"]
    ).dt.seconds / 60

    # Rename and filter cols
    trip_df = trip_df.rename(
        columns={
            "latitude_PU": "pickup_latitude",
            "longitude_PU": "pickup_longitude",
            "latitude_DO": "dropoff_latitude",
            "longitude_DO": "dropoff_longitude",
        }
    )
    
    trip_df['FS_ID'] = trip_df.index + 1000
    current_time_sec = int(round(time.time()))
    trip_df["FS_time"] = pd.Series([current_time_sec]*len(trip_df), dtype="float64")
    return trip_df


def clean_data(trip_df: pd.DataFrame):
    # Remove outliers
    trip_df = trip_df[
        (trip_df.fare_amount > 0)
        & (trip_df.fare_amount < 200)
        & (trip_df.passenger_count > 0)
        & (trip_df.duration_minutes > 0)
        & (trip_df.duration_minutes < 120)
        & (trip_df.geo_distance > 0)
        & (trip_df.geo_distance < 121)
    ].dropna()

    # Filter columns
    cols = [
        "fare_amount",
        "passenger_count",
        "pickup_latitude",
        "pickup_longitude",
        "dropoff_latitude",
        "dropoff_longitude",
        "geo_distance",
        "hour",
        "weekday",
        "month",
    ]
    
    cols_fg = [
        "fare_amount",
        "passenger_count",
        "pickup_latitude",
        "pickup_longitude",
        "dropoff_latitude",
        "dropoff_longitude",
        "geo_distance",
        "hour",
        "weekday",
        "month",
        "FS_ID",
        "FS_time"
    ]
    return trip_df[cols], trip_df[cols_fg]

def ingest_data(data_fg: pd.DataFrame, fg_name: str, sagemaker_session) -> None:
    
    # 4 threads per python process
    num_workers = 4
    num_processes = n_cores
    logger.info(f'Ingesting into feature group [{fg_name}] using {num_processes} processes and {num_workers} workers')
    fg = FeatureGroup(name=fg_name, sagemaker_session=sagemaker_session)
    response = fg.ingest(data_frame=data_fg, max_processes=num_processes, max_workers=num_workers, wait=True)
    """
    The ingest call above returns an IngestionManagerPandas instance as a response. Zero based indices of rows 
    that failed to be ingested are captured via failed_rows in this response. By asserting this count to be 0,
    we validated that all rows were successfully ingested without a failure.
    """
    assert len(response.failed_rows) == 0


def save_files(base_dir: str, data_df: pd.DataFrame, data_fg: pd.DataFrame, fg_name: str, 
               val_size=0.2, test_size=0.05, current_host=None, sagemaker_session=None):
        
    logger.info(f"Splitting {len(data_df)} rows of data into train, val, test.")

    train_df, val_df = train_test_split(data_df, test_size=val_size, random_state=42)
    val_df, test_df = train_test_split(val_df, test_size=test_size, random_state=42)

    logger.info(f"Writing out datasets to {base_dir}")
    tmp_id = uuid.uuid4().hex[:8]
    train_df.to_csv(f"{base_dir}/train/train_{current_host}_{tmp_id}.csv", header=False, index=False)
    val_df.to_csv(f"{base_dir}/validation/validation_{current_host}_{tmp_id}.csv", header=False, index=False)

    # Save test data without header
    test_df.to_csv(f"{base_dir}/test/test_{current_host}_{tmp_id}.csv", header=False, index=False)

    
    if fg_name:
        # batch ingestion to the feature group of all the data
        ingest_data(data_fg, fg_name, sagemaker_session)

    return

def _read_json(path):  # type: (str) -> dict
    """Read a JSON file.
    Args:
        path (str): Path to the file.
    Returns:
        (dict[object, object]): A dictionary representation of the JSON file.
    """
    with open(path, "r") as f:
        return json.load(f)

def main(base_dir: str, args: argparse.Namespace):
    # Input data files
    input_dir = os.path.join(base_dir, "input/data")
    input_file_list = glob.glob(f"{input_dir}/*.csv")
    logger.info(f"Input file list: {input_file_list}")

    hosts = _read_json("/opt/ml/config/resourceconfig.json")
    logger.info(hosts)
    current_host = hosts["current_host"]
    logger.info(current_host)
        
    if len(input_file_list) == 0:
        raise Exception(f"No input files found in {input_dir}")

    # Input zones file
    zones_dir = os.path.join(base_dir, "input/zones")
    zones_file = os.path.join(zones_dir, "taxi_zones.zip")
    if not os.path.exists(zones_file):
        raise Exception(f"Zones file {zones_file} does not exist")

    # Extract and load taxi zones geopandas dataframe
    extract_zones(zones_file, zones_dir)
    zone_df = load_zones(zones_dir)

    # Load input files
    data_df = load_data(input_file_list)
    data_df = enrich_data(data_df, zone_df)
    data_df, data_fg = clean_data(data_df)
    
    fg_name = args.ingest_featuregroup_name
    
    sagemaker_session = get_session(args.region, args.bucket)
    
    return save_files(base_dir, data_df, data_fg, fg_name, current_host=current_host, sagemaker_session=sagemaker_session)


if __name__ == "__main__":
    logger.info("Starting preprocessing.")
    args = parse_args()
    base_dir = args.base_dir
    main(base_dir, args)
    logger.info("Done")
