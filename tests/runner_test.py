import gzip
import json
import os
from typing import Generator

import pytest
from pyspark.errors import AnalysisException
from pyspark.sql import SparkSession, functions as f, types as t

from src.runner import to_df, load_property, load_property_day


@pytest.fixture(scope="session")
def spark() -> Generator[SparkSession, None, None]:
    s = SparkSession.builder.appName("Runner").master("local[*]").getOrCreate()
    yield s
    s.stop()


def test_to_df_csv(spark, tmp_path):
    # Create a temporary CSV file
    csv_content = "id,name\n1,John Doe\n2,Jane Doe"
    csv_path = tmp_path / "test.csv"
    csv_path.write_text(csv_content)

    schema = t.StructType([
        t.StructField("id", t.IntegerType(), True),
        t.StructField("name", t.StringType(), True)
    ])

    df = to_df(spark, "csv", schema, str(csv_path))
    assert df.count() == 2
    assert df.schema == schema


def test_to_df_json(spark, tmp_path):
    # Create a temporary JSON file
    json_content = '[{"id": 1, "name": "John Doe"}, {"id": 2, "name": "Jane Doe"}]'
    json_path = tmp_path / "test.json"
    json_path.write_text(json_content)

    schema = t.StructType([
        t.StructField("id", t.IntegerType(), True),
        t.StructField("name", t.StringType(), True)
    ])

    df = to_df(spark, "json", schema, str(json_path))
    assert df.count() == 2
    assert df.schema == schema


def test_to_df_invalid_path(spark):
    schema = t.StructType([
        t.StructField("id", t.IntegerType(), True),
        t.StructField("name", t.StringType(), True)
    ])

    with pytest.raises(Exception):
        to_df(spark, "csv", schema, "non_existent_path.csv")


def test_load_property_valid_csv(spark, tmp_path):
    # Create a temporary gzipped CSV file
    csv_content = """property_id,city_name,state_name,country_name,bedrooms,bathrooms,first_observed_dt,listing_type,latitude,longitude,rating_overall
    1,New York,NY,USA,3,2.0,2021-01-01,sale,40.7128,-74.0060,4
    2,Los Angeles,CA,USA,4,3.5,2021-02-01,rent,34.0522,-118.2437,5
    3,Chicago,IL,USA,2,1.0,2021-03-01,sale,41.8781,-87.6298,3"""

    csv_path = tmp_path / "property.csv.gz"
    with gzip.open(csv_path, 'wt') as f:
        f.write(csv_content)

    output_path = tmp_path / "output"
    os.makedirs(output_path)

    load_property(spark, in_path=str(csv_path), out_path=str(output_path))

    # Check if parquet file is created
    parquet_path = output_path / "property"
    assert parquet_path.exists()

    df = spark.read.parquet(str(parquet_path))
    assert df.count() == 3
    assert "valid" in df.columns
    assert "size" in df.columns

    # Verify the "size" and "valid" columns
    sizes = df.select("size").collect()
    valids = df.select("valid").collect()
    assert sizes == [("MEDIUM",), ("LARGE",), ("MEDIUM",)]
    assert valids == [(True,), (True,), (True,)]


def test_load_property_missing_columns(spark, tmp_path):
    # Create a temporary gzipped CSV file with missing columns
    csv_content = """property_id,city_name,state_name,country_name,bedrooms,first_observed_dt,listing_type,latitude,longitude
    1,New York,NY,USA,3,2021-01-01,sale,40.7128,-74.0060"""

    csv_path = tmp_path / "property.csv.gz"
    with gzip.open(csv_path, 'wt') as f:
        f.write(csv_content)

    output_path = tmp_path / "output"
    os.makedirs(output_path)

    with pytest.raises(Exception):
        load_property(spark, in_path=str(csv_path), out_path=str(output_path))


def test_load_property_day_valid_json(spark, tmp_path):
    # Create a temporary gzipped JSON file
    json_content = [
        {
            "property_id": "1",
            "calendar_dt": "2021-01-01",
            "status": "available",
            "price": "100",
            "reservation_id": None,
            "booking_dt": None,
            "start_dt": None
        },
        {
            "property_id": "2",
            "calendar_dt": "2021-02-01",
            "status": "reserved",
            "price": "200",
            "reservation_id": "r1",
            "booking_dt": "2021-01-15",
            "start_dt": "2021-02-01"
        }
    ]
    json_path = tmp_path / "property_day.json.gz"
    with gzip.open(json_path, 'wt') as out_file:
        json.dump(json_content, out_file)

    output_path = tmp_path / "output"
    os.makedirs(output_path)

    load_property_day(spark, in_path=str(json_path), out_path=str(output_path))

    # Check if parquet file is created
    parquet_path = output_path / "property_day"
    assert parquet_path.exists()

    df = spark.read.parquet(str(parquet_path))
    assert df.count() == 2
    assert df.filter(f.col("price").isNotNull()).count() == 2


def test_load_property_day_invalid_records(spark, tmp_path):
    # Create a temporary gzipped JSON file with invalid records
    json_content = [
        {
            "property_id": "1",
            "calendar_dt": "2021-01-01",
            "status": "available",
            "price": "100",
            "reservation_id": None,
            "booking_dt": None,
            "start_dt": None
        },
        {
            "property_id": "2",
            "calendar_dt": "2021-02-01",
            "status": "available",
            "price": "200",
            "reservation_id": "r1",
            "booking_dt": "2021-01-15",
            "start_dt": "2021-02-01"
        }
    ]
    json_path = tmp_path / "property_day.json.gz"
    with gzip.open(json_path, 'wt') as out_file:
        json.dump(json_content, out_file)

    output_path = tmp_path / "output"
    os.makedirs(output_path)

    with pytest.raises(ValueError, match="Invalid records found!"):
        load_property_day(spark, in_path=str(json_path), out_path=str(output_path))
