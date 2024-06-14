import logging
from typing import Literal

from pyspark.sql import DataFrame, SparkSession, functions as f, types as t

logger = logging.getLogger(__name__)


def to_df(spark: SparkSession, input_format: Literal["csv", "json"], schema: t.StructType, path: str) -> DataFrame:
    """
    Reads CSV or JSON files as PySpark DataFrame applying the provided schema.

    :param spark: The SparkSession to use.
    :param input_format: The format of the input file. It can be either "csv" or "json".
    :param schema: The schema of the input file.
    :param path: Path to the input file.
    :return: PySpark DataFrame with the contents of the input file.
    """
    reader = spark.read.format(input_format).schema(schema)
    if input_format == "csv":
        reader = reader.option("header", True).option("enforceSchema", False)
    return reader.load(path=path)


def load_property(spark: SparkSession, in_path: str = "data/property.csv.gz",
                  out_path: str = "output") -> None:
    """
    Loads the property data from the input file and saves it in parquet format.

    :param spark: The SparkSession to use.
    :param in_path: Source file path.
    :param out_path: Output destination.
    :return: None
    """
    schema = t.StructType([
        t.StructField(name="property_id", dataType=t.StringType(), nullable=False),
        t.StructField(name="city_name", dataType=t.StringType(), nullable=False),
        t.StructField(name="state_name", dataType=t.StringType(), nullable=False),
        t.StructField(name="country_name", dataType=t.StringType(), nullable=False),
        t.StructField(name="bedrooms", dataType=t.IntegerType(), nullable=True),
        t.StructField(name="bathrooms", dataType=t.FloatType(), nullable=True),
        t.StructField(name="first_observed_dt", dataType=t.DateType(), nullable=False),
        t.StructField(name="listing_type", dataType=t.StringType(), nullable=False),
        t.StructField(name="latitude", dataType=t.FloatType(), nullable=False),
        t.StructField(name="longitude", dataType=t.FloatType(), nullable=False),
        t.StructField(name="rating_overall", dataType=t.IntegerType(), nullable=True),
    ])
    df = to_df(spark, input_format="csv", schema=schema, path=in_path)

    # Add "valid" column.
    df = df.withColumn(
        "valid",
        f.col("bedrooms").isNotNull() &
        f.col("bathrooms").isNotNull() &
        f.col("rating_overall").isNotNull()
    )

    # Add "size" column.
    df = df.withColumn(
        "size",
        f.when(~f.col("valid"), "UNKNOWN")
        .when((f.col("bedrooms") >= 4) & (f.col("bathrooms") >= 3), "LARGE")
        # The requirement is "bedrooms >= 2 or < 4 and bathrooms >= 1 are MEDIUM" which is probably wrong.
        # It makes more sense "bedrooms >= 2 AND < 4 and bathrooms >= 1 are MEDIUM",
        # i.e. the number of bedrooms should be greater or equal to 2 and strictly smaller than 4.
        .when((f.col("bedrooms") >= 2) & (f.col("bedrooms") < 4) & (f.col("bathrooms") >= 1), "MEDIUM")
        .otherwise("SMALL")
    )

    df.show()
    df.printSchema()
    (df
     .write
     .mode("overwrite")
     .format("parquet")
     .save(path=f"{out_path}/property"))


def load_property_day(spark: SparkSession, in_path: str = "data/property_day.json.gz",
                      out_path: str = "output") -> None:
    """
    Loads the property_day data from the input file and saves it in parquet format.

    :param spark: The SparkSession to use.
    :param in_path: Source file path.
    :param out_path: Output destination.
    :return: None
    """
    schema = t.StructType([
        t.StructField(name="property_id", dataType=t.StringType(), nullable=False),
        t.StructField(name="calendar_dt", dataType=t.DateType(), nullable=False),
        t.StructField(name="status", dataType=t.StringType(), nullable=True),
        t.StructField(name="price", dataType=t.StringType(), nullable=True),
        t.StructField(name="reservation_id", dataType=t.StringType(), nullable=True),
        t.StructField(name="booking_dt", dataType=t.DateType(), nullable=True),
        t.StructField(name="start_dt", dataType=t.DateType(), nullable=True),
    ])
    df = to_df(spark, input_format="json", schema=schema, path=in_path)

    df = df.withColumn("price", f.col("price").cast(t.IntegerType()))

    # Check for invalid records.
    invalid_records = df.filter(f.col("reservation_id").isNotNull() & (f.col("status") != "reserved"))
    if not invalid_records.isEmpty():
        raise ValueError("Invalid records found!")

    df.show()
    df.printSchema()
    (df
     .write
     .mode("append")
     .format("parquet")
     .save(path=f"{out_path}/property_day"))

def main():
    spark = SparkSession.builder.appName("Runner").master("local[*]").getOrCreate()

    load_property(spark=spark)
    try:
        load_property_day(spark=spark)
    except ValueError as e:
        logger.error("Failed to load property_day data!", exc_info=e)

    spark.stop()


if __name__ == "__main__":
    main()
