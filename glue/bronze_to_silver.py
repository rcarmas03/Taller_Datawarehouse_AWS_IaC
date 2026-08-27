import sys
import datetime as dt
import pytz
import logging

from pyspark.sql import functions as F
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext


# ============================================================
# LOGGER
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)

# ============================================================
# SPARK / GLUE
# ============================================================

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session

job = Job(glue_context)

# ============================================================
# JOB PARAMETERS
# ============================================================

args = getResolvedOptions(
    sys.argv,
    [
        "S3_RAW_PATH",
        "S3_SILVER_PATH"
    ]
)


S3_PATH_RAW = args["S3_RAW_PATH"]
S3_PATH_SILVER = args["S3_SILVER_PATH"]


# ============================================================
# GENERAL PARAMETERS
# ============================================================

TZ_HOUSTON = pytz.timezone("America/Chicago")

NOW_HOUSTON = (
    dt.datetime
    .now(pytz.utc)
    .astimezone(TZ_HOUSTON)
)


logger.info("==============================================")
logger.info("Starting BRONZE TO SILVER job")
logger.info("Execution timestamp: %s", NOW_HOUSTON)
logger.info("RAW path: %s", S3_PATH_RAW)
logger.info("SILVER path: %s", S3_PATH_SILVER)
logger.info("==============================================")


# ============================================================
# READ RAW DATA
# ============================================================

try:

    logger.info("Reading RAW sales data")

    df_sales = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(S3_PATH_RAW)
    )

    logger.info(
        "RAW records: %s",
        df_sales.count()
    )

except Exception as e:

    logger.error("Error reading RAW sales data")
    logger.error(e)

    raise


# ============================================================
# TRANSFORMATION
# ============================================================

try:

    logger.info("Starting transformations")

    # --------------------------------------------------------
    # Remove duplicated sales
    # --------------------------------------------------------

    df_sales = (
        df_sales
        .dropDuplicates(["sale_id"])
    )


    # --------------------------------------------------------
    # Cast columns
    # --------------------------------------------------------

    df_sales = (
        df_sales

        .withColumn(
            "sale_id",
            F.col("sale_id").cast("integer")
        )

        .withColumn(
            "sale_date",
            F.to_date(
                F.col("sale_date"),
                "yyyy-MM-dd"
            )
        )

        .withColumn(
            "customer_id",
            F.col("customer_id").cast("integer")
        )

        .withColumn(
            "product_id",
            F.col("product_id").cast("integer")
        )

        .withColumn(
            "store_id",
            F.col("store_id").cast("integer")
        )

        .withColumn(
            "quantity",
            F.col("quantity").cast("integer")
        )

        .withColumn(
            "unit_price",
            F.col("unit_price").cast("decimal(12,2)")
        )

        .withColumn(
            "total_amount",
            F.col("total_amount").cast("decimal(14,2)")
        )
    )


    # --------------------------------------------------------
    # Remove invalid records
    # --------------------------------------------------------

    df_sales = (
        df_sales

        .filter(
            F.col("sale_id").isNotNull()
        )

        .filter(
            F.col("sale_date").isNotNull()
        )

        .filter(
            F.col("customer_id").isNotNull()
        )

        .filter(
            F.col("product_id").isNotNull()
        )

        .filter(
            F.col("store_id").isNotNull()
        )

        .filter(
            F.col("quantity") > 0
        )

        .filter(
            F.col("unit_price") >= 0
        )

        .filter(
            F.col("total_amount") >= 0
        )
    )


    # --------------------------------------------------------
    # Recalculate total amount
    # --------------------------------------------------------

    df_sales = (
        df_sales

        .withColumn(
            "calculated_amount",

            (
                F.col("quantity")
                * F.col("unit_price")
            ).cast("decimal(14,2)")
        )
    )


    # --------------------------------------------------------
    # Validate total amount
    # --------------------------------------------------------

    df_sales = (
        df_sales

        .withColumn(
            "amount_valid",

            F.when(
                F.abs(
                    F.col("total_amount")
                    -
                    F.col("calculated_amount")
                ) < F.lit(0.01),

                F.lit(True)

            ).otherwise(
                F.lit(False)
            )
        )
    )


    # --------------------------------------------------------
    # ETL timestamp
    # --------------------------------------------------------

    df_sales = (
        df_sales
        .withColumn(
            "ETL_TSTAMP",
            F.current_timestamp()
        )
    )


    # --------------------------------------------------------
    # Final column selection
    # --------------------------------------------------------

    df_sales = (
        df_sales

        .select(
            "sale_id",
            "sale_date",
            "customer_id",
            "product_id",
            "store_id",
            "quantity",
            "unit_price",
            "total_amount",
            "calculated_amount",
            "amount_valid",
            "ETL_TSTAMP"
        )
    )


    logger.info(
        "SILVER records after transformation: %s",
        df_sales.count()
    )


except Exception as e:

    logger.error(
        "Error transforming RAW sales data"
    )

    logger.error(e)

    raise


# ============================================================
# WRITE SILVER
# ============================================================

try:

    logger.info("Writing SILVER data")

    (
        df_sales
        .write
        .mode("overwrite")
        .format("parquet")
        .save(S3_PATH_SILVER)
    )

    logger.info(
        "SILVER data successfully written to: %s",
        S3_PATH_SILVER
    )

except Exception as e:

    logger.error(
        "Error writing SILVER data"
    )

    logger.error(e)

    raise


# ============================================================
# JOB FINISH
# ============================================================

logger.info("==============================================")
logger.info("BRONZE TO SILVER completed successfully")
logger.info("==============================================")


job.commit()