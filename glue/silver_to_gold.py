import sys
import datetime as dt
import pytz
import logging

from pyspark.sql import functions as F
from pyspark.sql.window import Window

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
        "S3_SILVER_PATH",
        "S3_GOLD_PATH"
    ]
)


S3_PATH_SILVER = args["S3_SILVER_PATH"]
S3_PATH_GOLD = args["S3_GOLD_PATH"]


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
logger.info("Starting SILVER TO GOLD job")
logger.info("Execution timestamp: %s", NOW_HOUSTON)
logger.info("SILVER path: %s", S3_PATH_SILVER)
logger.info("GOLD path: %s", S3_PATH_GOLD)
logger.info("==============================================")


# ============================================================
# READ SILVER
# ============================================================

try:

    logger.info("Reading SILVER sales data")

    df_sales = (
        spark.read
        .format("parquet")
        .load(S3_PATH_SILVER)
    )

    logger.info(
        "SILVER records: %s",
        df_sales.count()
    )

except Exception as e:

    logger.error(
        "Error reading SILVER sales data"
    )

    logger.error(e)

    raise


# ============================================================
# DIM DATE
# ============================================================

try:

    logger.info("Creating DIM_DATE")

    df_dim_date = (

        df_sales

        .select(
            F.col("sale_date")
        )

        .filter(
            F.col("sale_date").isNotNull()
        )

        .distinct()

        .withColumn(
            "date_id",

            F.date_format(
                F.col("sale_date"),
                "yyyyMMdd"
            ).cast("integer")
        )

        .withColumn(
            "year",
            F.year(F.col("sale_date"))
        )

        .withColumn(
            "month",
            F.month(F.col("sale_date"))
        )

        .withColumn(
            "month_name",
            F.date_format(
                F.col("sale_date"),
                "MMMM"
            )
        )

        .withColumn(
            "quarter",
            F.quarter(F.col("sale_date"))
        )

        .withColumn(
            "day",
            F.dayofmonth(F.col("sale_date"))
        )

        .withColumn(
            "day_of_week",
            F.dayofweek(F.col("sale_date"))
        )

        .withColumn(
            "is_weekend",

            F.when(
                F.dayofweek(
                    F.col("sale_date")
                ).isin(1, 7),

                F.lit(True)

            ).otherwise(
                F.lit(False)
            )
        )

        .withColumn(
            "ETL_TSTAMP",
            F.current_timestamp()
        )

        .select(
            "date_id",
            "sale_date",
            "year",
            "month",
            "month_name",
            "quarter",
            "day",
            "day_of_week",
            "is_weekend",
            "ETL_TSTAMP"
        )
    )


except Exception as e:

    logger.error(
        "Error creating DIM_DATE"
    )

    logger.error(e)

    raise


# ============================================================
# DIM CUSTOMER
# ============================================================

try:

    logger.info("Creating DIM_CUSTOMER")

    df_dim_customer = (

        df_sales

        .select(
            "customer_id"
        )

        .filter(
            F.col("customer_id").isNotNull()
        )

        .distinct()

        .withColumn(
            "customer_name",

            F.concat(
                F.lit("Customer "),
                F.col("customer_id")
            )
        )

        .withColumn(
            "ETL_TSTAMP",
            F.current_timestamp()
        )

        .select(
            "customer_id",
            "customer_name",
            "ETL_TSTAMP"
        )
    )


except Exception as e:

    logger.error(
        "Error creating DIM_CUSTOMER"
    )

    logger.error(e)

    raise


# ============================================================
# DIM PRODUCT
# ============================================================

try:

    logger.info("Creating DIM_PRODUCT")

    df_dim_product = (

        df_sales

        .select(
            "product_id"
        )

        .filter(
            F.col("product_id").isNotNull()
        )

        .distinct()

        .withColumn(
            "product_name",

            F.concat(
                F.lit("Product "),
                F.col("product_id")
            )
        )

        .withColumn(
            "category",
            F.lit("Unknown")
        )

        .withColumn(
            "ETL_TSTAMP",
            F.current_timestamp()
        )

        .select(
            "product_id",
            "product_name",
            "category",
            "ETL_TSTAMP"
        )
    )


except Exception as e:

    logger.error(
        "Error creating DIM_PRODUCT"
    )

    logger.error(e)

    raise


# ============================================================
# DIM STORE
# ============================================================

try:

    logger.info("Creating DIM_STORE")

    df_dim_store = (

        df_sales

        .select(
            "store_id"
        )

        .filter(
            F.col("store_id").isNotNull()
        )

        .distinct()

        .withColumn(
            "store_name",

            F.concat(
                F.lit("Store "),
                F.col("store_id")
            )
        )

        .withColumn(
            "ETL_TSTAMP",
            F.current_timestamp()
        )

        .select(
            "store_id",
            "store_name",
            "ETL_TSTAMP"
        )
    )


except Exception as e:

    logger.error(
        "Error creating DIM_STORE"
    )

    logger.error(e)

    raise


# ============================================================
# FACT SALES
# ============================================================

try:

    logger.info("Creating FACT_SALES")

    df_fact_sales = (

        df_sales

        .withColumn(
            "date_id",

            F.date_format(
                F.col("sale_date"),
                "yyyyMMdd"
            ).cast("integer")
        )

        .withColumn(
            "ETL_TSTAMP",
            F.current_timestamp()
        )

        .select(
            "sale_id",
            "date_id",
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


except Exception as e:

    logger.error(
        "Error creating FACT_SALES"
    )

    logger.error(e)

    raise


# ============================================================
# WRITE DIM DATE
# ============================================================

try:

    logger.info("Writing DIM_DATE")

    (
        df_dim_date
        .write
        .mode("overwrite")
        .format("parquet")
        .save(
            f"{S3_PATH_GOLD}/dim_date"
        )
    )

except Exception as e:

    logger.error(
        "Error writing DIM_DATE"
    )

    logger.error(e)

    raise


# ============================================================
# WRITE DIM CUSTOMER
# ============================================================

try:

    logger.info("Writing DIM_CUSTOMER")

    (
        df_dim_customer
        .write
        .mode("overwrite")
        .format("parquet")
        .save(
            f"{S3_PATH_GOLD}/dim_customer"
        )
    )

except Exception as e:

    logger.error(
        "Error writing DIM_CUSTOMER"
    )

    logger.error(e)

    raise


# ============================================================
# WRITE DIM PRODUCT
# ============================================================

try:

    logger.info("Writing DIM_PRODUCT")

    (
        df_dim_product
        .write
        .mode("overwrite")
        .format("parquet")
        .save(
            f"{S3_PATH_GOLD}/dim_product"
        )
    )

except Exception as e:

    logger.error(
        "Error writing DIM_PRODUCT"
    )

    logger.error(e)

    raise


# ============================================================
# WRITE DIM STORE
# ============================================================

try:

    logger.info("Writing DIM_STORE")

    (
        df_dim_store
        .write
        .mode("overwrite")
        .format("parquet")
        .save(
            f"{S3_PATH_GOLD}/dim_store"
        )
    )

except Exception as e:

    logger.error(
        "Error writing DIM_STORE"
    )

    logger.error(e)

    raise


# ============================================================
# WRITE FACT SALES
# ============================================================

try:

    logger.info("Writing FACT_SALES")

    (
        df_fact_sales
        .write
        .mode("overwrite")
        .format("parquet")
        .save(
            f"{S3_PATH_GOLD}/fact_sales"
        )
    )

except Exception as e:

    logger.error(
        "Error writing FACT_SALES"
    )

    logger.error(e)

    raise


# ============================================================
# JOB FINISH
# ============================================================

logger.info("==============================================")
logger.info("SILVER TO GOLD completed successfully")
logger.info("==============================================")


job.commit()