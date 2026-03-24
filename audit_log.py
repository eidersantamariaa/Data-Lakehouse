from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, LongType
import datetime

def write_audit_log(spark, namespace, table_name, accion, num_registros):
    schema = StructType([
        StructField("tabla", StringType(), False),
        StructField("accion", StringType(), False),
        StructField("timestamp", TimestampType(), False),
        StructField("fuente", StringType(), False),
        StructField("num_registros", LongType(), False),
    ])
    
    row = [(
        table_name,
        accion,
        datetime.now(),
        namespace,
        num_registros
    )]
    
    df_audit = spark.createDataFrame(row, schema)
    
    try:
        df_audit.writeTo(f"players.{namespace}.audit_log").append()
    except Exception:
        df_audit.writeTo(f"players.{namespace}.audit_log") \
            .tableProperty("format-version", "2") \
            .createIfNotExists()