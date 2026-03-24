from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, LongType
from pyspark.errors.exceptions.captured import AnalysisException
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
        datetime.datetime.now(),
        namespace,
        num_registros
    )]
    
    df_audit = spark.createDataFrame(row, schema)
    
    try:
        df_audit.writeTo(f"players.{namespace}.audit_log").append()
    except AnalysisException as e:
            if "TABLE_OR_VIEW_NOT_FOUND" in str(e) or "Table not found" in str(e):
                df_audit.writeTo(f"players.{namespace}.audit_log") \
                    .tableProperty("format-version", "2") \
                    .create()
                
    