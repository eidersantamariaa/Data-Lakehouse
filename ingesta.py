import json
from pyspark.sql import SparkSession
import pyspark

from audit_log import write_audit_log

def get_spark():
    conf = (
        pyspark.SparkConf()
        .setAppName('ingesta')
        .set('spark.jars.packages', 'org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.8.1,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.367')
        #.set('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions')
        .set("spark.sql.catalog.players", "org.apache.iceberg.spark.SparkCatalog")
        .set("spark.sql.catalog.players.type", "rest")
        .set("spark.sql.catalog.players.uri", "http://172.16.58.11:32688")
        .set("spark.sql.catalog.players.warehouse", "s3://warehouse/")
        .set("spark.sql.catalog.players.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .set("spark.sql.catalog.players.s3.endpoint", "http://172.16.58.11:31224")
        .set("spark.sql.catalog.players.s3.region", "us-east-1")
        .set("spark.sql.catalog.players.s3.path-style-access", "true")
        .set("spark.sql.catalog.players.client.region", "us-east-1")
        .set("spark.hadoop.fs.s3a.endpoint", "http://172.16.58.11:31224")
        .set("spark.hadoop.fs.s3a.endpoint.region", "us-east-1")
        .set("spark.hadoop.fs.s3a.access.key", "GK5f421d5f440758f74b0e0312")
        .set("spark.hadoop.fs.s3a.secret.key", "409baa63477885db12cd1db0a518748c5e83e971b5e8cf2129fe6c7498de125d")
        .set("spark.hadoop.fs.s3a.path.style.access", "true")
        .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .set("spark.sql.catalog.players.s3.checksum-algorithm", "NONE")
        .set("spark.sql.catalog.players.s3.streaming-upload-enabled", "false")
        .set("spark.sql.catalog.players.s3.chunked-encoding-enabled", "false")
        .set("spark.sql.catalog.players.s3.payload-signing-enabled", "true")
        .set("spark.sql.catalog.players.s3.http-client-type", "urlconnection")
        .set("spark.executor.extraJavaOptions", "-Daws.requestChecksumCalculation=when_required")
        .set("spark.driver.extraJavaOptions", "-Daws.requestChecksumCalculation=when_required")
        
    )
    return SparkSession.builder.config(conf=conf).getOrCreate()


def run_ingesta(config):
    spark = get_spark()
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS players.{config.NAMESPACE}")

    data = config.get_data()  # {"leagues": [...], "teams": [...], "players": [...]}

    for table_name, records in data.items():
        print(f"Writing {table_name} to Bronze... ({len(records)} records)")
        df = spark.read.json(spark.sparkContext.parallelize(
            [json.dumps(r) for r in records]
        ))
        try:
            df.writeTo(f"players.{config.NAMESPACE}.{table_name}_bronce").append()
            accion = "INSERT"
        except AnalysisException as e:
            if "TABLE_OR_VIEW_NOT_FOUND" in str(e) or "Table not found" in str(e):
                df.writeTo(f"players.{config.NAMESPACE}.{table_name}_bronce") \
                    .tableProperty("format-version", "2") \
                    .createIfNotExists()
                accion = "CREATE"
            else:
                raise  # error real → lo propagamos, no lo silenciamos
            write_audit_log(spark, config.NAMESPACE, f"{table_name}_bronce", accion, len(records))

        print(f"✅ {table_name} written successfully")

         # Compacta ficheros pequeños
        full = f"players.{config.NAMESPACE}.{table_name}_bronce"
        print(f"🔧 Compactando {full}...")
        spark.sql(f"""
            CALL players.system.rewrite_data_files(
                table => '{full}',
                strategy => 'binpack',
                options => map('target-file-size-bytes', '134217728')
            )
        """)
        print(f"✅ {full} compactado")