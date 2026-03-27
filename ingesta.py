import json
from pyspark.sql import SparkSession
import pyspark
from pyspark.errors.exceptions.captured import AnalysisException
from audit_log import write_audit_log

def get_spark():
    builder = SparkSession.builder.appName("ingesta")

    builder = builder.config(
        "spark.jars.packages",
        "org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.1,"
        "org.apache.hadoop:hadoop-aws:3.4.2,"
        "com.amazonaws:aws-java-sdk-bundle:1.12.367"
    )
    builder = builder.config(
        "spark.jars.excludes",
        "org.apache.hadoop:hadoop-client-runtime,org.apache.hadoop:hadoop-client-api"
    )
    builder = builder.config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    )

    # Iceberg catalog
    builder = builder.config("spark.sql.catalog.players", "org.apache.iceberg.spark.SparkCatalog")
    builder = builder.config("spark.sql.catalog.players.type", "rest")
    builder = builder.config("spark.sql.catalog.players.uri", "http://172.16.58.11:32688")
    builder = builder.config("spark.sql.catalog.players.warehouse", "s3://warehouse/")
    builder = builder.config("spark.sql.catalog.players.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")

    # S3 / Iceberg S3 config
    builder = builder.config("spark.sql.catalog.players.s3.endpoint", "http://172.16.58.11:31224")
    builder = builder.config("spark.sql.catalog.players.s3.region", "us-east-1")
    builder = builder.config("spark.sql.catalog.players.s3.path-style-access", "true")
    builder = builder.config("spark.sql.catalog.players.client.region", "us-east-1")

    # Hadoop S3A config
    builder = builder.config("spark.hadoop.fs.s3a.endpoint", "http://172.16.58.11:31224")
    builder = builder.config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1")
    builder = builder.config("spark.hadoop.fs.s3a.access.key", "GK5f421d5f440758f74b0e0312")
    builder = builder.config("spark.hadoop.fs.s3a.secret.key", "409baa63477885db12cd1db0a518748c5e83e971b5e8cf2129fe6c7498de125d")
    builder = builder.config("spark.hadoop.fs.s3a.path.style.access", "true")
    builder = builder.config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    # Iceberg S3 tuning
    builder = builder.config("spark.sql.catalog.players.s3.checksum-algorithm", "NONE")
    builder = builder.config("spark.sql.catalog.players.s3.streaming-upload-enabled", "false")
    builder = builder.config("spark.sql.catalog.players.s3.chunked-encoding-enabled", "false")
    builder = builder.config("spark.sql.catalog.players.s3.payload-signing-enabled", "true")
    builder = builder.config("spark.sql.catalog.players.s3.http-client-type", "urlconnection")

    # Java options
    builder = builder.config("spark.executor.extraJavaOptions", "-Daws.requestChecksumCalculation=when_required")
    builder = builder.config("spark.driver.extraJavaOptions", "-Daws.requestChecksumCalculation=when_required")
    
    # Credenciales para Iceberg S3FileIO (SDK v2)
    builder = builder.config("spark.sql.catalog.players.s3.access-key-id", "GK5f421d5f440758f74b0e0312")
    builder = builder.config("spark.sql.catalog.players.s3.secret-access-key", "409baa63477885db12cd1db0a518748c5e83e971b5e8cf2129fe6c7498de125d")

    conf = builder.getOrCreate().sparkContext.getConf()
    print("Extensions:", conf.get("spark.sql.extensions", "NO CONFIGURADO"))
    print("Catalog players:", conf.get("spark.sql.catalog.players", "NO CONFIGURADO"))

    return builder.getOrCreate()


def run_ingesta(config):
    spark = get_spark()
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS players.{config.NAMESPACE}")

    data = config.get_data()

    for table_name, records in data.items():
        print(f"Processing {table_name}... ({len(records)} records)")
        full_table = f"players.{config.NAMESPACE}.{table_name}_bronce"
        
        df_new = spark.read.json(
            spark.sparkContext.parallelize([json.dumps(r) for r in records])
        )

        # ¿Existe la tabla?
        try:
            spark.sql(f"SELECT 1 FROM {full_table} LIMIT 1")
            table_exists = True
        except Exception:
            table_exists = False

        if not table_exists:
            # Primera vez: crear directamente
            df_new.writeTo(full_table) \
                .tableProperty("format-version", "2") \
                .tableProperty("write.delete.mode", "merge-on-read") \
                .tableProperty("write.update.mode", "merge-on-read") \
                .tableProperty("write.merge.mode", "merge-on-read") \
                .create()
            accion = "CREATE"

        else:
            # Evolución de esquema: añadir columnas nuevas si las hay
            existing_cols = set(spark.table(full_table).columns)
            new_cols = set(df_new.columns) - existing_cols

            for col in new_cols:
                col_type = dict(df_new.dtypes)[col]
                print(f"  ➕ Nueva columna detectada: {col} ({col_type})")
                spark.sql(f"ALTER TABLE {full_table} ADD COLUMN {col} {col_type}")
                accion = "ALTER"

            # Upsert: MERGE INTO usando 'id' como clave
            df_new.createOrReplaceTempView(f"incoming_{table_name}")

            update_set = ", ".join(
                [f"target.{c} = source.{c}" for c in df_new.columns if c != "id"]
            )
            insert_cols = ", ".join(df_new.columns)
            insert_vals = ", ".join([f"source.{c}" for c in df_new.columns])

            spark.sql(f"""
                MERGE INTO {full_table} AS target
                USING incoming_{table_name} AS source
                ON target.id = source.id
                WHEN MATCHED AND (
                    {" OR ".join([f"target.{c} <> source.{c} OR (target.{c} IS NULL AND source.{c} IS NOT NULL)"
                                  for c in df_new.columns if c != "id"])}
                ) THEN UPDATE SET {update_set}
                WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
            """)
            accion = "MERGE"

        write_audit_log(spark, config.NAMESPACE, f"{table_name}_bronce", accion, len(records))
        print(f"✅ {table_name} procesado ({accion})")

        # Compactar
        print(f"🔧 Compactando {full_table}...")
        spark.sql(f"""
            CALL players.system.rewrite_data_files(
                table => '{full_table}',
                strategy => 'binpack',
                options => map('target-file-size-bytes', '134217728')
            )
        """)
        print(f"✅ {full_table} compactado")