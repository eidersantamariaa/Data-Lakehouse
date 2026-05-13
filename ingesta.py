import json
from pyspark.sql import SparkSession
import pyspark
from pyspark.errors.exceptions.captured import AnalysisException
from audit_log import write_audit_log


def _resolve_merge_keys(table_name, columns):
    """Return the best merge key columns for a table."""
    column_set = set(columns)

    preferred_keys = {
        "leagues": ["idLeague"],
        "teams": ["idTeam"],
        "players": ["idPlayer"],
    }

    for key in preferred_keys.get(table_name, []):
        if key in column_set:
            return [key]

    if "id" in column_set:
        return ["id"]

    # Fallbacks for tables that are keyed by a natural composite key.
    if table_name == "teams" and {"season", "team"}.issubset(column_set):
        return ["season", "team"]
    if table_name == "players" and {"season", "team", "player"}.issubset(column_set):
        return ["season", "team", "player"]

    id_like_columns = [col for col in columns if col.lower().startswith("id")]
    if len(id_like_columns) == 1:
        return id_like_columns

    return []
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


def run_ingesta(config, test_mode=False):
    spark = get_spark()
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS players.{config.NAMESPACE}")

    data = config.get_data(test_mode=test_mode)
    processed = []

    for table_name, records in data.items():
        print(f"Processing {table_name}... ({len(records)} records{' [test mode]' if test_mode else ''})")
        full_table = f"players.{config.NAMESPACE}.{config.API}_{table_name}"
        
        df_new = spark.read.json(
            spark.sparkContext.parallelize([json.dumps(r) for r in records])
        )

        # ¿Existe la tabla?
        table_exists = spark.catalog.tableExists(full_table)

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

            merge_keys = _resolve_merge_keys(table_name, df_new.columns)
            if not merge_keys:
                print(f"⚠ No merge key found for {full_table}, appending rows instead of MERGE")
                df_new.writeTo(full_table).append()
                accion = "APPEND"
                write_audit_log(spark, config.NAMESPACE, f"{table_name}_bronce", accion, len(records))
                print(f"✅ {table_name} procesado ({accion})")
                print(f"🔧 Compactando {full_table}...")
                spark.sql(f"""
                    CALL players.system.rewrite_data_files(
                        table => '{full_table}',
                        strategy => 'binpack',
                        options => map('target-file-size-bytes', '134217728')
                    )
                """)
                print(f"✅ {full_table} compactado")
                continue

            for col in new_cols:
                col_type = dict(df_new.dtypes)[col]
                print(f"  ➕ Nueva columna detectada: {col} ({col_type})")
                spark.sql(f"ALTER TABLE {full_table} ADD COLUMN {col} {col_type}")
                accion = "ALTER"

            # Upsert: MERGE INTO usando la clave más adecuada para la tabla
            df_new.createOrReplaceTempView(f"incoming_{table_name}")

            update_set = ", ".join(
                [f"target.{c} = source.{c}" for c in df_new.columns if c not in merge_keys]
            )
            insert_cols = ", ".join(df_new.columns)
            insert_vals = ", ".join([f"source.{c}" for c in df_new.columns])

            merge_condition = " AND ".join([f"target.{c} = source.{c}" for c in merge_keys])
            diff_condition = " OR ".join(
                [
                    f"target.{c} <> source.{c} OR (target.{c} IS NULL AND source.{c} IS NOT NULL)"
                    for c in df_new.columns if c not in merge_keys
                ]
            )

            spark.sql(f"""
                MERGE INTO {full_table} AS target
                USING incoming_{table_name} AS source
                ON {merge_condition}
                WHEN MATCHED AND (
                    {diff_condition}
                ) THEN UPDATE SET {update_set}
                WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
            """)
            accion = "MERGE"

        write_audit_log(spark, config.NAMESPACE, f"{table_name}_bronce", accion, len(records))
        print(f"✅ {table_name} procesado ({accion})")
        processed.append({"table": table_name, "rows": len(records), "action": accion})

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

    return {"status": "ok", "tables": processed}