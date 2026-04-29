import soccerdata as sd
from pyspark.sql import SparkSession
import pyspark
import pyarrow as pa
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, FloatType

def get_spark():
    builder = SparkSession.builder.appName("ingesta")

    builder = builder.config("spark.driver.memory", "4g")
    builder = builder.config("spark.executor.memory", "4g")
    builder = builder.config("spark.sql.shuffle.partitions", "8")  # default 200 es excesivo para local
    builder = builder.config("spark.driver.maxResultSize", "2g")

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

spark = get_spark()
spark.sql(f"CREATE NAMESPACE IF NOT EXISTS players.fbref")

def getTeams():
    types = ['standard', 'keeper', 'shooting', 'playing_time', 'misc']

    fbref = sd.FBref(leagues=['Big 5 European Leagues Combined'])

    for t in types:
        teams = fbref.read_team_season_stats(stat_type=t)

        teams.columns = [
            "_".join([str(x).strip() for x in col if x and str(x) != ""])
            if isinstance(col, tuple)
            else str(col).strip()
            for col in teams.columns
        ]

        teams.columns = [
            col.replace(" ", "_")
            .replace("-", "_")
            .replace("%", "pct")
            .replace("/", "_")
            .replace(".", "")
            .replace("+", "")
            .lower()
            for col in teams.columns
        ]

        teams.columns = [
            f"_{col}" if col[0].isdigit() else col
            for col in teams.columns
        ]

        full_table = "players.fbref.teams"

        df_new = spark.createDataFrame(teams.reset_index())
        
        # ¿Existe la tabla?
        try:
            spark.sql(f"SELECT 1 FROM {full_table} LIMIT 1")
            table_exists = True
        except Exception:
            table_exists = False

        if not table_exists:
            df_new.writeTo(full_table) \
                .tableProperty("format-version", "2") \
                .tableProperty("write.delete.mode", "merge-on-read") \
                .tableProperty("write.update.mode", "merge-on-read") \
                .tableProperty("write.merge.mode", "merge-on-read") \
                .create()
        else:
            # Evolución de esquema: añadir columnas nuevas si las hay
            existing_cols = set(spark.table(full_table).columns)
            new_cols = set(df_new.columns) - existing_cols

            for col in new_cols:
                col_type = dict(df_new.dtypes)[col]
                print(f"  ➕ Nueva columna detectada: {col} ({col_type})")
                spark.sql(f"ALTER TABLE {full_table} ADD COLUMN {col} {col_type}")

            # Upsert: MERGE INTO usando 'id' como clave
            df_new.createOrReplaceTempView(f"incoming_teams")

            merge_keys = {"season", "team"}
            update_set = ", ".join(
                [f"target.{c} = source.{c}" for c in df_new.columns if c not in merge_keys]
            )
            insert_cols = ", ".join(df_new.columns)
            insert_vals = ", ".join([f"source.{c}" for c in df_new.columns])

            spark.sql(f"""
                MERGE INTO {full_table} AS target
                USING incoming_teams AS source
                ON target.season = source.season AND target.team = source.team
                WHEN MATCHED AND (
                    {" OR ".join([f"target.{c} <> source.{c} OR (target.{c} IS NULL AND source.{c} IS NOT NULL)"
                                    for c in df_new.columns if c not in merge_keys])}
                ) THEN UPDATE SET {update_set}
                WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
            """)

def getPlayers():
    types = ['standard', 'keeper', 'shooting', 'playing_time', 'misc']

    fbref = sd.FBref(leagues=['Big 5 European Leagues Combined'])

    for t in types:
        players = fbref.read_player_season_stats(stat_type=t)

        players.columns = [
            "_".join([str(x).strip() for x in col if x and str(x) != ""])
            if isinstance(col, tuple)
            else str(col).strip()
            for col in players.columns
        ]

        players.columns = [
            col.replace(" ", "_")
            .replace("-", "_")
            .replace("%", "pct")
            .replace("/", "_")
            .replace(".", "")
            .replace("+", "")
            .lower()
            for col in players.columns
        ]

        players.columns = [
            f"_{col}" if col[0].isdigit() else col
            for col in players.columns
        ]

        full_table = "players.fbref.players"

        players = players.reset_index()

        # Aplanar dicts/lists y homogeneizar tipos
        for col in players.columns:
            players[col] = players[col].apply(lambda x: 
                str(x) if isinstance(x, (dict, list)) else x
            )

        # Reemplazar NaN/NaT por None
        players = players.where(players.notna(), other=None)

        # Detectar qué columnas siguen con tipos mixtos
        for col in players.columns:
            col_types = players[col].dropna().apply(type).unique()
            if len(col_types) > 1:
                print(f"⚠️  {col} -> {col_types}")
                players[col] = players[col].astype(str).where(players[col].notna(), other=None)

        # Usar PyArrow como puente en vez de inferencia directa de Spark
        arrow_table = pa.Table.from_pandas(players, preserve_index=False)
        df_new = spark.createDataFrame(arrow_table.to_pandas())
        
        # ¿Existe la tabla?
        try:
            spark.sql(f"SELECT 1 FROM {full_table} LIMIT 1")
            table_exists = True
        except Exception:
            table_exists = False

        if not table_exists:
            df_new.writeTo(full_table) \
                .tableProperty("format-version", "2") \
                .tableProperty("write.delete.mode", "merge-on-read") \
                .tableProperty("write.update.mode", "merge-on-read") \
                .tableProperty("write.merge.mode", "merge-on-read") \
                .create()
        else:
            # Evolución de esquema: añadir columnas nuevas si las hay
            existing_cols = set(spark.table(full_table).columns)
            new_cols = set(df_new.columns) - existing_cols

            for col in new_cols:
                col_type = dict(df_new.dtypes)[col]
                print(f"  ➕ Nueva columna detectada: {col} ({col_type})")
                spark.sql(f"ALTER TABLE {full_table} ADD COLUMN {col} {col_type}")

            # Antes del MERGE, alinear tipos con la tabla destino
            for field in df_new.schema.fields:
                if isinstance(field.dataType, (DoubleType, FloatType)):
                    df_new = df_new.withColumn(
                        field.name,
                        F.when(F.isnan(F.col(field.name)), None).otherwise(F.col(field.name))
                    )
            
            target_schema = {field.name: field.dataType 
                            for field in spark.table(full_table).schema.fields}

            for col_name, target_type in target_schema.items():
                if col_name in df_new.columns:
                    source_type = dict(df_new.dtypes)[col_name]
                    target_type_str = target_type.simpleString()
                    if source_type != target_type_str:
                        print(f"  🔄 Cast {col_name}: {source_type} -> {target_type_str}")
                        df_new = df_new.withColumn(
                            col_name, 
                            F.col(col_name).cast(target_type)
                        )

            df_new.createOrReplaceTempView("incoming_players")

            merge_keys = {"player", "team", "season"}
            update_set = ", ".join(
                [f"target.{c} = source.{c}" for c in df_new.columns if c not in merge_keys]
            )
            insert_cols = ", ".join(df_new.columns)
            insert_vals = ", ".join([f"source.{c}" for c in df_new.columns])

            spark.sql(f"""
                MERGE INTO {full_table} AS target
                USING incoming_players AS source
                ON target.player = source.player AND target.team = source.team AND target.season = source.season
                WHEN MATCHED AND (
                    {" OR ".join([f"target.{c} <> source.{c} OR (target.{c} IS NULL AND source.{c} IS NOT NULL)"
                                    for c in df_new.columns if c not in merge_keys])}
                ) THEN UPDATE SET {update_set}
                WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
            """)

def get_data():
    getTeams()
    getPlayers()

get_data()