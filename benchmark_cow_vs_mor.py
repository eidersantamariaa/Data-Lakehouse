"""
Benchmark COW vs MOR con datos reales de una tabla Iceberg.

Ejemplo:
cd /home/esantamaria/iceberg
source venv/bin/activate
python benchmark_cow_vs_mor.py \
  --source-table players.thesportsdb.players_bronce \
  --namespace thesportsdb \
  --base-table bench_players_real \
  --key-col idPlayer \
  --sample-rows 200000 \
  --update-ratio 0.3
"""

import argparse
import time

from pyspark.sql import functions as F
from pyspark.sql.types import DateType, NumericType, StringType, TimestampType

from ingesta import get_spark


def timed(label, fn):
    start = time.perf_counter()
    fn()
    elapsed = time.perf_counter() - start
    return label, elapsed


def create_tables_from_source(spark, source_table, namespace, base_name):
    cow_table = f"players.{namespace}.{base_name}_cow"
    mor_table = f"players.{namespace}.{base_name}_mor"

    spark.sql(f"DROP TABLE IF EXISTS {cow_table} PURGE")
    spark.sql(f"DROP TABLE IF EXISTS {mor_table} PURGE")

    spark.sql(
        f"""
        CREATE TABLE {cow_table}
        USING iceberg
        TBLPROPERTIES (
            'format-version'='2',
            'write.update.mode'='copy-on-write',
            'write.delete.mode'='copy-on-write',
            'write.merge.mode'='copy-on-write'
        )
        AS SELECT * FROM {source_table} WHERE 1 = 0
        """
    )

    spark.sql(
        f"""
        CREATE TABLE {mor_table}
        USING iceberg
        TBLPROPERTIES (
            'format-version'='2',
            'write.update.mode'='merge-on-read',
            'write.delete.mode'='merge-on-read',
            'write.merge.mode'='merge-on-read'
        )
        AS SELECT * FROM {source_table} WHERE 1 = 0
        """
    )

    return cow_table, mor_table


def detect_update_column(df, key_col, explicit_update_col):
    if explicit_update_col:
        if explicit_update_col not in df.columns:
            raise ValueError(f"La columna de update '{explicit_update_col}' no existe en la tabla fuente")
        return explicit_update_col

    for field in df.schema.fields:
        if field.name == key_col:
            continue
        if isinstance(field.dataType, (NumericType, StringType, TimestampType, DateType)):
            return field.name

    raise ValueError(
        "No se encontro columna editable automaticamente. Usa --update-col para indicarla."
    )


def build_base_df(source_df, key_col, sample_rows):
    if sample_rows and sample_rows > 0:
        # Intento de estabilidad: ordenar por key si existe.
        if key_col in source_df.columns:
            return source_df.orderBy(F.col(key_col)).limit(sample_rows)
        return source_df.limit(sample_rows)
    return source_df


def initial_load(table_name, base_df):
    def _run():
        base_df.writeTo(table_name).append()

    return timed(f"{table_name} initial_load", _run)


def build_updates_df(base_df, key_col, update_col, update_ratio):
    updates = base_df.where(F.rand(seed=42) < update_ratio)

    dt = next(f.dataType for f in base_df.schema.fields if f.name == update_col)

    if isinstance(dt, NumericType):
        updates = updates.withColumn(update_col, F.col(update_col) + F.lit(1))
    elif isinstance(dt, StringType):
        updates = updates.withColumn(update_col, F.concat(F.col(update_col), F.lit("_u")))
    elif isinstance(dt, (TimestampType, DateType)):
        updates = updates.withColumn(update_col, F.current_timestamp())
    else:
        raise ValueError(
            f"No se puede modificar automaticamente la columna {update_col} (tipo {dt}). "
            "Usa --update-col con una columna string/numerica/timestamp/date."
        )

    return updates.select(key_col, update_col)


def merge_updates(spark, table_name, key_col, update_col, updates_df):
    updates_df.createOrReplaceTempView("bench_updates")

    def _run():
        spark.sql(
            f"""
            MERGE INTO {table_name} t
            USING bench_updates s
            ON t.{key_col} = s.{key_col}
            WHEN MATCHED THEN UPDATE SET
                t.{update_col} = s.{update_col}
            """
        )

    return timed(f"{table_name} merge_update", _run)


def delete_slice(spark, table_name, key_col):
    def _run():
        spark.sql(
            f"DELETE FROM {table_name} WHERE pmod(abs(hash({key_col})), 10) = 0"
        )

    return timed(f"{table_name} delete_10pct", _run)


def read_full(spark, table_name):
    def _run():
        spark.read.table(table_name).count()

    return timed(f"{table_name} read_count", _run)


def snapshot_count(spark, table_name):
    return spark.sql(f"SELECT COUNT(*) AS n FROM {table_name}.snapshots").collect()[0]["n"]


def run(cli_args=None):
    parser = argparse.ArgumentParser(description="Benchmark COW vs MOR con datos reales")
    parser.add_argument("--source-table", default=None, help="Tabla fuente completa. Ej: players.thesportsdb.players_bronce")
    parser.add_argument("--dataset", default="thesportsdb", help="Dataset ingerido (namespace), p.ej. thesportsdb o transfermarkt")
    parser.add_argument("--entity", default="players", choices=["players", "teams", "leagues"], help="Entidad ingerida a benchmarkear")
    parser.add_argument("--namespace", default="thesportsdb")
    parser.add_argument("--base-table", default="bench_real")
    parser.add_argument("--key-col", default="id")
    parser.add_argument("--update-col", default=None)
    parser.add_argument("--sample-rows", type=int, default=200000)
    parser.add_argument("--update-ratio", type=float, default=0.3)
    parser.add_argument("--keep-tables", action="store_true")
    # In notebooks, sys.argv includes extra kernel args. parse_known_args avoids failing there.
    args, _ = parser.parse_known_args(cli_args)

    spark = get_spark()
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS players.{args.namespace}")

    source_table = args.source_table or f"players.{args.dataset}.{args.entity}_bronce"

    source_df = spark.read.table(source_table)
    if args.key_col not in source_df.columns:
        raise ValueError(
            f"La columna clave '{args.key_col}' no existe en {source_table}. "
            f"Columnas disponibles: {source_df.columns}"
        )

    update_col = detect_update_column(source_df, args.key_col, args.update_col)
    base_df = build_base_df(source_df, args.key_col, args.sample_rows).cache()
    base_rows = base_df.count()

    if base_rows == 0:
        raise ValueError("La tabla fuente no tiene filas para benchmark")

    cow_table, mor_table = create_tables_from_source(
        spark, source_table, args.namespace, args.base_table
    )

    updates_df = build_updates_df(base_df, args.key_col, update_col, args.update_ratio).cache()
    updates_rows = updates_df.count()

    print("=== Config ===")
    print(f"source_table: {source_table}")
    print(f"key_col: {args.key_col}")
    print(f"update_col: {update_col}")
    print(f"sample_rows: {base_rows}")
    print(f"updates_rows: {updates_rows}")

    results = []
    for table_name in [cow_table, mor_table]:
        results.append(initial_load(table_name, base_df))
        results.append(merge_updates(spark, table_name, args.key_col, update_col, updates_df))
        results.append(delete_slice(spark, table_name, args.key_col))
        results.append(read_full(spark, table_name))

    print("\n=== Benchmark Results (seconds) ===")
    for label, secs in results:
        print(f"{label:<60} {secs:>8.3f}")

    cow_snaps = snapshot_count(spark, cow_table)
    mor_snaps = snapshot_count(spark, mor_table)

    print("\n=== Snapshot Count ===")
    print(f"{cow_table}: {cow_snaps}")
    print(f"{mor_table}: {mor_snaps}")

    print("\n=== Summary by operation ===")
    ops = ["initial_load", "merge_update", "delete_10pct", "read_count"]
    by_operation = {}
    for op in ops:
        cow_val = next(v for k, v in results if k == f"{cow_table} {op}")
        mor_val = next(v for k, v in results if k == f"{mor_table} {op}")
        faster = "COW" if cow_val < mor_val else "MOR"
        ratio = max(cow_val, mor_val) / max(min(cow_val, mor_val), 1e-9)
        print(f"{op:<20} COW={cow_val:8.3f}s  MOR={mor_val:8.3f}s  faster={faster} ({ratio:.2f}x)")
        by_operation[op] = {
            "cow_seconds": cow_val,
            "mor_seconds": mor_val,
            "faster": faster,
            "ratio": ratio,
        }

    if not args.keep_tables:
        spark.sql(f"DROP TABLE IF EXISTS {cow_table} PURGE")
        spark.sql(f"DROP TABLE IF EXISTS {mor_table} PURGE")

    spark.stop()

    return {
        "source_table": source_table,
        "cow_table": cow_table,
        "mor_table": mor_table,
        "results": results,
        "by_operation": by_operation,
        "cow_snapshots": cow_snaps,
        "mor_snapshots": mor_snaps,
    }


def main():
    run()


if __name__ == "__main__":
    main()
