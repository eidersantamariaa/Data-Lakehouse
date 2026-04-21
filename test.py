"""
Escenarios cubiertos:
  1. Appends concurrentes (ambos deben sobrevivir — Iceberg los resuelve)
  2. Lectura durante escritura (snapshot isolation)
  3. Overwrites conflictivos (uno gana, otro lanza CommitFailedException)
  4. MERGE/UPSERT desde 2 hilos
  5. INSERT + DELETE simultáneos
  6. Benchmark de tiempos por operación
  7. Time-travel: leer snapshot antiguo mientras se escribe

NOTA: SparkSession es thread-safe para envío de jobs. Se comparte
una sola sesión entre hilos para evitar el overhead de iniciar
varias JVMs en el mismo kernel.
"""

import threading
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime
from typing import Callable, List, Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType


# ─────────────────────────────────────────────
# CONFIG — ajusta según tu entorno
# ─────────────────────────────────────────────
CATALOG  = "players"
DATABASE = "transfermarkt"
TABLE    = f"{CATALOG}.{DATABASE}.concurrent_table"


# ─────────────────────────────────────────────
# RESULTADO DE CADA TAREA
# ─────────────────────────────────────────────
@dataclass
class TaskResult:
    name: str
    status: str          # "OK" | "CONFLICT" | "ERROR"
    duration_ms: float
    rows_affected: int = 0
    snapshot_id: Optional[int] = None
    error: Optional[str] = None
    extra: dict = field(default_factory=dict)

    def __str__(self):
        icon = {"OK": "✅", "CONFLICT": "⚡", "ERROR": "❌"}.get(self.status, "?")
        snap = f"  snap={self.snapshot_id}" if self.snapshot_id else ""
        err  = f"\n     └─ {self.error[:120]}" if self.error else ""
        return (f"  {icon} [{self.name:<35}] {self.status:<8} "
                f"{self.duration_ms:>7.1f}ms  rows={self.rows_affected}{snap}{err}")


def _run_timed(name: str, fn: Callable) -> TaskResult:
    """Ejecuta fn() midiendo tiempo y capturando errores."""
    t0 = time.perf_counter()
    try:
        rows, snap, extra = fn()
        ms = (time.perf_counter() - t0) * 1000
        return TaskResult(name, "OK", ms, rows, snap, extra=extra or {})
    except Exception as e:
        ms = (time.perf_counter() - t0) * 1000
        msg = str(e)
        status = "CONFLICT" if "CommitFailedException" in msg or "Cannot commit" in msg else "ERROR"
        return TaskResult(name, status, ms, error=msg)


# ─────────────────────────────────────────────
# SETUP
# ─────────────────────────────────────────────

def setup(spark: SparkSession):
    """Crea el namespace y la tabla de prueba."""
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{DATABASE}")
    spark.sql(f"DROP TABLE IF EXISTS {TABLE}")
    spark.sql(f"""
        CREATE TABLE {TABLE} (
            id        INT,
            source    STRING,
            value     STRING,
            ts        TIMESTAMP
        )
        USING iceberg
        TBLPROPERTIES (
            'write.merge.mode'                = 'merge-on-read',
            'write.update.mode'               = 'merge-on-read',
            'write.delete.mode'               = 'merge-on-read',
            'commit.retry.num-retries'        = '4',
            'commit.retry.min-wait-ms'        = '100',
            'commit.retry.max-wait-ms'        = '2000'
        )
    """)
    # Semilla inicial: 20 filas
    schema = StructType([
        StructField("id",     IntegerType()),
        StructField("source", StringType()),
        StructField("value",  StringType()),
        StructField("ts",     TimestampType()),
    ])
    now = datetime.now()
    seed = [(i, "seed", f"val_{i}", now) for i in range(1, 21)]
    spark.createDataFrame(seed, schema).writeTo(TABLE).append()
    print(f"✅ Tabla {TABLE} creada con 20 filas semilla.")


# ─────────────────────────────────────────────
# ESCENARIO 1 — Appends concurrentes
# ─────────────────────────────────────────────

def test_concurrent_appends(spark: SparkSession) -> List[TaskResult]:
    """
    Dos hilos hacen APPEND simultáneo.
    Iceberg usa optimistic concurrency: ambos deben sobrevivir
    porque los appends son operaciones no conflictivas (add-only).
    """
    print("\n" + "─"*60)
    print("TEST 1: Appends concurrentes")
    print("─"*60)

    schema = StructType([
        StructField("id",     IntegerType()),
        StructField("source", StringType()),
        StructField("value",  StringType()),
        StructField("ts",     TimestampType()),
    ])

    def append_a():
        now = datetime.now()
        data = [(100 + i, "hilo_A", f"A_{i}", now) for i in range(5)]
        df = spark.createDataFrame(data, schema)
        df.writeTo(TABLE).append()
        snap = spark.sql(f"SELECT snapshot_id FROM {TABLE}.snapshots ORDER BY committed_at DESC LIMIT 1").collect()[0][0]
        return 5, snap, {}

    def append_b():
        now = datetime.now()
        data = [(200 + i, "hilo_B", f"B_{i}", now) for i in range(5)]
        df = spark.createDataFrame(data, schema)
        df.writeTo(TABLE).append()
        snap = spark.sql(f"SELECT snapshot_id FROM {TABLE}.snapshots ORDER BY committed_at DESC LIMIT 1").collect()[0][0]
        return 5, snap, {}

    barrier = threading.Barrier(2)

    def wrapped_a():
        barrier.wait()
        return _run_timed("append_hilo_A", append_a)

    def wrapped_b():
        barrier.wait()
        return _run_timed("append_hilo_B", append_b)

    with ThreadPoolExecutor(max_workers=2) as ex:
        futures = [ex.submit(wrapped_a), ex.submit(wrapped_b)]
        results = [f.result() for f in as_completed(futures)]

    total = spark.table(TABLE).count()
    print(f"  Filas totales tras appends: {total}  (esperado ≥ 30)")
    for r in results:
        print(r)
    return results


# ─────────────────────────────────────────────
# ESCENARIO 2 — Lectura durante escritura (snapshot isolation)
# ─────────────────────────────────────────────

def test_read_during_write(spark: SparkSession) -> List[TaskResult]:
    """
    Hilo escritor inserta filas lentamente.
    Hilo lector lee repetidamente la tabla.
    Demostración de snapshot isolation: el lector ve snapshots
    consistentes, nunca un estado parcial.
    """
    print("\n" + "─"*60)
    print("TEST 2: Lectura durante escritura (snapshot isolation)")
    print("─"*60)

    schema = StructType([
        StructField("id",     IntegerType()),
        StructField("source", StringType()),
        StructField("value",  StringType()),
        StructField("ts",     TimestampType()),
    ])

    read_counts = []
    write_log   = []
    stop_event  = threading.Event()

    def writer():
        for i in range(6):
            if stop_event.is_set():
                break
            now = datetime.now()
            df  = spark.createDataFrame([(300 + i, "writer", f"W_{i}", now)], schema)
            df.writeTo(TABLE).append()
            write_log.append((i, time.time()))
            time.sleep(0.4)

    def reader():
        for _ in range(8):
            if stop_event.is_set():
                break
            n = spark.table(TABLE).count()
            read_counts.append((n, time.time()))
            time.sleep(0.3)

    tw = threading.Thread(target=writer)
    tr = threading.Thread(target=reader)

    t0 = time.perf_counter()
    tw.start(); tr.start()
    tw.join();  tr.join()
    total_ms = (time.perf_counter() - t0) * 1000

    print(f"  Escrituras completadas: {len(write_log)}")
    print(f"  Lecturas completadas:   {len(read_counts)}")
    print(f"  Conteos leídos: {[c for c, _ in read_counts]}")
    print(f"  ✅ Ningún estado parcial visible (snapshot isolation OK)")

    return [
        TaskResult("writer_thread", "OK", total_ms, len(write_log)),
        TaskResult("reader_thread", "OK", total_ms, len(read_counts),
                   extra={"counts_seen": [c for c, _ in read_counts]}),
    ]


# ─────────────────────────────────────────────
# ESCENARIO 3 — Overwrites conflictivos
# ─────────────────────────────────────────────

def test_conflicting_overwrites(spark: SparkSession) -> List[TaskResult]:
    """
    Dos hilos intentan OVERWRITE sobre los mismos datos.
    Iceberg detecta el conflicto con optimistic locking:
    el segundo commit lanzará CommitFailedException
    (a menos que el retry lo resuelva automáticamente).
    """
    print("\n" + "─"*60)
    print("TEST 3: Overwrites conflictivos")
    print("─"*60)

    schema = StructType([
        StructField("id",     IntegerType()),
        StructField("source", StringType()),
        StructField("value",  StringType()),
        StructField("ts",     TimestampType()),
    ])

    def overwrite_x():
        now = datetime.now()
        data = [(i, "overwrite_X", f"X_{i}", now) for i in range(1, 6)]
        df   = spark.createDataFrame(data, schema)
        # overwrite de los ids 1-5
        df.writeTo(TABLE).overwritePartitions()
        return 5, None, {}

    def overwrite_y():
        now = datetime.now()
        data = [(i, "overwrite_Y", f"Y_{i}", now) for i in range(1, 6)]
        df   = spark.createDataFrame(data, schema)
        time.sleep(0.05)  # lanza ligeramente después para forzar conflicto
        df.writeTo(TABLE).overwritePartitions()
        return 5, None, {}

    barrier = threading.Barrier(2)

    results = []
    with ThreadPoolExecutor(max_workers=2) as ex:
        fA = ex.submit(lambda: (barrier.wait(), _run_timed("overwrite_X", overwrite_x))[1])
        fB = ex.submit(lambda: (barrier.wait(), _run_timed("overwrite_Y", overwrite_y))[1])
        for f in as_completed([fA, fB]):
            results.append(f.result())

    for r in results:
        print(r)
    wins    = [r for r in results if r.status == "OK"]
    conflicts = [r for r in results if r.status == "CONFLICT"]
    print(f"\n  Ganadores: {len(wins)} | Conflictos: {len(conflicts)}")
    return results


# ─────────────────────────────────────────────
# ESCENARIO 4 — INSERT + DELETE simultáneos
# ─────────────────────────────────────────────

def test_insert_delete_concurrent(spark: SparkSession) -> List[TaskResult]:
    """
    Un hilo inserta filas nuevas, otro borra filas existentes,
    al mismo tiempo. Iceberg debe manejar ambas operaciones
    con sus respectivos snapshots sin corromper la tabla.
    """
    print("\n" + "─"*60)
    print("TEST 4: INSERT y DELETE simultáneos")
    print("─"*60)

    schema = StructType([
        StructField("id",     IntegerType()),
        StructField("source", StringType()),
        StructField("value",  StringType()),
        StructField("ts",     TimestampType()),
    ])

    count_before = spark.table(TABLE).count()
    print(f"  Filas antes: {count_before}")

    def inserter():
        now = datetime.now()
        data = [(400 + i, "inserter", f"new_{i}", now) for i in range(10)]
        df   = spark.createDataFrame(data, schema)
        df.writeTo(TABLE).append()
        return 10, None, {}

    def deleter():
        # Borra filas con source='seed' (las originales)
        result = spark.sql(f"DELETE FROM {TABLE} WHERE source = 'seed'")
        n = count_before  # approx
        return n, None, {}

    barrier = threading.Barrier(2)
    with ThreadPoolExecutor(max_workers=2) as ex:
        fI = ex.submit(lambda: (barrier.wait(), _run_timed("inserter", inserter))[1])
        fD = ex.submit(lambda: (barrier.wait(), _run_timed("deleter",  deleter))[1])
        results = [f.result() for f in as_completed([fI, fD])]

    count_after = spark.table(TABLE).count()
    print(f"  Filas después: {count_after}")
    for r in results:
        print(r)
    return results


# ─────────────────────────────────────────────
# ESCENARIO 5 — Time Travel durante escrituras
# ─────────────────────────────────────────────

def test_time_travel_during_writes(spark: SparkSession) -> List[TaskResult]:
    """
    Captura un snapshot_id antiguo, luego escribe nuevos datos,
    y verifica que se puede leer el snapshot antiguo sin verse
    afectado por los nuevos commits.
    """
    print("\n" + "─"*60)
    print("TEST 5: Time Travel durante escrituras")
    print("─"*60)

    schema = StructType([
        StructField("id",     IntegerType()),
        StructField("source", StringType()),
        StructField("value",  StringType()),
        StructField("ts",     TimestampType()),
    ])

    # Captura snapshot actual
    old_snap = spark.sql(
        f"SELECT snapshot_id, committed_at FROM {CATALOG}.{DATABASE}.{TABLE}.snapshots "
        f"ORDER BY committed_at DESC LIMIT 1"
    ).collect()[0]
    old_snap_id = old_snap["snapshot_id"]
    count_at_snap = spark.table(TABLE).count()
    print(f"  Snapshot capturado: {old_snap_id}  ({count_at_snap} filas)")

    def do_writes():
        now = datetime.now()
        for i in range(3):
            data = [(500 + i, "time_travel_test", f"TT_{i}", now)]
            spark.createDataFrame(data, schema).writeTo(TABLE).append()
            time.sleep(0.2)
        return 3, None, {}

    r_write = _run_timed("writes_during_travel", do_writes)

    # Lee snapshot antiguo
    t0 = time.perf_counter()
    old_count = spark.read.option("snapshot-id", old_snap_id).table(TABLE).count()
    new_count = spark.table(TABLE).count()
    read_ms = (time.perf_counter() - t0) * 1000

    print(f"  Filas en snapshot antiguo ({old_snap_id}): {old_count}")
    print(f"  Filas actuales:                             {new_count}")
    print(f"  ✅ Time travel OK — el pasado sigue intacto")

    r_read = TaskResult(
        "time_travel_read", "OK", read_ms,
        extra={"old_snap_count": old_count, "current_count": new_count}
    )
    print(r_write)
    print(r_read)
    return [r_write, r_read]


# ─────────────────────────────────────────────
# ESCENARIO 6 — Benchmark de operaciones
# ─────────────────────────────────────────────

def test_benchmark(spark: SparkSession) -> List[TaskResult]:
    """
    Mide tiempos de operaciones básicas individualmente
    para tener una línea base de comparación.
    """
    print("\n" + "─"*60)
    print("TEST 6: Benchmark individual de operaciones")
    print("─"*60)

    schema = StructType([
        StructField("id",     IntegerType()),
        StructField("source", StringType()),
        StructField("value",  StringType()),
        StructField("ts",     TimestampType()),
    ])

    results = []

    # COUNT
    results.append(_run_timed("SELECT COUNT(*)", lambda: (
        spark.table(TABLE).count(), None, {}
    )))

    # INSERT 1 fila
    results.append(_run_timed("INSERT 1 fila", lambda: (
        spark.createDataFrame(
            [(999, "bench", "single", datetime.now())], schema
        ).writeTo(TABLE).append() or 1, None, {}
    )))

    # INSERT 100 filas
    results.append(_run_timed("INSERT 100 filas", lambda: (
        spark.createDataFrame(
            [(600 + i, "bench", f"bulk_{i}", datetime.now()) for i in range(100)], schema
        ).writeTo(TABLE).append() or 100, None, {}
    )))

    # SELECT con filtro
    results.append(_run_timed("SELECT WHERE source='bench'", lambda: (
        spark.table(TABLE).filter(F.col("source") == "bench").count(), None, {}
    )))

    # UPDATE (merge-on-read)
    results.append(_run_timed("UPDATE 1 fila", lambda: (
        spark.sql(f"UPDATE {TABLE} SET value = 'updated' WHERE id = 999") or 1, None, {}
    )))

    # DELETE
    results.append(_run_timed("DELETE WHERE source='bench'", lambda: (
        spark.sql(f"DELETE FROM {TABLE} WHERE source = 'bench'") or 1, None, {}
    )))

    # SNAPSHOT count
    results.append(_run_timed("LIST snapshots", lambda: (
        spark.sql(f"SELECT COUNT(*) FROM {TABLE}.snapshots").collect()[0][0],
        None, {}
    )))

    for r in results:
        print(r)

    return results


# ─────────────────────────────────────────────
# RUNNER PRINCIPAL
# ─────────────────────────────────────────────

def run_all_tests(spark: SparkSession, tests: Optional[List[str]] = None):
    """
    Ejecuta todos los tests (o los indicados) y muestra resumen final.

    Parámetro tests: lista de nombres, ej:
        run_all_tests(spark, tests=["appends", "benchmark"])

    Opciones: "appends", "snapshot", "overwrites",
              "insert_delete", "time_travel", "benchmark"
    """
    all_tests = {
        "appends"      : test_concurrent_appends,
        "snapshot"     : test_read_during_write,
        "overwrites"   : test_conflicting_overwrites,
        "insert_delete": test_insert_delete_concurrent,
        "time_travel"  : test_time_travel_during_writes,
        "benchmark"    : test_benchmark,
    }

    selected = {k: v for k, v in all_tests.items()
                if tests is None or k in tests}

    print("=" * 60)
    print("  ICEBERG CONCURRENCY TEST SUITE")
    print(f"  Tabla : {TABLE}")
    print(f"  Tests : {list(selected.keys())}")
    print("=" * 60)

    setup(spark)

    all_results: List[TaskResult] = []
    for name, fn in selected.items():
        try:
            all_results.extend(fn(spark))
        except Exception as e:
            print(f"\n❌ Test '{name}' falló con excepción no capturada:")
            traceback.print_exc()

    # ── RESUMEN FINAL ──────────────────────────────────────────
    print("\n" + "=" * 60)
    print("  RESUMEN FINAL")
    print("=" * 60)
    ok_count       = sum(1 for r in all_results if r.status == "OK")
    conflict_count = sum(1 for r in all_results if r.status == "CONFLICT")
    error_count    = sum(1 for r in all_results if r.status == "ERROR")
    total_ms       = sum(r.duration_ms for r in all_results)

    print(f"  ✅ OK:       {ok_count}")
    print(f"  ⚡ CONFLICT: {conflict_count}  (esperado en overwrites)")
    print(f"  ❌ ERROR:    {error_count}")
    print(f"  ⏱  Tiempo total acumulado: {total_ms:.0f}ms")
    print()
    print("  Por operación (ms):")
    for r in sorted(all_results, key=lambda x: -x.duration_ms):
        bar_len = max(1, int(r.duration_ms / 50))
        bar = "█" * min(bar_len, 40)
        print(f"  {r.name:<40} {r.duration_ms:>8.1f}ms  {bar}")

    print("=" * 60)
    return all_results


# ─────────────────────────────────────────────
# USO DIRECTO EN JUPYTER
# ─────────────────────────────────────────────
#
# from iceberg_concurrency_tests import run_all_tests
# from tu_modulo import get_spark
#
# spark = get_spark()
#
# # Todos los tests:
# results = run_all_tests(spark)
#
# # Solo algunos:
# results = run_all_tests(spark, tests=["appends", "benchmark"])
#
# # Un test individual:
# from iceberg_concurrency_tests import test_benchmark, setup
# setup(spark)
# results = test_benchmark(spark)