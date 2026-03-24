from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, HTMLResponse
from pyiceberg.catalog import load_catalog
from pyiceberg.io.fsspec import FsspecFileIO
import pyarrow as pa
import boto3

app = FastAPI()
catalog = None
s3_config = {}

"""
endpoint=http://172.16.58.11:32688
access_key=GK5f421d5f440758f74b0e0312
secret_key=409baa63477885db12cd1db0a518748c5e83e971b5e8cf2129fe6c7498de125d
bucket=warehouse

"""

@app.post("/connect")
async def connect_catalog(req: Request):
    global catalog, s3_config
    data = await req.json()
    try:
        catalog = load_catalog(
            "garage",
            **{
                "type": "rest",
                "uri": data["ep"],
                "s3.endpoint": data["ep_s3"],
                "s3.access-key-id": data["ak"],
                "s3.secret-access-key": data["sk"],
                "s3.region": "us-east-1",
                "s3.path-style-access": "true",
                "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO",
            }
        )
        # Guarda los datos de conexión para usarlos en preview
        s3_config = data

        namespaces = catalog.list_namespaces()
        print("✅ namespaces:", namespaces)
        return JSONResponse({"status": "ok"})
    except Exception as e:
        print("❌ error:", e)
        return JSONResponse({"status": "error", "msg": str(e)}, status_code=400)

@app.get("/tables")
def list_tables():
    if catalog is None:
        return []
    try:
        namespaces = catalog.list_namespaces()
        tables = []
        for ns in namespaces:
            ns_str = ns[0]
            for tbl in catalog.list_tables(namespace=ns_str):
                tables.append(f"{ns_str}.{tbl[-1]}")
        return tables
    except Exception as e:
        print("❌ error:", e)
        return []

@app.get("/schema/{table}")
def schema(table: str):
    if catalog is None:
        return {}
    ns, tbl_name = table.split(".", 1)
    t = catalog.load_table((ns, tbl_name))
    snap = t.current_snapshot()
    summary = snap.summary if snap else {}
    size_bytes = int(summary.get("total-files-size", 0))
    size_str = f"{size_bytes/1024/1024:.1f} MB" if size_bytes > 1024*1024 else f"{size_bytes/1024:.1f} KB"
    return {
        "schema": [
            {"id": f.field_id, "n": f.name, "t": str(f.field_type).lower(), "req": f.required}
            for f in t.schema().fields
        ],
        "rows": int(summary.get("total-records", 0)),
        "files": int(summary.get("total-data-files", 0)),
        "size": size_str,
        "part": str(t.spec()) or "unpartitioned",
        "loc": t.location(),
    }

@app.get("/preview/{table}")
def preview(table: str):
    if catalog is None:
        return {}
    ns, tbl_name = table.split(".", 1)
    t = catalog.load_table((ns, tbl_name))
    snap = t.current_snapshot()
    summary = snap.summary if snap else {}

    try:
        import subprocess, pyarrow.parquet as pq, io as _io, os, tempfile

        location = t.location()  # s3://warehouse/thesportsdb/leagues_bronce
        data_path = location + "/data/"

        env = {
            **os.environ,
            "AWS_ACCESS_KEY_ID": s3_config["ak"],
            "AWS_SECRET_ACCESS_KEY": s3_config["sk"],
            "AWS_DEFAULT_REGION": "us-east-1",
        }

        # Lista los parquets con aws cli
        result = subprocess.run([
            "docker", "exec", "spark-iceberg",
            "aws", "s3", "ls", data_path,
            "--endpoint-url", s3_config["ep_s3"],
        ], capture_output=True, text=True, env=env)

        print("stdout:", result.stdout)
        print("stderr:", result.stderr)
        print("returncode:", result.returncode)

        parquets = []
        for line in result.stdout.strip().splitlines():
            parts = line.split()
            if parts and parts[-1].endswith(".parquet"):
                parquets.append(data_path + parts[-1])

        print(f"parquets encontrados: {len(parquets)}")
        for p in parquets[:3]:
            print(f"  {p}")

        rows = []
        for parquet_key in parquets:
            if len(rows) >= 5:
                break

            with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
                tmp_path = tmp.name

            # Descarga dentro del contenedor y copia al host
            subprocess.run([
                "docker", "exec", "spark-iceberg",
                "aws", "s3", "cp", parquets[0], "/tmp/preview.parquet",
                "--endpoint-url", s3_config["ep_s3"],
            ], capture_output=True, env=env)

            subprocess.run([
                "docker", "cp", "spark-iceberg:/tmp/preview.parquet", tmp_path
            ], capture_output=True)

            batch = pq.read_table(tmp_path)
            print(f"filas en este fichero: {batch.num_rows}")
            os.unlink(tmp_path)

            needed = 5 - len(rows)
            for row in batch.slice(0, min(needed, batch.num_rows)).to_pylist():
                for k, v in row.items():
                    row[k] = str(v) if not isinstance(v, (str, int, float, bool, type(None))) else v
                rows.append(row)

    except Exception as e:
        print("❌ preview error:", e)
        import traceback; traceback.print_exc()
        rows = []

    return {
        "schema": [
            {"id": f.field_id, "n": f.name, "t": str(f.field_type).lower(), "req": f.required}
            for f in t.schema().fields
        ],
        "rows": int(summary.get("total-records", 0)),
        "preview": rows,
        "snap": snap.snapshot_id if snap else "—",
    }

@app.get("/snapshots/{table}")
def snapshots(table: str):
    if catalog is None:
        return {}
    ns, tbl_name = table.split(".", 1)
    t = catalog.load_table((ns, tbl_name))
    current_id = t.current_snapshot().snapshot_id if t.current_snapshot() else None
    snaps = []
    for s in reversed(list(t.metadata.snapshots)):
        sm = s.summary or {}
        from datetime import datetime
        ts = datetime.fromtimestamp(s.timestamp_ms / 1000).strftime("%Y-%m-%d %H:%M")
        snaps.append({
            "id": s.snapshot_id,
            "ts": ts,
            "op": sm.get("operation", "—"),
            "add": sm.get("added-data-files", "0"),
            "rem": sm.get("deleted-data-files", "0"),
            "rec": sm.get("total-records", "—"),
            "current": s.snapshot_id == current_id,
        })
    return {"snaps": snaps}

@app.get("/", response_class=HTMLResponse)
async def root():
    with open("ui.html") as f:
        return f.read()