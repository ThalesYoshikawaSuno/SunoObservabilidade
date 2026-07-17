#!/usr/bin/env python3
"""
Coleta dados de Airbyte, Airflow (via OpenMetadata) e Snowflake (via OpenMetadata)
e grava em data/*.csv — consumidos pelo dashboard estático (index.html).
Roda semanalmente via GitHub Actions.

Variáveis de ambiente (mesmas do .env.om já usado nos outros scripts):
  AIRBYTE_URL, AIRBYTE_CLIENT_ID, AIRBYTE_CLIENT_SECRET
  CF_ACCESS_CLIENT_ID, CF_ACCESS_CLIENT_SECRET
  OM_URL, OM_TOKEN
  AIRFLOW_SERVICE_NAME (default "Suno Airflow")
  SNOWFLAKE_SERVICE_NAME (default "Suno SnowFlake")
"""

import os
import csv
import time
import requests
from urllib.parse import quote
from datetime import datetime, timezone
from collections import defaultdict

requests.packages.urllib3.disable_warnings()

AIRBYTE_URL           = os.environ["AIRBYTE_URL"]
AIRBYTE_CLIENT_ID     = os.environ["AIRBYTE_CLIENT_ID"]
AIRBYTE_CLIENT_SECRET = os.environ["AIRBYTE_CLIENT_SECRET"]
CF_ACCESS_ID          = os.environ.get("CF_ACCESS_CLIENT_ID", "")
CF_ACCESS_SECRET      = os.environ.get("CF_ACCESS_CLIENT_SECRET", "")

OM_URL   = os.environ["OM_URL"]
OM_TOKEN = os.environ["OM_TOKEN"]

AIRFLOW_SERVICE_NAME   = os.environ.get("AIRFLOW_SERVICE_NAME", "Suno Airflow")
SNOWFLAKE_SERVICE_NAME = os.environ.get("SNOWFLAKE_SERVICE_NAME", "Suno SnowFlake")

PULSE_SAMPLES = 10  # últimas N execuções mostradas na faixa de pulso

AIRBYTE_HEADERS = {
    "Content-Type": "application/json",
    "Accept": "*/*",
    "Accept-Encoding": "gzip, deflate",
    "User-Agent": "Mozilla/5.0 (compatible; OpenMetadata/1.0)",
}
if CF_ACCESS_ID:
    AIRBYTE_HEADERS["CF-Access-Client-Id"] = CF_ACCESS_ID
    AIRBYTE_HEADERS["CF-Access-Client-Secret"] = CF_ACCESS_SECRET

OUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
os.makedirs(OUT_DIR, exist_ok=True)


def write_csv(name, fieldnames, rows):
    path = os.path.join(OUT_DIR, name)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r)
    print(f"  gravado: {path} ({len(rows)} linhas)")


def write_meta():
    path = os.path.join(OUT_DIR, "_meta.csv")
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["gerado_em"])
        w.writerow([datetime.now(timezone.utc).isoformat()])


# ── Airbyte ──────────────────────────────────────────────────────────────────

def airbyte_token():
    r = requests.post(
        f"{AIRBYTE_URL}/api/public/v1/applications/token",
        headers=AIRBYTE_HEADERS,
        json={"client_id": AIRBYTE_CLIENT_ID, "client_secret": AIRBYTE_CLIENT_SECRET, "grant-type": "client_credentials"},
        verify=False, timeout=30,
    )
    r.raise_for_status()
    return r.json()["access_token"]


def airbyte_list_all(session, path, params=None):
    params = dict(params or {})
    params.setdefault("limit", 100)
    offset, out = 0, []
    while True:
        params["offset"] = offset
        r = session.get(f"{AIRBYTE_URL}/api/public/v1{path}", params=params, verify=False, timeout=60)
        r.raise_for_status()
        items = r.json().get("data", [])
        out.extend(items)
        if len(items) < params["limit"]:
            break
        offset += params["limit"]
    return out


PULSE_MAP = {
    "succeeded": "success", "failed": "failed", "incomplete": "failed",
    "cancelled": "skipped", "running": "running", "pending": "running",
}


def fetch_airbyte():
    print("Coletando Airbyte...")
    token = airbyte_token()
    s = requests.Session()
    s.headers.update(AIRBYTE_HEADERS)
    s.headers["Authorization"] = f"Bearer {token}"

    workspaces = airbyte_list_all(s, "/workspaces")
    ws_ids = [w["workspaceId"] for w in workspaces]
    connections = airbyte_list_all(s, "/connections", {"workspaceIds": ",".join(ws_ids)})

    rows = []
    for conn in connections:
        cid = conn["connectionId"]
        name = conn.get("name", cid)
        status = conn.get("status", "unknown")
        schedule = conn.get("scheduleType", "unknown")

        jobs = airbyte_list_all(s, "/jobs", {"connectionId": cid, "jobType": "sync", "orderBy": "createdAt|DESC", "limit": 30})
        jobs.sort(key=lambda j: j.get("startTime", ""), reverse=True)

        pulse = [PULSE_MAP.get(j.get("status"), "unknown") for j in jobs[:PULSE_SAMPLES]]
        pulse.reverse()

        completed = [j for j in jobs if j.get("status") in ("succeeded", "failed", "incomplete")]
        n_success = sum(1 for j in completed if j.get("status") == "succeeded")
        success_rate = round(100 * n_success / len(completed), 1) if completed else ""

        last_job = jobs[0] if jobs else None

        rows.append({
            "id": cid,
            "nome": name,
            "status_conexao": status,
            "tipo_agendamento": schedule,
            "ultimo_status": PULSE_MAP.get(last_job.get("status"), "sem_execucao") if last_job else "sem_execucao",
            "ultima_execucao": last_job.get("startTime", "") if last_job else "",
            "taxa_sucesso_pct": success_rate,
            "execucoes_amostra": len(completed),
            "pulso": "|".join(pulse),
        })

    write_csv("airbyte.csv",
              ["id", "nome", "status_conexao", "tipo_agendamento", "ultimo_status",
               "ultima_execucao", "taxa_sucesso_pct", "execucoes_amostra", "pulso"],
              rows)


# ── OpenMetadata (Airflow) ───────────────────────────────────────────────────

def om_session():
    s = requests.Session()
    s.headers.update({"Authorization": f"Bearer {OM_TOKEN}"})
    return s


def om_get_all_pipelines(s, service_name):
    out, after = [], None
    while True:
        params = {"service": service_name, "fields": "pipelineStatus", "limit": 100}
        if after:
            params["after"] = after
        r = s.get(f"{OM_URL}/api/v1/pipelines", params=params, verify=False, timeout=60)
        r.raise_for_status()
        data = r.json()
        out.extend(data.get("data", []))
        after = data.get("paging", {}).get("after")
        if not after:
            break
    return out


def om_pipeline_status_history(s, fqn, samples):
    now_ms = int(time.time() * 1000)
    r = s.get(f"{OM_URL}/api/v1/pipelines/{quote(fqn, safe='')}/status",
              params={"startTs": 0, "endTs": now_ms}, verify=False, timeout=30)
    if r.status_code != 200:
        return []
    items = sorted(r.json().get("data", []), key=lambda x: x.get("timestamp", 0))
    return items[-samples:]


STATUS_TO_PULSE = {"Successful": "success", "Failed": "failed", "Pending": "running", "Skipped": "skipped"}


def fetch_airflow():
    print("Coletando Airflow (via OpenMetadata)...")
    s = om_session()
    pipelines = om_get_all_pipelines(s, AIRFLOW_SERVICE_NAME)

    rows = []
    for p in pipelines:
        name = p.get("displayName") or p.get("name")
        fqn = p.get("fullyQualifiedName")
        latest = p.get("pipelineStatus") or {}
        latest_status = latest.get("executionStatus", "sem_execucao")

        history = om_pipeline_status_history(s, fqn, PULSE_SAMPLES)
        pulse = [STATUS_TO_PULSE.get(h.get("executionStatus"), "unknown") for h in history]

        completed = [h for h in history if h.get("executionStatus") in ("Successful", "Failed")]
        n_success = sum(1 for h in completed if h.get("executionStatus") == "Successful")
        success_rate = round(100 * n_success / len(completed), 1) if completed else ""

        rows.append({
            "id": p.get("id"),
            "nome": name,
            "ultimo_status": STATUS_TO_PULSE.get(latest_status, "sem_execucao"),
            "ultima_execucao": datetime.fromtimestamp(latest["timestamp"] / 1000, tz=timezone.utc).isoformat() if latest.get("timestamp") else "",
            "taxa_sucesso_pct": success_rate,
            "execucoes_amostra": len(completed),
            "pulso": "|".join(pulse),
        })

    write_csv("airflow.csv",
              ["id", "nome", "ultimo_status", "ultima_execucao", "taxa_sucesso_pct", "execucoes_amostra", "pulso"],
              rows)


# ── OpenMetadata (Snowflake) ─────────────────────────────────────────────────

def fetch_snowflake():
    print("Coletando Snowflake (via OpenMetadata)...")
    s = om_session()

    # Catálogo
    tables, after = [], None
    while True:
        params = {"service": SNOWFLAKE_SERVICE_NAME, "fields": "description,owners", "limit": 100}
        if after:
            params["after"] = after
        r = s.get(f"{OM_URL}/api/v1/tables", params=params, verify=False, timeout=60)
        r.raise_for_status()
        data = r.json()
        tables.extend(data.get("data", []))
        after = data.get("paging", {}).get("after")
        if not after:
            break

    total_tables = len(tables)
    com_descricao = sum(1 for t in tables if t.get("description"))
    com_owner = sum(1 for t in tables if t.get("owners"))

    # Qualidade de dados (DQ) — pagina por tudo
    tests, after = [], None
    while True:
        params = {"fields": "testCaseResult,entityLink", "limit": 100}
        if after:
            params["after"] = after
        r = s.get(f"{OM_URL}/api/v1/dataQuality/testCases", params=params, verify=False, timeout=60)
        r.raise_for_status()
        data = r.json()
        tests.extend(data.get("data", []))
        after = data.get("paging", {}).get("after")
        if not after:
            break

    sf_tests = [t for t in tests if SNOWFLAKE_SERVICE_NAME.replace(" ", "") in (t.get("entityLink") or "").replace(" ", "")]
    dq_rows = []
    for t in sf_tests:
        result = (t.get("testCaseResult") or {}).get("testCaseStatus", "Sem execução")
        dq_rows.append({
            "nome_teste": t.get("name"),
            "entidade": t.get("entityLink", ""),
            "resultado": result,
        })

    # Contratos de dados (CD) — endpoint confirmado funcionando no 1.13.1
    cd_rows = []
    after = None
    while True:
        params = {"limit": 50}
        if after:
            params["after"] = after
        r = s.get(f"{OM_URL}/api/v1/dataContracts", params=params, verify=False, timeout=30)
        if r.status_code != 200:
            break
        data = r.json()
        for c in data.get("data", []):
            status = (c.get("latestResult") or {}).get("status", "Sem execução")
            cd_rows.append({
                "nome_contrato": c.get("name"),
                "entidade": (c.get("entity") or {}).get("fullyQualifiedName", ""),
                "status": status,
            })
        after = data.get("paging", {}).get("after")
        if not after:
            break

    write_csv("snowflake_catalogo.csv",
              ["total_tabelas", "pct_com_descricao", "pct_com_owner"],
              [{
                  "total_tabelas": total_tables,
                  "pct_com_descricao": round(100 * com_descricao / total_tables, 1) if total_tables else "",
                  "pct_com_owner": round(100 * com_owner / total_tables, 1) if total_tables else "",
              }])
    write_csv("snowflake_dq.csv", ["nome_teste", "entidade", "resultado"], dq_rows)
    write_csv("snowflake_cd.csv", ["nome_contrato", "entidade", "status"], cd_rows)


def main():
    print("=" * 65)
    print("Coletando dados do dashboard semanal — Suno")
    print("=" * 65)
    fetch_airbyte()
    fetch_airflow()
    fetch_snowflake()
    write_meta()
    print("Concluído.")


if __name__ == "__main__":
    main()
