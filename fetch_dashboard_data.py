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
import re
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
# URL publica da UI do OM, se diferente da URL da API (ex.: API interna vs UI
# atras de proxy). Cai pra OM_URL se nao for definida.
OM_UI_URL = os.environ.get("OM_UI_URL", OM_URL)

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


# ── Classificação por BU ────────────────────────────────────────────────────
# Mesmas regras de gerar_relatorio_bu.py — fonte única de verdade pra BU,
# não duplicar critério diferente aqui. Se as regras mudarem lá, replicar aqui.
BU_RULES = {
    "ASSET": [
        "aum_", "aua_", "fundos_kpi", "fundos_rentabilidade", "ms_asset",
        "quantum", "cotistas", "fiis", "etf", "bdrs", "wallet",
        "asset_lista", "peers_funds", "positionbroker",
    ],
    "CONSULTORIA": [
        "consultoria", "assessoria", "wealth", "auc_", "advisory",
        "funil_advisory", "funil_consultoria", "gorila", "xp_seguros",
        "qtd_assessores", "movimenta", "cadastro_consultoria",
    ],
    "ASSINATURAS": [
        "hotmart", "payments", "assinatura", "subscri", "lifecycle",
        "membros_wp", "power_automate", "status_invest",
    ],
    "MARKETING": [
        "marketing", "google_ads", "instagram", "youtube", "facebook",
        "ga4", "leads", "lead_magnet", "midias_pagas", "canal_vs_lead",
        "comunidade_viva", "aniversario", "vendas_por_hora",
    ],
    "RESEARCH": [
        "research", "news_vector", "news_ai",
        "stocks", "bonds", "cryptocoins", "indices", "indexers",
        "sectors", "securities", "security", "currency", "options",
        "statistics", "integration", "sales", "fundosinvestimentos",
        "trafego_noticias", "trafego_portais", "portais_financeiros",
        "suno_noticias_wp",
    ],
}
BU_ORDER = list(BU_RULES.keys())


def classify_bu(*parts):
    haystack = " ".join(p or "" for p in parts).lower()
    if "status_invest" in haystack:
        return "ASSINATURAS"
    for bu in BU_ORDER:
        for kw in BU_RULES[bu]:
            if kw in haystack:
                return bu
    return "A REVISAR"


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


def om_get_all(s, endpoint, params_base):
    out, after = [], None
    while True:
        params = dict(params_base)
        if after:
            params["after"] = after
        r = s.get(f"{OM_URL}/api/v1/{endpoint}", params=params, verify=False, timeout=60)
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
    pipelines = om_get_all(s, "pipelines", {"service": AIRFLOW_SERVICE_NAME, "fields": "pipelineStatus", "limit": 100})

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


# ── OpenMetadata (Snowflake + BU) ────────────────────────────────────────────

TABLE_LINK_RE = re.compile(r"<#E::table::([^:]+(?:\.[^:]+)*?)(?:::|>)")


def om_table_ui_url(fqn):
    return f"{OM_UI_URL}/table/{quote(fqn, safe='')}"


def fetch_snowflake():
    print("Coletando Snowflake (via OpenMetadata)...")
    s = om_session()

    # IMPORTANTE: /tables?service=X NAO filtra nada nesta versao da API (confirmado
    # empiricamente - dois nomes de servico diferentes retornam o mesmo total/cursor).
    # /databases?service=X funciona corretamente. Por isso: lista as databases do
    # servico primeiro, depois busca tabelas por database (tambem confirmado
    # funcionando). Evita tanto contagem errada quanto contaminacao de tabelas de
    # outros servicos (ex.: Suno_Fontes_Externas) no catalogo Snowflake.
    databases = om_get_all(s, "databases", {"service": SNOWFLAKE_SERVICE_NAME, "limit": 100})
    tables = []
    for db in databases:
        db_fqn = db.get("fullyQualifiedName")
        if not db_fqn:
            continue
        tables.extend(om_get_all(s, "tables", {
            "database": db_fqn,
            "fields": "description,owners,sourceUrl",
            "limit": 100,
        }))

    total_tables = len(tables)
    com_descricao = sum(1 for t in tables if t.get("description"))
    com_owner = sum(1 for t in tables if t.get("owners"))

    table_by_fqn = {}
    for t in tables:
        fqn = t.get("fullyQualifiedName", "")
        parts = fqn.split(".")
        database = parts[1] if len(parts) > 1 else ""
        schema = parts[2] if len(parts) > 2 else ""
        name = parts[-1] if parts else fqn
        bu = classify_bu(database, schema, name)
        table_by_fqn[fqn] = {
            "bu": bu, "database": database, "schema": schema, "tabela": name, "fqn": fqn,
            "tem_descricao": "Sim" if t.get("description") else "Não",
            "tem_owner": "Sim" if t.get("owners") else "Não",
            "om_url": om_table_ui_url(fqn),
            "snowflake_url": t.get("sourceUrl") or "",
            "testes_total": 0, "testes_falha": 0, "testes_sucesso": 0, "testes_sem_execucao": 0,
        }

    # Qualidade de dados (DQ) — pagina por tudo, associa por FQN de tabela
    tests = om_get_all(s, "dataQuality/testCases", {"fields": "testCaseResult,entityLink", "limit": 100})

    sf_tests = [t for t in tests if SNOWFLAKE_SERVICE_NAME.replace(" ", "") in (t.get("entityLink") or "").replace(" ", "")]
    dq_rows = []
    for t in sf_tests:
        link = t.get("entityLink") or ""
        m = TABLE_LINK_RE.search(link)
        table_fqn = m.group(1) if m else ""
        result = (t.get("testCaseResult") or {}).get("testCaseStatus", "Sem execução")
        dq_rows.append({"nome_teste": t.get("name"), "entidade": link, "resultado": result})

        row = table_by_fqn.get(table_fqn)
        if row:
            row["testes_total"] += 1
            if result in ("Failed", "Aborted"):
                row["testes_falha"] += 1
            elif result == "Success":
                row["testes_sucesso"] += 1
            else:
                row["testes_sem_execucao"] += 1

    # Contratos de dados (CD)
    contracts = om_get_all(s, "dataContracts", {"limit": 50})
    cd_rows = []
    for c in contracts:
        status = (c.get("latestResult") or {}).get("status", "Sem execução")
        cd_rows.append({
            "nome_contrato": c.get("name"),
            "entidade": (c.get("entity") or {}).get("fullyQualifiedName", ""),
            "status": status,
        })

    # Dashboards — total + classificacao aproximada por BU (nome/displayName),
    # ja que o OM ainda nao tem domain/BU atribuido a cada dashboard.
    dashboards = om_get_all(s, "dashboards", {"fields": "description", "limit": 100})
    dash_bu_count = defaultdict(int)
    for db in dashboards:
        bu = classify_bu(db.get("name", ""), db.get("displayName", ""), db.get("description", ""))
        dash_bu_count[bu] += 1

    # Agregado por BU
    bu_rows = []
    for bu in BU_ORDER + ["A REVISAR"]:
        bu_tables = [r for r in table_by_fqn.values() if r["bu"] == bu]
        bu_rows.append({
            "bu": bu,
            "total_tabelas": len(bu_tables),
            "tabelas_com_descricao": sum(1 for r in bu_tables if r["tem_descricao"] == "Sim"),
            "tabelas_com_owner": sum(1 for r in bu_tables if r["tem_owner"] == "Sim"),
            "testes_total": sum(r["testes_total"] for r in bu_tables),
            "testes_falha": sum(r["testes_falha"] for r in bu_tables),
            "testes_sucesso": sum(r["testes_sucesso"] for r in bu_tables),
            "tabelas_sem_teste": sum(1 for r in bu_tables if r["testes_total"] == 0),
            "dashboards_aprox": dash_bu_count.get(bu, 0),
        })

    write_csv("snowflake_catalogo.csv",
              ["total_tabelas", "pct_com_descricao", "pct_com_owner", "total_dashboards"],
              [{
                  "total_tabelas": total_tables,
                  "pct_com_descricao": round(100 * com_descricao / total_tables, 1) if total_tables else "",
                  "pct_com_owner": round(100 * com_owner / total_tables, 1) if total_tables else "",
                  "total_dashboards": len(dashboards),
              }])
    write_csv("snowflake_dq.csv", ["nome_teste", "entidade", "resultado"], dq_rows)
    write_csv("snowflake_cd.csv", ["nome_contrato", "entidade", "status"], cd_rows)
    write_csv("snowflake_bu.csv",
              ["bu", "total_tabelas", "tabelas_com_descricao", "tabelas_com_owner",
               "testes_total", "testes_falha", "testes_sucesso", "tabelas_sem_teste", "dashboards_aprox"],
              bu_rows)
    write_csv("snowflake_tabelas.csv",
              ["bu", "database", "schema", "tabela", "fqn", "om_url", "snowflake_url",
               "tem_descricao", "tem_owner", "testes_total", "testes_falha", "testes_sucesso", "testes_sem_execucao"],
              sorted(table_by_fqn.values(), key=lambda r: (r["bu"], r["schema"], r["tabela"])))


# ── OpenMetadata (visao geral: agentes, apps, glossario) ────────────────────

AGENT_PIPELINE_TYPES = ["metadata", "lineage", "profiler", "autoClassification", "usage"]
AGENT_SERVICES = [SNOWFLAKE_SERVICE_NAME, AIRFLOW_SERVICE_NAME, "Suno AirByte"]
LAYER_SUFFIXES = ["Bronze", "Prata", "Ouro", "Platina", "Outros"]


def guess_layer(pipeline_name):
    for layer in LAYER_SUFFIXES:
        if pipeline_name.endswith("_" + layer) or pipeline_name.endswith(" " + layer):
            return layer
    return "—"


def fetch_om_overview():
    print("Coletando visao geral do OM (agentes, apps, glossario)...")
    s = om_session()

    # Agentes de ingestao (metadata/lineage/profiler/autoClassification/usage),
    # nos 3 servicos (Snowflake, Airflow, Airbyte) - nao so Snowflake.
    agent_rows = []
    for service_name in AGENT_SERVICES:
        for ptype in AGENT_PIPELINE_TYPES:
            pipelines = om_get_all(s, "services/ingestionPipelines", {
                "service": service_name, "pipelineType": ptype, "limit": 100,
            })
            for p in pipelines:
                name = p.get("displayName") or p.get("name")
                paused = (p.get("airflowConfig") or {}).get("pausePipeline", False)
                cron = (p.get("airflowConfig") or {}).get("scheduleInterval", "")
                agent_rows.append({
                    "nome": name,
                    "servico": service_name,
                    "tipo": ptype,
                    "camada": guess_layer(p.get("name", "")),
                    "pausado": "Sim" if paused else "Nao",
                    "cron": cron,
                })
    write_csv("om_agentes.csv", ["nome", "servico", "tipo", "camada", "pausado", "cron"], agent_rows)

    # Apps nativas do OM
    app_rows = []
    apps = om_get_all(s, "apps", {"limit": 50})
    for a in apps:
        schedule = (a.get("appSchedule") or {}).get("cronExpression", "")
        app_rows.append({
            "nome": a.get("displayName") or a.get("name"),
            "enabled": "Sim" if a.get("enabled") else "Não",
            "cron": schedule,
        })
    write_csv("om_apps.csv", ["nome", "enabled", "cron"], app_rows)

    # Glossario e Domains
    glossary_terms = om_get_all(s, "glossaryTerms", {"limit": 100})
    domains = om_get_all(s, "domains", {"limit": 50})
    write_csv("om_glossario.csv", ["total_termos", "total_domains"],
              [{"total_termos": len(glossary_terms), "total_domains": len(domains)}])


# Scripts do servidor (/root/openmetadata/*.py) — mantido manualmente aqui,
# nao existe no OM pra buscar dinamicamente. Atualizar se a lista mudar.
SERVER_SCRIPTS = [
    {"nome": "generate_descriptions.py", "agendamento": "manual", "descricao": "Gera resumoTabela/origemDados via parsing de DDL/comentario."},
    {"nome": "generate_pipeline_descriptions.py", "agendamento": "manual", "descricao": "Mesma logica para PIPELINE entities (Airbyte/Airflow)."},
    {"nome": "create_all_test_pipelines.py", "agendamento": "manual", "descricao": "Cria pipelines de teste de DQ para tabelas."},
    {"nome": "deploy_trigger_test_pipelines.py", "agendamento": "manual", "descricao": "Deploy + trigger de pipelines de teste de DQ."},
    {"nome": "create_all_contracts.py", "agendamento": "manual", "descricao": "Cria Data Contracts a partir de testCases existentes."},
    {"nome": "gerar_dq_cd_mapeamento.py", "agendamento": "manual", "descricao": "Geracao de DQ+CD para mapeamento especifico via Excel."},
    {"nome": "gerar_relatorio_bu.py", "agendamento": "manual", "descricao": "Classificacao por BU + relatorio de cobertura DQ/CD em Excel."},
    {"nome": "airflow_lineage.py", "agendamento": "0 13 * * *", "descricao": "Extrai lineage das DAGs via GitLab, insere no OM. Unico com cron real."},
    {"nome": "airflow_external_lineage.py", "agendamento": "manual (recomendado: semanal)", "descricao": "Detecta fontes externas (S3, Sheets, APIs) nas DAGs, cria servico consolidado + lineage direcional."},
    {"nome": "airbyte_report.py", "agendamento": "manual", "descricao": "Relatorio Excel de conexoes Airbyte, bypassa o conector nativo (bug conhecido)."},
    {"nome": "dag_report.py", "agendamento": "manual (auditoria ocasional)", "descricao": "Relatorio de DAGs com deteccao de escrita Snowflake via regex no codigo-fonte."},
    {"nome": "buscar_hubspot_dags.py", "agendamento": "manual (arquivado)", "descricao": "Utilitario pontual de investigacao — candidato a arquivar."},
]


def write_scripts_csv():
    write_csv("om_scripts.csv", ["nome", "agendamento", "descricao"], SERVER_SCRIPTS)


def main():
    print("=" * 65)
    print("Coletando dados do dashboard semanal — Suno")
    print("=" * 65)
    fetch_airbyte()
    fetch_airflow()
    fetch_snowflake()
    fetch_om_overview()
    write_scripts_csv()
    write_meta()
    print("Concluído.")


if __name__ == "__main__":
    main()
