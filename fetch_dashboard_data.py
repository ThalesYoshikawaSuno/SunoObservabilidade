#!/usr/bin/env python3
"""
Coleta dados de Airbyte, Airflow (via OpenMetadata) e Snowflake (via OpenMetadata)
e grava em data/*.csv — consumidos pelo dashboard estático (index.html).

Duas cadencias via GitHub Actions, controladas por --only:
  - Hora em hora (6h-20h): airbyte, airflow_status, ingestion_status, reconcile
    (tudo que muda com frequencia - status/pulso/saude das ingestoes nativas).
  - Semanal: airflow_info, snowflake, om (dono, frequencia, catalogo - muda pouco).

Variáveis de ambiente (mesmas do .env.om já usado nos outros scripts):
  AIRBYTE_URL, AIRBYTE_CLIENT_ID, AIRBYTE_CLIENT_SECRET
  CF_ACCESS_CLIENT_ID, CF_ACCESS_CLIENT_SECRET
  OM_URL, OM_TOKEN
  AIRFLOW_SERVICE_NAME (default "Suno Airflow")
  SNOWFLAKE_SERVICE_NAME (default "Suno SnowFlake")
  SLACK_TOKEN, SLACK_USER_ID (opcional — alerta de reconciliacao; SLACK_USER_ID
    e o member id direto, ex. U0AM30R1FU6, evita depender do scope
    users:read.email que o bot nao tem hoje. SLACK_USER_EMAIL e so fallback
    se SLACK_USER_ID nao estiver setado)
  AIRFLOW_API_URL, AIRFLOW_API_USER, AIRFLOW_API_PASS (opcional — reconciliacao Airflow)
  CF_ACCESS_AIRFLOW_CLIENT_ID, CF_ACCESS_AIRFLOW_CLIENT_SECRET (opcional — Cloudflare
    Access do Airflow, app separado do Airbyte, so precisa se AIRFLOW_API_URL estiver setado)
"""

import os
import re
import csv
import time
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
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

# URLs publicas das UIs (para o botao "abrir" de cada linha no dashboard).
# Nao confundir com AIRBYTE_URL/OM_URL acima, que sao endpoints de API e podem
# nao ser navegaveis por humano (proxy interno cf-proxy:800x).
AIRFLOW_PUBLIC_URL  = os.environ.get("AIRFLOW_PUBLIC_URL", "https://airflow-v2-3438dd66106286d7.suno.com.br")
AIRBYTE_PUBLIC_URL  = os.environ.get("AIRBYTE_PUBLIC_URL", "https://airbyte-c2dc4574f078cdf5.suno.com.br")

# GitLab - mesma credencial/repo ja usada pelo airflow_lineage.py no servidor
# (le default_args do codigo das DAGs). GITLAB_TOKEN opcional: se nao setado,
# o dashboard so usa o owner que ja vem do OM, sem quebrar a run.
GITLAB_TOKEN   = os.environ.get("GITLAB_TOKEN", "")
GITLAB_PROJECT = os.environ.get("GITLAB_PROJECT", "suno-research/data-team/suno-airflow-dags")
GITLAB_BRANCH  = os.environ.get("GITLAB_BRANCH", "master")

# Slack (alerta de reconciliacao — DAG/ingestion real que nao aparece no OM).
# Token do mesmo bot ja usado em airbyte_report.py no servidor. Sem SLACK_TOKEN
# setado, notify_slack_dm() so loga no console e nao quebra a run.
SLACK_TOKEN      = os.environ.get("SLACK_TOKEN", "")
SLACK_USER_EMAIL = os.environ.get("SLACK_USER_EMAIL", "thales.yoshikawa@suno.com.br")
# Se setado, pula o lookup por e-mail (users.lookupByEmail exige o scope
# users:read.email, que o bot atual nao tem) e manda direto pra esse member id.
SLACK_USER_ID    = os.environ.get("SLACK_USER_ID", "")

# Credenciais diretas da API do Airflow (opcional, so usada pela reconciliacao
# pra comparar DAGs reais vs. catalogadas no OM). Sem elas, reconcile_and_alert()
# pula a checagem do lado Airflow sem quebrar a run.
AIRFLOW_API_URL  = os.environ.get("AIRFLOW_API_URL", "")
AIRFLOW_API_USER = os.environ.get("AIRFLOW_API_USER", "")
AIRFLOW_API_PASS = os.environ.get("AIRFLOW_API_PASS", "")
# Cloudflare Access do Airflow e um app separado do Airbyte (CF_ACCESS_CLIENT_ID/
# SECRET acima) - service token diferente, nao reaproveitar.
CF_ACCESS_AIRFLOW_ID     = os.environ.get("CF_ACCESS_AIRFLOW_CLIENT_ID", "")
CF_ACCESS_AIRFLOW_SECRET = os.environ.get("CF_ACCESS_AIRFLOW_CLIENT_SECRET", "")

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


# ── Merge incremental (status de hora em hora + info semanal no mesmo CSV) ──
# O dashboard le um unico arquivo por painel (airflow.csv/airbyte.csv) com
# TODAS as colunas. Pra rodar status a cada hora e info (dono, frequencia)
# so 1x/semana sem duas fontes divergentes, cada fetch so atualiza as colunas
# que le, preservando o resto do que ja estava gravado.

def read_csv_as_dict(name, key="id"):
    path = os.path.join(OUT_DIR, name)
    if not os.path.exists(path):
        return {}
    with open(path, newline="", encoding="utf-8") as f:
        return {row[key]: row for row in csv.DictReader(f) if row.get(key)}


def merge_write_csv(name, fieldnames, key, updates):
    """updates: dict {id: {campo: valor, ...}} com so os campos que essa run
    calculou. Campos ausentes de uma linha ja existente ficam como estavam."""
    existing = read_csv_as_dict(name, key)
    for uid, patch in updates.items():
        row = existing.setdefault(uid, {f: "" for f in fieldnames})
        row.update(patch)
        row[key] = uid
    write_csv(name, fieldnames, list(existing.values()))


# ── Historico incremental (append-only, so grava execucao nova) ─────────────
# O OM nao guarda historico longo de status de pipeline de forma confiavel
# (endpoint de status por periodo se mostrou instavel/limitado na pratica).
# Pra ter tendencia real (duracao recente vs. historica), o proprio dashboard
# passa a acumular isso a partir de agora, 1 linha por (id, timestamp de execucao).

def append_history(name, rows, key_fields=("id", "ultima_execucao")):
    path = os.path.join(OUT_DIR, name)
    fieldnames = ["id", "nome", "ultimo_status", "ultima_execucao", "taxa_sucesso_pct", "coletado_em"]
    seen = set()
    if os.path.exists(path):
        with open(path, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                seen.add(tuple(row.get(k, "") for k in key_fields))
    new_rows = []
    for r in rows:
        k = tuple(str(r.get(k, "")) for k in key_fields)
        if k in seen or not r.get("ultima_execucao"):
            continue
        seen.add(k)
        new_rows.append({**{f: r.get(f, "") for f in fieldnames}, "coletado_em": datetime.now(timezone.utc).isoformat()})
    if not new_rows:
        print(f"  {name}: nenhuma execucao nova")
        return
    write_header = not os.path.exists(path)
    with open(path, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        if write_header:
            w.writeheader()
        for r in new_rows:
            w.writerow(r)
    print(f"  {name}: +{len(new_rows)} execucoes novas")


# ── Slack (alerta de reconciliacao) ──────────────────────────────────────────

_SLACK_USER_ID = None


def _slack_user_id():
    global _SLACK_USER_ID
    if SLACK_USER_ID:
        return SLACK_USER_ID
    if _SLACK_USER_ID is None and SLACK_TOKEN:
        try:
            r = requests.get("https://slack.com/api/users.lookupByEmail",
                              headers={"Authorization": f"Bearer {SLACK_TOKEN}"},
                              params={"email": SLACK_USER_EMAIL}, timeout=15)
            d = r.json()
            _SLACK_USER_ID = d.get("user", {}).get("id", "") if d.get("ok") else ""
            if not d.get("ok"):
                print(f"  [slack] lookupByEmail falhou: {d.get('error')}")
        except requests.RequestException as e:
            print(f"  [slack] lookupByEmail erro: {e}")
            _SLACK_USER_ID = ""
    return _SLACK_USER_ID or ""


def notify_slack_dm(text, channel=None):
    """channel=None manda pro usuario padrao (SLACK_USER_ID/lookup por e-mail).
    Passar um member id especifico manda pra essa pessoa em vez disso."""
    if not SLACK_TOKEN:
        print(f"  [slack] (SLACK_TOKEN nao setado, so log) {text}")
        return
    uid = channel or _slack_user_id()
    if not uid:
        print(f"  [slack] usuario nao resolvido, so log: {text}")
        return
    try:
        r = requests.post("https://slack.com/api/chat.postMessage",
                           headers={"Authorization": f"Bearer {SLACK_TOKEN}"},
                           json={"channel": uid, "text": text}, timeout=15)
        d = r.json()
        if not d.get("ok"):
            print(f"  [slack] postMessage falhou (channel={uid}): {d.get('error')}")
    except requests.RequestException as e:
        print(f"  [slack] postMessage erro (channel={uid}): {e}")


# ── Mapeamento time -> Slack (pra rotear alerta pro responsavel real, alem
# do usuario padrao que SEMPRE recebe). Atualizar aqui conforme o time muda
# de pessoas ou o mapeamento de responsaveis for expandido no OM/GitLab. ──
SLACK_TEAM_MAP = {
    "paulo sousa": "U09TUHARWQ4",
    "paulo.sousa@suno.com.br": "U09TUHARWQ4",
    "giovanni vargas": "U09RA5XRE90",
    "giovanni.vargas@suno.com.br": "U09RA5XRE90",
    "cedric": "U09RGBPF7AS",
    "cedric fagundes": "U09RGBPF7AS",
    "cedric.fagundes@suno.com.br": "U09RGBPF7AS",
    "andre camacho": "U09MGLPK615",
    "andré camacho": "U09MGLPK615",
    "andre.camacho@suno.com.br": "U09MGLPK615",
}


def slack_id_for_responsible(responsavel):
    """Tenta casar o texto livre do campo 'responsavel' (nome do OM, autor do
    GitLab, e-mail) contra o time conhecido. None se nao bater com ninguem -
    nesse caso so o usuario padrao (sempre notificado) recebe o alerta."""
    if not responsavel:
        return None
    haystack = responsavel.lower()
    for key, slack_id in SLACK_TEAM_MAP.items():
        if key in haystack:
            return slack_id
    return None


def notify_with_responsible(text, responsavel=None):
    """Sempre notifica o usuario padrao. Se o responsavel bater com alguem do
    time, notifica essa pessoa tambem (mensagem separada, nao substitui)."""
    notify_slack_dm(text)
    match_id = slack_id_for_responsible(responsavel)
    if match_id:
        notify_slack_dm(text, channel=match_id)


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

BASIC_TIMING_PT = {
    "Every 15 MINUTES": "A cada 15 min", "Every 30 MINUTES": "A cada 30 min",
    "Every HOUR": "A cada hora", "Every 2 HOURS": "A cada 2h", "Every 3 HOURS": "A cada 3h",
    "Every 4 HOURS": "A cada 4h", "Every 6 HOURS": "A cada 6h", "Every 8 HOURS": "A cada 8h",
    "Every 12 HOURS": "A cada 12h", "Every 24 HOURS": "Diário",
}


def airbyte_frequencia(schedule):
    """Detalhe legivel do agendamento a partir de conn['schedule'] (objeto
    aninhado - 'scheduleType' NAO fica no nivel raiz da conexao, api publica
    do airbyte retorna {'scheduleType':..., 'cronExpression'|'basicTiming':...})."""
    schedule = schedule or {}
    stype = schedule.get("scheduleType", "unknown")
    if stype == "manual":
        return "Manual"
    if stype == "cron":
        return schedule.get("cronExpression", "") or "Cron"
    if stype == "basic":
        raw = schedule.get("basicTiming", "")
        return BASIC_TIMING_PT.get(raw, raw)
    return ""


AIRBYTE_FIELDS = ["id", "nome", "responsavel", "status_conexao", "tipo_agendamento", "frequencia",
                   "ultimo_status", "ultima_execucao", "taxa_sucesso_pct", "execucoes_amostra",
                   "pulso", "pulso_datas", "url"]


def fetch_airbyte():
    """Status + info da conexao Airbyte. Roda de hora em hora (nao ha custo
    extra relevante em separar - as chamadas ja fazem tudo numa mesma leva,
    diferente do Airflow, onde a checagem de dono via GitLab pesa)."""
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
        # 'scheduleType' fica dentro de conn['schedule'], nao no nivel raiz da
        # conexao (confirmado ao vivo contra a api publica do airbyte) - o
        # conn.get("scheduleType") direto sempre caia no default "unknown".
        schedule_obj = conn.get("schedule") or {}
        schedule = schedule_obj.get("scheduleType", "unknown")
        frequencia = airbyte_frequencia(schedule_obj)
        # workspaceId normalmente vem no proprio objeto de conexao; fallback pro
        # unico workspace se so existir um (evita link quebrado por campo ausente).
        wsid = conn.get("workspaceId") or (ws_ids[0] if len(ws_ids) == 1 else "")
        url = f"{AIRBYTE_PUBLIC_URL}/workspaces/{wsid}/connections/{cid}/status" if wsid else ""

        jobs = airbyte_list_all(s, "/jobs", {"connectionId": cid, "jobType": "sync", "orderBy": "createdAt|DESC", "limit": 30})
        jobs.sort(key=lambda j: j.get("startTime", ""), reverse=True)

        pulse = [PULSE_MAP.get(j.get("status"), "unknown") for j in jobs[:PULSE_SAMPLES]]
        pulse.reverse()
        pulse_dates = [j.get("startTime", "") for j in jobs[:PULSE_SAMPLES]]
        pulse_dates.reverse()

        completed = [j for j in jobs if j.get("status") in ("succeeded", "failed", "incomplete")]
        n_success = sum(1 for j in completed if j.get("status") == "succeeded")
        success_rate = round(100 * n_success / len(completed), 1) if completed else ""

        last_job = jobs[0] if jobs else None

        rows.append({
            "id": cid,
            "nome": name,
            # Sem fonte de "responsavel" pra conexao Airbyte ainda - nem a API do
            # Airbyte nem o OM tem esse dado hoje (diferente do Airflow, que pelo
            # menos tem cobertura parcial via default_args no codigo). Deixar
            # vazio em vez de inventar; coluna existe pra manter o mesmo layout
            # de tabela entre Airbyte e Airflow no dashboard.
            "responsavel": "",
            "status_conexao": status,
            "tipo_agendamento": schedule,
            "frequencia": frequencia,
            "ultimo_status": PULSE_MAP.get(last_job.get("status"), "sem_execucao") if last_job else "sem_execucao",
            "ultima_execucao": last_job.get("startTime", "") if last_job else "",
            "taxa_sucesso_pct": success_rate,
            "execucoes_amostra": len(completed),
            "pulso": "|".join(pulse),
            "pulso_datas": "|".join(pulse_dates),
            "url": url,
        })

    merge_write_csv("airbyte.csv", AIRBYTE_FIELDS, "id", {r["id"]: r for r in rows})
    append_history("history_airbyte.csv", rows)
    return rows


# ── GitLab (autor do ultimo commit do arquivo da DAG, fallback quando o OM
# nao tem owner declarado em default_args) ──────────────────────────────────

_GITLAB_SESSION = None


def gitlab_session():
    global _GITLAB_SESSION
    if _GITLAB_SESSION is None:
        s = requests.Session()
        s.headers.update({"PRIVATE-TOKEN": GITLAB_TOKEN})
        retry = Retry(total=3, backoff_factor=2, status_forcelist=[502, 503, 504],
                      allowed_methods=["GET"])
        s.mount("https://", HTTPAdapter(max_retries=retry))
        _GITLAB_SESSION = s
    return _GITLAB_SESSION


def gitlab_last_author(dag_id):
    """Autor do ultimo commit do arquivo da DAG no GitLab. Mesma resolucao de
    nome de arquivo do airflow_lineage.py (tenta '{dag_id}_dag.py', depois
    '{dag_id}.py', repo eh flat na raiz). None se GITLAB_TOKEN nao setado,
    arquivo nao encontrado, ou sem historico de commit."""
    if not GITLAB_TOKEN or not dag_id:
        return ""
    s = gitlab_session()
    proj = quote(GITLAB_PROJECT, safe="")
    for filename in (f"{dag_id}_dag.py", f"{dag_id}.py"):
        try:
            r = s.get(f"https://gitlab.com/api/v4/projects/{proj}/repository/commits",
                       params={"path": filename, "ref_name": GITLAB_BRANCH, "per_page": 1},
                       timeout=20)
            if r.status_code == 200:
                commits = r.json()
                if commits:
                    return commits[0].get("author_name", "")
        except requests.RequestException:
            pass
    return ""


# ── OpenMetadata (Airflow) ───────────────────────────────────────────────────

def om_session():
    s = requests.Session()
    s.headers.update({"Authorization": f"Bearer {OM_TOKEN}"})
    # Retry em falha de conexao/timeout - o runner do GitHub Actions ja teve
    # blip de rede pontual pra alcancar o servidor (2026-08-26, ConnectTimeout
    # isolado, servidor respondeu normal logo depois). Nao adianta falhar a
    # run inteira por 1 timeout passageiro.
    retry = Retry(total=3, backoff_factor=2, status_forcelist=[502, 503, 504],
                  allowed_methods=["GET"])
    adapter = HTTPAdapter(max_retries=retry)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
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

AIRFLOW_PRESET_PT = {
    "@once": "Uma vez", "@hourly": "A cada hora", "@daily": "Diário",
    "@weekly": "Semanal", "@monthly": "Mensal", "@yearly": "Anual",
    "None": "Sem agendamento",
}


def airflow_frequencia(schedule_interval):
    if not schedule_interval:
        return "Sem agendamento"
    return AIRFLOW_PRESET_PT.get(schedule_interval, schedule_interval)


AIRFLOW_FIELDS = ["id", "nome", "responsavel", "frequencia", "ultimo_status", "ultima_execucao",
                   "taxa_sucesso_pct", "execucoes_amostra", "pulso", "pulso_datas", "url"]


def _om_airflow_pipelines(s, fields):
    return om_get_all(s, "pipelines", {"service": AIRFLOW_SERVICE_NAME, "fields": fields, "limit": 100})


def fetch_airflow_status():
    """So status/pulso - roda de hora em hora. Nao busca owners/extension/
    scheduleInterval (isso e fetch_airflow_info(), semanal) - evita reformatar
    dono/frequencia a toa a cada hora quando esse dado quase nunca muda."""
    print("Coletando Airflow — status (via OpenMetadata)...")
    s = om_session()
    pipelines = _om_airflow_pipelines(s, "pipelineStatus")

    rows = []
    for p in pipelines:
        name = p.get("displayName") or p.get("name")
        dag_id = p.get("name")
        fqn = p.get("fullyQualifiedName")
        latest = p.get("pipelineStatus") or {}
        latest_status = latest.get("executionStatus", "sem_execucao")
        url = f"{AIRFLOW_PUBLIC_URL}/dags/{quote(dag_id, safe='')}/grid" if dag_id else ""

        history = om_pipeline_status_history(s, fqn, PULSE_SAMPLES)
        pulse = [STATUS_TO_PULSE.get(h.get("executionStatus"), "unknown") for h in history]
        pulse_dates = [
            datetime.fromtimestamp(h["timestamp"] / 1000, tz=timezone.utc).isoformat() if h.get("timestamp") else ""
            for h in history
        ]

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
            "pulso_datas": "|".join(pulse_dates),
            "url": url,
        })

    merge_write_csv("airflow.csv", AIRFLOW_FIELDS, "id", {r["id"]: r for r in rows})
    append_history("history_airflow.csv", rows)
    return pipelines


def fetch_airflow_info():
    """Dono + frequencia - roda 1x/semana. E aqui que mora o fallback ao vivo
    pro GitLab (caro, 1 chamada por DAG sem owner/cache) - cadencia semanal
    mantem isso raro."""
    print("Coletando Airflow — info (dono/frequencia, via OpenMetadata)...")
    s = om_session()
    pipelines = _om_airflow_pipelines(s, "owners,extension,scheduleInterval")

    updates = {}
    for p in pipelines:
        dag_id = p.get("name")
        name = p.get("displayName") or dag_id
        url = f"{AIRFLOW_PUBLIC_URL}/dags/{quote(dag_id, safe='')}/grid" if dag_id else ""
        owners = p.get("owners") or []
        responsavel = ", ".join(o.get("displayName") or o.get("name", "") for o in owners) or ""
        if not responsavel:
            cached = (p.get("extension") or {}).get("gitlabLastAuthor") or ""
            autor = cached.split(" <")[0] if cached else gitlab_last_author(dag_id)
            responsavel = f"{autor} (GitLab)" if autor else ""
        updates[p.get("id")] = {
            "nome": name, "url": url,
            "responsavel": responsavel,
            "frequencia": airflow_frequencia(p.get("scheduleInterval")),
        }

    merge_write_csv("airflow.csv", AIRFLOW_FIELDS, "id", updates)


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


# ── Saude das ingestoes nativas do OM (os agentes que escaneiam Airflow/
# Airbyte/Snowflake em si, nao os DAGs/conexoes individuais) ────────────────
# Motivo de existir: os scans nativos do Airflow/Airbyte estavam levando horas
# em vez de minutos (bug conhecido do OM 1.13.x com Airflow 3.x embutido -
# PR open-metadata/OpenMetadata#32005, ainda sem backport pra 1.13 em 27/ago/2026).
# Essa secao da visibilidade rapida (hora em hora) sem precisar entrar no
# servidor - so leitura de status, nao dispara nenhum scan de verdade.

INGESTION_HEALTH_SERVICES = [AIRFLOW_SERVICE_NAME, "Suno AirByte"]


def fetch_ingestion_status():
    print("Coletando status das ingestoes nativas do OM (Airflow/Airbyte)...")
    s = om_session()
    rows = []
    for service_name in INGESTION_HEALTH_SERVICES:
        pipelines = om_get_all(s, "services/ingestionPipelines", {
            "service": service_name, "pipelineType": "metadata", "fields": "pipelineStatuses", "limit": 100,
        })
        for p in pipelines:
            statuses = p.get("pipelineStatuses") or []
            if isinstance(statuses, dict):
                statuses = [statuses]
            statuses = sorted(statuses, key=lambda x: x.get("startDate", 0))
            last = statuses[-1] if statuses else {}
            start, end = last.get("startDate"), last.get("endDate")
            dur_min = round((end - start) / 1000 / 60, 1) if start and end else ""
            rows.append({
                "id": p.get("id"),
                "nome": p.get("displayName") or p.get("name"),
                "servico": service_name,
                "ultimo_status": last.get("pipelineState", "sem_execucao"),
                "ultima_execucao": datetime.fromtimestamp(start / 1000, tz=timezone.utc).isoformat() if start else "",
                "duracao_min": dur_min,
                "cron": (p.get("airflowConfig") or {}).get("scheduleInterval", ""),
            })
    write_csv("om_ingestion_status.csv",
              ["id", "nome", "servico", "ultimo_status", "ultima_execucao", "duracao_min", "cron"], rows)
    append_history("history_ingestion_status.csv", [
        {"id": r["id"], "nome": r["nome"], "ultimo_status": r["ultimo_status"],
         "ultima_execucao": r["ultima_execucao"], "taxa_sucesso_pct": ""} for r in rows
    ])


# ── Reconciliacao: DAG/conexao real sem correspondente catalogado no OM ─────
# So roda se as credenciais opcionais estiverem setadas (AIRFLOW_API_* pro
# lado Airflow). Lado Airbyte reaproveita a lista real que fetch_airbyte() ja
# buscou - sem chamada extra.

def _om_pipeline_names(s, service_name):
    pipelines = om_get_all(s, "pipelines", {"service": service_name, "limit": 100})
    return {p.get("name") for p in pipelines if p.get("name")}


# ── Estado de alerta ja enviado - evita reenviar o mesmo item toda hora.
# Um item some da lista (e volta a poder alertar) quando deixa de aparecer
# como "faltando" - ou seja, foi corrigido (catalogado) ou parou de rodar. ──

def _load_alerted_keys():
    path = os.path.join(OUT_DIR, "reconcile_alerted.csv")
    if not os.path.exists(path):
        return set()
    with open(path, newline="", encoding="utf-8") as f:
        return {row["key"] for row in csv.DictReader(f) if row.get("key")}


def _save_alerted_keys(keys):
    write_csv("reconcile_alerted.csv", ["key"], [{"key": k} for k in sorted(keys)])


def reconcile_and_alert(airbyte_rows=None):
    print("Reconciliando DAGs/conexoes reais vs. catalogadas no OM...")
    s = om_session()
    missing = []  # lista de (key, mensagem, responsavel_hint)

    if AIRFLOW_API_URL and AIRFLOW_API_USER and AIRFLOW_API_PASS:
        try:
            # AIRFLOW_API_URL costuma ser a URL publica, atras de Cloudflare Access
            # - sem o header CF-Access-Client-Id/Secret a chamada nem chega no
            # Airflow, o Cloudflare barra antes com 403. App do Airflow no
            # Cloudflare Access e separado do Airbyte, token diferente.
            headers = {}
            if CF_ACCESS_AIRFLOW_ID:
                headers["CF-Access-Client-Id"] = CF_ACCESS_AIRFLOW_ID
                headers["CF-Access-Client-Secret"] = CF_ACCESS_AIRFLOW_SECRET
            r = requests.get(f"{AIRFLOW_API_URL}/api/v1/dags", auth=(AIRFLOW_API_USER, AIRFLOW_API_PASS),
                              headers=headers, params={"limit": 1000}, verify=False, timeout=60)
            real_dag_ids = {d["dag_id"] for d in r.json().get("dags", [])}
            om_dag_ids = _om_pipeline_names(s, AIRFLOW_SERVICE_NAME)
            for dag_id in sorted(real_dag_ids - om_dag_ids):
                # checagem pontual (pode ser paginacao/atraso de indexacao, nao
                # necessariamente ausencia real) antes de declarar "faltando".
                rr = s.get(f"{OM_URL}/api/v1/pipelines/name/{quote(AIRFLOW_SERVICE_NAME + '.' + dag_id, safe='')}",
                           verify=False, timeout=20)
                if rr.status_code != 200:
                    # DAG nao esta no OM, entao nao tem "owner" la - tenta o
                    # autor do ultimo commit no GitLab como pista de quem
                    # notificar, mesmo padrao usado no restante do script.
                    hint = gitlab_last_author(dag_id)
                    msg = f"Airflow DAG `{dag_id}` roda de verdade mas nao esta catalogada no OM"
                    if hint:
                        msg += f" (ultimo commit: {hint})"
                    missing.append((f"airflow:{dag_id}", msg, hint))
        except requests.RequestException as e:
            print(f"  [reconcile] Airflow API indisponivel, pulando: {e}")
    else:
        print("  [reconcile] AIRFLOW_API_URL/USER/PASS nao setados, pulando lado Airflow")

    if airbyte_rows:
        om_conn_names = _om_pipeline_names(s, "Suno AirByte")
        rows_by_name = {r["nome"]: r for r in airbyte_rows}
        real_names = set(rows_by_name)
        for name in sorted(real_names - om_conn_names):
            # airbyte hoje nao tem fonte de responsavel (ver fetch_airbyte),
            # entao o hint fica vazio - so o usuario padrao recebe esses.
            hint = rows_by_name[name].get("responsavel") or None
            missing.append((f"airbyte:{name}", f"Conexao Airbyte `{name}` roda de verdade mas nao esta catalogada no OM", hint))

    # so alerta o que e novo desde a ultima run - item ja avisado antes (e
    # ainda faltando) nao gera notificacao repetida toda hora. Volta a
    # alertar se sumir da lista (corrigido) e reaparecer depois.
    already_alerted = _load_alerted_keys()
    new_missing = [(k, m, h) for k, m, h in missing if k not in already_alerted]

    if new_missing:
        text = "⚠️ *Dashboard Suno — itens novos sem catalogo no OM:*\n" + "\n".join(f"• {m}" for _, m, _ in new_missing)
        notify_slack_dm(text)  # resumo completo sempre vai pro usuario padrao
        # alem do resumo, quem tiver responsavel identificado recebe o item
        # dele isoladamente tambem.
        for _, msg, hint in new_missing:
            match_id = slack_id_for_responsible(hint)
            if match_id:
                notify_slack_dm(f"⚠️ *Dashboard Suno:* {msg}", channel=match_id)
        print(f"  {len(new_missing)} item(ns) NOVO(s) sem correspondencia no OM — Slack notificado")
    else:
        print(f"  nada novo pra alertar ({len(missing)} item(ns) ja conhecido(s) continuam faltando)" if missing else "  nada faltando — tudo catalogado")

    _save_alerted_keys({k for k, _, _ in missing})


FETCHERS = {
    "airbyte": fetch_airbyte,             # status+info Airbyte, hora em hora
    "airflow_status": fetch_airflow_status,  # status Airflow, hora em hora
    "airflow_info": fetch_airflow_info,      # dono/frequencia Airflow, semanal
    "ingestion_status": fetch_ingestion_status,  # saude das ingestoes nativas do OM, hora em hora
    "snowflake": fetch_snowflake,
    "om": fetch_om_overview,
}


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--only", default="",
        help="Lista separada por virgula: airbyte,airflow_status,airflow_info,"
             "ingestion_status,snowflake,om,reconcile. "
             "Vazio = roda tudo (comportamento padrao).",
    )
    args = parser.parse_args()
    only = [s.strip() for s in args.only.split(",") if s.strip()] or list(FETCHERS) + ["reconcile"]

    print("=" * 65)
    print(f"Coletando dados do dashboard — Suno ({', '.join(only)})")
    print("=" * 65)
    airbyte_rows = None
    for key in only:
        if key == "reconcile":
            continue  # roda por ultimo, depois de airbyte pra reaproveitar os dados
        result = FETCHERS[key]()
        if key == "airbyte":
            airbyte_rows = result
    if "reconcile" in only:
        reconcile_and_alert(airbyte_rows)
    if "om" in only:
        write_scripts_csv()
    write_meta()
    print("Concluído.")


if __name__ == "__main__":
    main()
