# Dashboard Semanal — Airbyte, Airflow e Snowflake

Site estático (Vercel) atualizado semanalmente via GitHub Actions. Mesmo
padrão dos outros dashboards da Suno: Actions roda o coletor, grava CSVs,
commita no repo; o `index.html` só lê esses CSVs — sem backend.

## Setup

1. **Criar o repositório** e subir esta pasta:
   ```
   git init && git add . && git commit -m "setup inicial"
   gh repo create ThalesYoshikawaSuno/suno-dashboard-semanal --private --source=. --push
   ```

2. **Secrets do repositório** (Settings → Secrets and variables → Actions) —
   os mesmos valores do `.env.om` que você já usa nos outros scripts:
   `AIRBYTE_URL`, `AIRBYTE_CLIENT_ID`, `AIRBYTE_CLIENT_SECRET`,
   `CF_ACCESS_CLIENT_ID`, `CF_ACCESS_CLIENT_SECRET`, `OM_URL`, `OM_TOKEN`

   > Mesma ressalva de sempre: o GitHub Actions roda na nuvem, então o `OM_URL`
   > precisa ser alcançável de fora da rede interna (ou usar self-hosted runner).

3. **Rodar manualmente a primeira vez**: aba Actions → "Atualizar dados do
   dashboard (semanal)" → Run workflow. Confirma que `data/*.csv` vieram com
   dados reais (substituindo os de exemplo que já estão aqui).

4. **Deploy no Vercel**: Add New → Project → importa o repo. Site estático
   puro, sem build step — Vercel já detecta o `index.html`.

## Frequência

Cron padrão: toda segunda-feira, 8h UTC (5h BRT). Pra mudar, edita
`.github/workflows/update-data.yml`. Também dá pra disparar manualmente a
qualquer momento pela aba Actions (`workflow_dispatch`).

## Arquivos gerados (`data/`)

- `airbyte.csv` — todas as conexões, status, taxa de sucesso, pulso das últimas execuções
- `airflow.csv` — todas as DAGs, mesma estrutura
- `snowflake_catalogo.csv` — cobertura de descrição/owner
- `snowflake_dq.csv` — todos os testes de qualidade e resultado
- `snowflake_cd.csv` — todos os contratos de dados e status
- `_meta.csv` — data/hora da última coleta
