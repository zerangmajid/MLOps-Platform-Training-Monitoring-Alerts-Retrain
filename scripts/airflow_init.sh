#!/usr/bin/env bash
set -euo pipefail
export PATH="/home/airflow/.local/bin:$PATH"

echo "[init] starting init"
airflow db migrate

# 1) admin user
ADMIN_EMAIL="${AIRFLOW_SMTP_USER:-admin@example.com}"
if airflow users list | awk '{print $1}' | grep -xq admin; then
  echo "[init] updating password for admin"
  airflow users update --username admin --password admin
else
  echo "[init] creating admin"
  airflow users create \
    --username admin --password admin \
    --firstname Air --lastname Flow \
    --role Admin --email "$ADMIN_EMAIL"
fi

# 2) metrics_db connection -> همیشه به مقدار درست ست شود
#    نکته: اگر AIRFLOW_CONN_METRICS_DB ست نباشد، پیش‌فرض به DB 'test' می‌رود
URI="${AIRFLOW_CONN_METRICS_DB:-postgresql+psycopg2://postgres:postgres@db:5432/test}"
echo "[init] ensuring connection metrics_db -> $URI"
airflow connections delete metrics_db >/dev/null 2>&1 || true
airflow connections add metrics_db --conn-uri "$URI"
echo "[init] connection metrics_db ensured."

# 3) فقط جهت اطمینان از وجود ایمیل مقصد در env
echo "[init] ALERT_EMAIL_TO=${ALERT_EMAIL_TO:-<unset>}"

echo "[init] DB migrated, admin ensured, connection set."
