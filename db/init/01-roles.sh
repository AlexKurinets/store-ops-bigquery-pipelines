#!/bin/sh
# Creates the datasets (schemas) and reader roles. Passwords come from the secrets volume, generated on first
# start; none are stored in the repository.
set -eu
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
  -v db="$POSTGRES_DB" \
  -v pipe="$(cat /secrets/pipeline_password)" \
  -v dash="$(cat /secrets/dashboard_reader_password)" \
  -v agent="$(cat /secrets/agent_reader_password)" \
  -f /docker-entrypoint-initdb.d/roles.sql.in
