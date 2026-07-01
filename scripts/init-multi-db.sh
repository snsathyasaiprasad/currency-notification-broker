#!/bin/bash
set -e
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
    CREATE DATABASE currency_db;
    CREATE DATABASE notification_db;
EOSQL
