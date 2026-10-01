# Postgres 18 with pg_partman, for the rotten database in tests.
# `make image` builds this as rotten-db-test:18 for internal/testdb.StartRotten.
FROM postgres:18
RUN apt-get update \
 && apt-get install -y --no-install-recommends postgresql-18-partman \
 && rm -rf /var/lib/apt/lists/*
