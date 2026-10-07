# Observed Postgres (14 through 18) with pg_stat_statement_context (pssc)
# installed, for internal/testdb.StartObserved and the dev observed
# databases. `make image` builds it as rotten-observed-test:<major> for each
# major. The image only installs pssc; whoever runs it sets
# shared_preload_libraries = 'pg_stat_statements, pg_stat_statement_context'
# (pgss first) and creates the extension.
#
# PSSC_COMMIT pins the source, from https://github.com/benchub/pg_stat_statement_context,
# so builds are reproducible. Bump it on purpose.
ARG PG_MAJOR=18

FROM postgres:${PG_MAJOR} AS build
ARG PG_MAJOR
ARG PSSC_COMMIT=64d1c768c6523867b25247d1645841b976412cd4
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      "postgresql-server-dev-${PG_MAJOR}" build-essential git ca-certificates \
 && rm -rf /var/lib/apt/lists/*
WORKDIR /pssc
# git verifies the fetched objects against their hashes, and the rev-parse
# check fails the build unless the checkout is exactly PSSC_COMMIT.
RUN git init -q . \
 && git fetch -q --depth 1 https://github.com/benchub/pg_stat_statement_context "${PSSC_COMMIT}" \
 && git checkout -q FETCH_HEAD \
 && test "$(git rev-parse HEAD)" = "${PSSC_COMMIT}" \
 && make -j"$(nproc)" \
 && make install DESTDIR=/out

FROM postgres:${PG_MAJOR}
COPY --from=build /out/ /
