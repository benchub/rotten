# Preparing an observed database

Each `rotten-worker` reads `pg_stat_statements` from one Postgres database,
the *observed* database. Postgres 14 through 18 are supported. The worker
connects as an observer role, `rotten_observer` by default, which can read
statistics but nothing else.

## 1. Install pg_stat_statements

Add `pg_stat_statements` to `shared_preload_libraries` in `postgresql.conf`
(it's a comma-separated list) and restart Postgres. Then, in the database the
worker will connect to:

```sql
CREATE EXTENSION pg_stat_statements;
```

If the extension was already installed and the cluster was upgraded, for
example with `pg_upgrade` to 17 or 18, the extension in the database can
still be an older version than the server ships. Update it before step 2:

```sql
ALTER EXTENSION pg_stat_statements UPDATE;
SELECT extversion FROM pg_extension WHERE extname = 'pg_stat_statements';
```

The min/max reset below needs extension version 1.11 or later, which comes
with Postgres 17. The worker checks `extversion`, not the server version: on
an older extension it skips the reset without an error, so min and max cover
the entry's whole lifetime.

## 2. Create the observer role and its grants

As a superuser, run `schema/observer.sql` with psql in the same database:

```sh
psql -d appdb -v observer_role=rotten_observer -v observer_schema=rotten -f schema/observer.sql
```

Both variables are optional. `observer_role` defaults to `rotten_observer`,
and `observer_schema` defaults to `rotten`. The script is safe to rerun. It
doesn't set a password; set one yourself, or use another auth method.

What it grants depends on the Postgres version:

| Version | Grants | Why |
| --- | --- | --- |
| 14, 15, 16 | `pg_read_all_stats` | Lets the observer see every role's rows and query text in `pg_stat_statements`. These versions can't reset min and max on their own, so the worker never resets anything. |
| 17, 18 | `pg_read_all_stats`, plus `USAGE` on `observer_schema` and `EXECUTE` on `<observer_schema>.pg_stat_statements_minmax_reset()` | After each harvest the worker resets only the min and max times, so they cover one window. The script creates a `SECURITY DEFINER` wrapper that calls `pg_stat_statements_reset(0, 0, 0, true)` and nothing else. The raw reset function can't be granted safely, because with its defaults it does a full reset. |

On 17 and 18 the script refuses to run, and changes nothing, if
`pg_stat_statements` isn't installed, or if `observer_schema` or an existing
wrapper is owned by someone else, or if another non-superuser role can create
objects in that schema. If you pass an `observer_schema` other than `rotten`,
set the worker's `MinmaxResetSchema` to match.

Replicas need no setup of their own; the role and wrapper replicate from the
primary. On a replica the min/max reset only affects that replica's
statistics.

## What the worker does and doesn't do

- It never resets `pg_stat_statements` counters, so other tools that read
  them keep working. It keeps a snapshot of the last harvest and reports the
  difference.
- On 17 and 18 it resets min and max after each harvest, as long as the
  extension is at version 1.11 or later. On 14 through 16,
  min and max cover the entry's whole lifetime, and there's no `stats_since`
  column.
- Min, max, mean and stddev times are execution time only. Total time is
  planning plus execution.
- It reads the most interesting statements, not all of them: the top 100 by
  each of 19 metrics.

Then configure the worker; see [worker.md](worker.md).
