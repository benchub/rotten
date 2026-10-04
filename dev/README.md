# Dev stack

`dev/docker-compose.yaml` starts the three-network layout from `docs/plan.md`:

| Network | Services |
| --- | --- |
| `observed` | `observed-postgres`, `worker`, `traffic` |
| `edge` | `worker`, `server` |
| `core` | `server`, `rotten-db` |

Start it from the repository root:

```sh
docker compose -f dev/docker-compose.yaml up
```

The stack creates a runtime-only test CA and server certificate in the
`test-certs` named volume. No private keys are committed. The server listens on
`https://localhost:8443`; its container certificate is issued for
`rotten-server`, the Docker network name used by workers, and also for
`localhost` and `127.0.0.1` for host-side checks. To verify with the CA instead
of `curl -k`, copy it out and pass it to curl:

```sh
docker compose -f dev/docker-compose.yaml cp certs:/certs/ca.pem ca.pem
curl --cacert ca.pem https://localhost:8443/healthz
```

`server-migrate` applies the rotten schema to `rotten-db` before the server
starts. `worker-key` creates a runtime-only pass key in the `worker-secrets`
volume, pinned to the dev worker FQDN. The server has an HTTPS `/healthz`
healthcheck, and the real `worker` service waits for the server and key before
running `rotten-worker -config /src/dev/worker.json`. `observed-postgres` is
Postgres 18 with `pg_stat_statements` preloaded. The worker is on `observed`
and `edge` only; it reaches the rotten DB only through `rotten-server`.

`traffic` runs application-like load on `observed-postgres` so the
controller, action and job views have something to show; see
[Traffic](#traffic).

The topology tests assert isolation by probing container IPs on the Docker
networks. They do not cover host-published ports: anything reachable through
`host.docker.internal` bypasses Docker bridge-network membership by design.

Stop and remove the stack with:

```sh
docker compose -f dev/docker-compose.yaml down
```

Add `-v` to `down` when you also want to remove the generated certificates,
worker pass key, worker state, and database volumes.

The UI runs at `http://localhost:3000` in `ROTTEN_UI_AUTH=oidc` mode with
`OMNIAUTH_FAKE=1`, so `/login` offers offline "fake viewer" and "fake admin"
sign-ins with no identity provider. See `ui/README.md`.

The dev services share named Go module and build-cache volumes. After changing
`go.mod` or `go.sum`, run `docker compose -f dev/docker-compose.yaml down -v`
before the next `up` so the dev image and volumes agree on the module cache.

## Traffic

The `traffic` service (`dev/cmd/traffic`, library `internal/devtraffic`)
plays a small made-up LMS against `observed-postgres` until stopped. It
starts with the rest of the stack. To start it in a stack that's already
running, from the repository root:

```sh
docker compose -f dev/docker-compose.yaml up -d traffic
docker compose -f dev/docker-compose.yaml logs -f traffic
```

`docker compose -f dev/docker-compose.yaml stop traffic` stops it.

On start it connects as `postgres` and, idempotently (it doesn't rely on
`observed-init.sql`, which only runs on a fresh volume):

- creates the login roles `lms_web` and `lms_jobs` (password: the role name);
- creates shard databases `lms_shard_1` to `lms_shard_4`, each with a schema of
  the same name holding `users`, `courses`, `enrollments`, `favorites`,
  `assignments`, `submissions` and `page_views`;
- seeds any shard that's empty (2,000 users, 100 courses, 800 assignments,
  about 6,000 enrollments, 20,000 page views each, at `-scale 1`).

Then it starts web requests and jobs at random (Poisson) times, one a second
by default. Each runs 1 to 6 of 30 query shapes on one connection to a random
shard, as `lms_web` or `lms_jobs`. The shapes include point reads, joins,
aggregates, `IN` lists of varying length, multi-row `VALUES` inserts, upserts,
updates and deletes. `page_view_report` and `search_users` scan, so they're
always slowish. Three shapes are usually fast but slow down in scheduled
episodes (see below), so the outliers report has something to find.
`prune_page_views` keeps `page_views` from growing without bound. Each shape
in `internal/devtraffic`'s table records its SQL and the contexts that run it,
and is flagged read-only or not, ready for routing to a replica. A test checks
the flag by running the shape in a `READ ONLY` transaction.

Every statement carries one marginalia comment in production's format, keys
in alphabetical order:

```text
/*action:list_favorite_courses,context_id:<uuid>,controller:favorites,hostname:app01000122021x,pid:N*/
/*context_id:<12 digits>,hostname:job01000104520x,job_tag:Enrollment.recompute_final_score,pid:N*/
```

There are 14 web actions across `favorites`, `courses`, `users`, `login`,
`enrollments_api`, `assignments`, `submissions` and `gradebooks`, and 10 job
tags such as `Submission.auto_grade` and `PageView.flush_buffer`. Most shapes
run under several of them. Context IDs are fresh per request or job.
Hostnames (`app010001220210` to `app010001220215`, `job010001045201` to
`job010001045204`) and pids come from a small fixed pool.
`internal/devtraffic`'s tests check that `dev/worker.json`'s regexes extract
the intended controller, action or job tag from every statement.

Configuration, as flags or environment variables (flags win). In the compose
service, set `TRAFFIC_RATE` or `TRAFFIC_EPISODE_EVERY` in your shell or
`dev/.env`; for the rest, add them to the service's `environment`.

| Flag | Environment | Default | Meaning |
| --- | --- | --- | --- |
| `-admin-dsn` | `TRAFFIC_ADMIN_DSN` | local `postgres` on `observed` | superuser DSN used for setup; the load reuses its host |
| `-rate` | `TRAFFIC_RATE` | `1` | requests and jobs started per second, on average |
| `-conns` | `TRAFFIC_CONNS` | `1` | most connections per role per shard database |
| `-shards` | `TRAFFIC_SHARDS` | `4` | shard databases |
| `-scale` | `TRAFFIC_SCALE` | `1` | seed size for new shards; existing shards are kept |
| `-comments` | `TRAFFIC_COMMENTS` | `auto` | `leading`, `trailing`, or `auto` (see below) |
| `-seed` | `TRAFFIC_SEED` | `0` (clock) | random seed |
| `-duration` | `TRAFFIC_DURATION` | `0` (forever) | stop after this long |
| `-episode-every` | `TRAFFIC_EPISODE_EVERY` | `15m` | start a slow episode at each multiple of this since the Unix epoch; `0` turns them off |
| `-episode-length` | `TRAFFIC_EPISODE_LENGTH` | `2m` | how long each episode lasts; less than `-episode-every` |
| `-episode` | `TRAFFIC_EPISODE` | empty (schedule) | run one episode, `slow_read`, `lock_wait` or `sleep`, for the whole run |

If setup or connecting fails, for example while Postgres restarts, it logs
the error and retries every 5 seconds.

### Slow episodes and the outliers report

The outliers report lists a fingerprint whose mean time per call in the
chosen range is more than 3 standard deviations above its own history (or,
with no spread, twice its mean). A query that's always slow isn't an
outlier, so each episode makes one usually fast shape much slower, with the
same SQL and so the same fingerprint:

| Episode | Shape (contexts) | Normally | In the episode | How |
| --- | --- | --- | --- | --- |
| `slow_read` | `export_enrollments` (`gradebooks#export`, `Reports::GradeExport.generate`) | 10 to 20 ms | about 1.1 s | The client reads the export's 6,000 rows (about 1 MB) with a 10 ms pause every 50 rows. Once the socket buffers fill, Postgres blocks sending rows, and that counts as execution time. |
| `lock_wait` | `touch_user` (`users#dashboard`, `login#create`, `User.touch_last_seen`) | under 0.1 ms | about 0.5 s | A `SisImport.process_users` job on each shard updates users 1 to 20 in a transaction that it keeps open for 1.5 s, then pauses 0.3 s, and repeats. `touch_user` picks only those users, so it waits on their row locks. |
| `sleep` | `course_activity` (`gradebooks#show`, `Reports::CourseActivity.generate`) | about 1 ms | 0.3 to 0.6 s | Its `pg_sleep($2)` argument is 0 outside the episode. |

Episodes start at each multiple of `-episode-every` since the Unix epoch
(by default at :00, :15, :30 and :45 past each hour, UTC), last
`-episode-length`, and take turns, so each kind recurs every 45 minutes. The
generator logs each one's start and end. They're bounded: the export is a
fixed size, the lock holders are one connection per shard, and `0` turns
episodes off. While a `touch_user` waits, it holds its shard's only web
connection (at `-conns 1`), so that shard's other web requests queue
briefly. The queueing happens in the client, so pg_stat_statements doesn't
count it.

The load's connections use a 16 KB socket receive buffer, so that slow
reading shows up on the server. Otherwise the kernel buffers several
megabytes between client and server, and Postgres would finish the export
before the client fell behind. `internal/devtraffic`'s
`TestEpisodesSlowTheirShapeInPgss` checks each episode in pg_stat_statements'
own exec times. It runs Postgres with a small TCP send buffer and connects by
the container's address, because Docker's forwarded port buffers everything.
On a compose network (MTU 1500) the small receive buffer is enough.

History builds up one sample per fingerprint per 10-second worker window in
which the fingerprint ran. The report needs 30 samples outside the chosen
range, and it counts samples from after the range, too. At the default rate,
`touch_user` runs in most windows, while `export_enrollments` and
`course_activity` run in a little under half. So **after about 15 minutes of
traffic, any finished episode can show up**, including the first one. To see
an episode, open the outliers report with a custom range from its start to
its end, as logged, for example `14:15:00` to `14:17:00` UTC. Its shape
should be at or near the top, several standard deviations above its history. Preset
ranges (1 hour and up) mostly don't show episodes. They average 2 slow
minutes with up to hours of normal windows. And once a kind has run twice,
each of its episodes is in the other's history, which widens the deviation.

### What the worker can attribute

These findings shaped the generator. The worker is unchanged.

- **One context per pg_stat_statements entry.** `pg_stat_statements` keeps
  one query text per (user, database, top-level, queryid) entry, the first it
  saw, and comments don't change the queryid. The worker extracts one
  controller, action or job tag from that text and credits all of the entry's
  calls in the window to it. So a query that runs under many contexts is
  credited to whichever context happened to run it first, for as long as the
  entry lives. Several contexts per fingerprint appear only when several
  entries share a rotten fingerprint. The generator gets that by running every
  shape in 4 shard databases as 2 roles (and, for `bulk_insert_page_views`,
  with `VALUES` lists of 2 to 10 rows; rotten's fingerprint folds `IN` and
  `VALUES` lists and schema names). Each fingerprint gets up to 8 entries. A
  warm-up pass then runs each shape once per entry, under a different context
  that runs it, so each entry's first, attributed context differs. The UI
  shows 3 to 6 contexts on about 10 fingerprints, and the rest show 1 or 2.
  New entries, after `pg_stat_statements` evicts or resets, take whichever
  context comes next.
- **Postgres 18 drops leading comments.** On Postgres 18, the text
  `pg_stat_statements` keeps starts at the statement, without a leading
  comment; 14 to 17 keep it. Comments elsewhere in the statement survive on
  all versions. Production-style leading marginalia gives rotten no contexts
  on 18. So `-comments auto` (the default) puts the comment first on 14 to 17
  and last on 18 and later. The dev stack runs 18, so its comments trail.
  `-comments leading` shows what production sees on 18.
- **Shard schemas don't split entries on 18.** Postgres 18 computes queryids
  from relation names rather than OIDs, so the same table name in two schemas
  of one database is a single entry. That's why shards are databases, not
  just schemas.
