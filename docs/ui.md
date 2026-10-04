# Deploying the UI

The UI is a Rails 8.1 app in `ui/`. It shows the reports, a page for each
fingerprint, and pass key admin. It connects to the rotten database as
`rotten_ui` and never runs migrations; `rotten-server migrate` owns the
schema, so run that first ([database.md](database.md)).

This page is the reference for deploying it and for every environment
variable it reads. `ui/README.md` describes how login, reports and key admin
behave, and how to develop the app.

## Build the image

The report pages run the SQL files in `reports/`, which live outside `ui/`,
so the build needs them as a named build context. From the repository root:

```sh
docker build --build-context reports=reports -t rotten-ui ui
```

(From inside `ui/`, the same thing is `--build-context reports=../reports`.)
The app refuses to boot if any report file is missing.

The image runs as user `1000`. Thruster listens on port 80 and passes
requests to Puma. `GET /up` is the health check. It runs `SELECT 1` with a
one-second timeout and answers 200, or 503 if the database can't be reached.

## What production needs

- **TLS in front.** The app assumes it's behind a proxy or load balancer that
  terminates TLS. It treats every request as HTTPS, marks cookies `Secure`
  and sends HSTS. Make sure the proxy passes the client's address, since the
  login rate limits use it.
- **`DATABASE_URL`**, connecting as `rotten_ui`.
- **`SECRET_KEY_BASE`**, a long random secret, such as the output of
  `openssl rand -hex 64`. It encrypts the session cookie. Keep it stable
  across restarts and the same on every instance; changing it signs everyone
  out. There's no credentials file in the repository, so this env var is the
  only source.
- **`ROTTEN_UI_HOSTS`**, the host names the app answers to.
- **`ROTTEN_UI_AUTH`** and the settings for that mode, below.

```sh
docker run -d -p 8080:80 \
  -e DATABASE_URL='postgres://rotten_ui:...@db.example.com/rotten?sslmode=verify-full' \
  -e SECRET_KEY_BASE=... \
  -e ROTTEN_UI_HOSTS=rotten.example.com \
  -e ROTTEN_UI_AUTH=oidc \
  -e OIDC_ISSUER='https://<your-okta-domain>/oauth2/default' \
  -e OIDC_CLIENT_ID=... -e OIDC_CLIENT_SECRET=... \
  -e ROTTEN_UI_VIEWER_GROUP=rotten-viewers -e ROTTEN_UI_ADMIN_GROUP=rotten-admins \
  rotten-ui
```

## Environment reference

### General

| Variable | Required | Meaning |
| --- | --- | --- |
| `RAILS_ENV` | yes, `production` | The image sets it. |
| `DATABASE_URL` | yes | Postgres URL for the rotten database, as `rotten_ui`. The app uses the `rotten` schema. |
| `SECRET_KEY_BASE` | yes | See above. Rails reads it itself. |
| `ROTTEN_UI_HOSTS` | yes, in production | Comma-separated host names, such as `rotten.example.com`. A name starting with a dot, such as `.example.com`, also allows its subdomains. A request whose `Host` or `X-Forwarded-Host` isn't listed gets a 403, except `/up`, so health checks can use an IP address. The app refuses to boot without it. |
| `ROTTEN_UI_AUTH` | yes | `oidc` or `password`. The app refuses to boot if it's missing or anything else. |
| `ROTTEN_UI_REPORT_TIMEOUT` | no, default `15` | Statement timeout for each report query, in seconds. Fractions such as `2.5` are allowed. It must be a number from 0.001 to 2147483. The fingerprint page runs three queries, each with this timeout. |
| `ROTTEN_UI_SESSION_LIFETIME_HOURS` | no, default `12` | How long a sign-in lasts, in hours. Fractions such as `0.5` are allowed. It must be a number from 0.01 to 8760; the app refuses to boot otherwise. The expiry is fixed at sign-in and activity doesn't extend it. See [Sessions](#sessions). |
| `DATABASE_CONNECT_TIMEOUT` | no, default `2` | Seconds to wait when connecting to the database. |
| `RAILS_MAX_THREADS` | no | Puma threads per process (default 3) and the database pool size per process (default 5). Set it once to keep them equal. |
| `WEB_CONCURRENCY` | no; unset runs Puma in single mode, one process | Puma worker processes. Puma reads it itself. Each process has its own login rate-limit counters, so more processes allow more login attempts; see [Login rate limits](#login-rate-limits). |
| `RAILS_LOG_LEVEL` | no, default `info` | Log level. Logs go to stdout. `debug` may log personal data. |
| `PORT` | no, default `3000` | Puma's port. In the image, Thruster listens on 80 and proxies to Puma; leave this alone there. |
| `PIDFILE` | no | Where Puma writes its PID file. Unset means no PID file in production. |

### OIDC mode

`ROTTEN_UI_AUTH=oidc`. None of these values has a default in this
repository; they all come from your deployment.

| Variable | Required | Meaning |
| --- | --- | --- |
| `OIDC_ISSUER` | yes | The issuer URL. Endpoints and keys come from its `/.well-known/openid-configuration`. |
| `OIDC_CLIENT_ID` | yes | Client ID of the app registered with the identity provider. |
| `OIDC_CLIENT_SECRET` | yes | Its client secret. |
| `OIDC_GROUPS_CLAIM` | no, default `groups` | The claim, in the ID token or userinfo, that lists the user's groups. |
| `ROTTEN_UI_VIEWER_GROUP` | no | Group whose members may sign in. If unset, any authenticated user is a viewer. |
| `ROTTEN_UI_ADMIN_GROUP` | no | Group whose members are admins. **If unset, nobody is an admin through OIDC.** |
| `OIDC_REDIRECT_URI` | no | Full callback URL. If unset, it's built from the request as `<scheme>://<host>/auth/openid_connect/callback`. Set it if a proxy changes the host. |
| `OIDC_SCOPES` | no, default `openid email profile` | Space-separated scopes. `openid` is always added. Add `groups` when the provider needs that scope to send the groups claim, as Okta does. Providers that don't know a `groups` scope, such as Google, refuse the login with `invalid_scope` if it's asked for. |
| `ROTTEN_UI_CSP_FORM_ACTION_ORIGINS` | no | Comma-separated extra origins, such as `https://login.example.com`, that the browser may pass through after **Sign in**. Each must be an `http://` or `https://` origin with no path. See `ui/README.md`. |

The app refuses to boot if `OIDC_ISSUER`, `OIDC_CLIENT_ID` or
`OIDC_CLIENT_SECRET` is missing or blank. The error names the variables,
never their values.

Group names must match exactly, case included. Roles are copied from the
groups claim at each login, so a change in group membership takes effect at
the user's next login. Sessions expire after
`ROTTEN_UI_SESSION_LIFETIME_HOURS`, so that's at most 12 hours by default.
A login refused because the user is no longer in an allowed group also ends
every session they still have.

### Password mode

`ROTTEN_UI_AUTH=password` needs nothing else. Users are managed with rake
tasks, run in the app's container with the same environment:

```sh
bin/rails "users:create[alice@example.com,viewer]"
bin/rails "users:reset_password[alice@example.com]"
bin/rails "users:disable[alice@example.com]"
bin/rails "users:enable[alice@example.com]"
```

`users:create` takes `viewer` or `admin` and prints a random password once.
`users:reset_password` prints a new one. `users:disable` locks a user out in
either mode, so it's also the kill switch for OIDC users, and `users:enable`
lets them back in. Signed-in password users can change their own password
at `/password`, linked from the home page, by giving their current one. New
passwords need at least 12 characters and at most 72 bytes. See
`ui/README.md` for the details.

`users:disable` and `users:reset_password` end every session the user has,
and `users:enable` doesn't bring any of them back. See [Sessions](#sessions).

### Development only

| Variable | Meaning |
| --- | --- |
| `OMNIAUTH_FAKE` | With `RAILS_ENV=development`, `ROTTEN_UI_AUTH=oidc` and `OMNIAUTH_FAKE=1`, `/login` offers fake viewer and admin sign-ins with no identity provider, and the `OIDC_*` values become optional. It does nothing in any other environment. |

The test suite also reads `ROTTEN_UI_TEST_SEED_DATABASE_URL`, which
`make test-ui` sets.

## Pointing OIDC at Okta

Keep your Okta values in your deploy repository or secret store, never in
this one. In the Okta admin console:

1. Create an app integration: **OIDC**, **Web Application**.
2. Grant type: **Authorization Code**. Rotten sends PKCE.
3. Sign-in redirect URI: `https://<ui-host>/auth/openid_connect/callback`.
   Sign-out redirect URIs aren't used.
4. Assign the groups whose members should be able to sign in.
5. Add a groups claim named `groups` (or the name you put in
   `OIDC_GROUPS_CLAIM`), filtered to the Rotten groups so tokens stay small.
   With a custom authorization server, such as `default`, add the claim to
   the authorization server and also add a `groups` scope there; otherwise
   Okta refuses the login with `invalid_scope`.
6. Copy the client ID and client secret.

Then set:

```sh
ROTTEN_UI_AUTH=oidc
OIDC_ISSUER=https://<your-okta-domain>/oauth2/default
OIDC_CLIENT_ID=<client-id>
OIDC_CLIENT_SECRET=<client-secret>
OIDC_SCOPES="openid email profile groups"
ROTTEN_UI_VIEWER_GROUP=<viewer-group-name>
ROTTEN_UI_ADMIN_GROUP=<admin-group-name>
```

Use `https://<your-okta-domain>/oauth2/<server-id>` for another custom
authorization server, or `https://<your-okta-domain>` for the org
authorization server. It must be exactly the `issuer` that the server's
`/.well-known/openid-configuration` reports.

Okta sends `email_verified` with the `email` scope. Rotten needs it to be
`true` to create a user or link one by email. If Okta routes some users to
another identity provider, add that provider's origin to
`ROTTEN_UI_CSP_FORM_ACTION_ORIGINS`.

Other providers work the same way; `ui/README.md` has a checklist.

## Sessions

The session lives in the encrypted cookie, so the server can't delete one.
Instead each session records the user's `session_generation` (a column on
`rotten.users`) when it starts, and a session whose generation is behind the
user's is refused. These bump the generation, ending every session that user
has on any device, including copied cookies:

- signing out,
- changing or resetting a password (the browser that changed it stays
  signed in),
- `users:disable`,
- an OIDC login refused because the user is no longer in an allowed group.

Each session also expires `ROTTEN_UI_SESSION_LIFETIME_HOURS` (default 12)
after sign-in, however active it is, and the user is sent to `/login` to
sign in again. For OIDC users that re-checks their groups. Changing your
password starts a fresh lifetime, since it asks for the current password.

Sessions from before migration 0010 have neither a generation nor an expiry,
so they count as ended: everyone signs in again once after upgrading.

## Login rate limits

In password mode, signing in and changing a password are rate limited. Every
attempt counts, successful or not, and an attempt over a limit gets a 429
with a "Too many ... attempts" message.

| Action | Limits |
| --- | --- |
| Sign in (`POST /login`) | 10 attempts per client IP address and 5 attempts per email address, every 3 minutes |
| Change password (`PATCH /password`) | 10 attempts per client IP address and 5 attempts per user, every 3 minutes, counted separately from sign-in |

The counters live in `Rails.cache`, which is a memory store in production,
so each process keeps its own counters. They aren't shared between Puma
workers or containers, and a restart resets them. N processes allow N times
these limits: with `WEB_CONCURRENCY=4` in each of 2 containers, a client
gets 80 sign-in attempts per IP address every 3 minutes, not 10.

Puma runs one process by default: `config/puma.rb` doesn't set `workers`, the
image doesn't set `WEB_CONCURRENCY`, and unset means single mode. Keep it
that way and run one UI process; it's the only setup where the limits are
exactly as listed. If you must run more, scale the limits down to match by
dividing `ATTEMPTS_PER_IP` and `ATTEMPTS_PER_EMAIL` in
`ui/app/controllers/sessions_controller.rb` by the number of processes,
rounding down; the password change limits reuse them. Leave
`ATTEMPTS_WINDOW` unchanged: shortening it would weaken the limits, not
tighten them.
Each limit must stay at least 1, since a limit of 0 refuses every attempt
and locks everyone out, so the number of processes can't be more than the
smallest limit.

## Running more than one process

Each process has its own login rate-limit counters; see
[Login rate limits](#login-rate-limits). Sessions live in the encrypted
cookie, so any process can serve any request, as long as they all share
`SECRET_KEY_BASE`.
