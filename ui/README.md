# Rotten UI

Rails 8.1 on Ruby 3.4. Ruby runs only in Docker: `make test-ui` runs the specs
against a migrated rotten database, and `dev/docker-compose.yaml` runs the app
at `http://localhost:3000`. `rotten-server migrate` owns the schema; Rails never
runs migrations.

`make test-ui UI_SPEC_ARGS=spec/system/reports_spec.rb` runs only the given
specs. The report specs seed the Go report fixture through
`ROTTEN_UI_TEST_SEED_DATABASE_URL`, a `rotten_owner` connection that
`make test-ui` sets, because `rotten_ui` can only read the event tables.

## Styles

The CSS is Tailwind v4 through `tailwindcss-rails` (the standalone binary, no
Node). The sources are in `app/assets/tailwind/`:

- `application.css` imports Tailwind and the files below, and holds the design
  tokens in `@theme`: the palette, fonts, type scale, radii and shadows. Every
  color in the views comes from a token, and the text pairs meet WCAG AA.
- `base.css` sets element defaults, link styles and the focus outline.
- `components.css` and `reports.css` hold the recurring components, built with
  `@apply` in `@layer components`: cards, buttons, inputs, tables, badges,
  alerts, the report chooser and the charts.

Views use utilities for one-off layout and the component classes for the rest.
There are no animations or transitions, and no inline styles, because the CSP
forbids them.

The build writes `app/assets/builds/tailwind.css`, which is gitignored:

- `make test-ui` runs `bin/rails tailwindcss:build` before the specs, and
  `spec/requests/stylesheet_spec.rb` fails if the layout doesn't link the
  built CSS or it's stale.
- The dev stack's `ui` service builds it before `rails server` starts, and the
  `ui-css` service runs `bin/rails tailwindcss:watch[always]` so a refresh
  picks up changes. Outside Docker, `bin/dev` does the same through
  `Procfile.dev`.
- The production image builds it in `assets:precompile`.

## Dev image

The dev image (`ui/dev.Dockerfile`, with Chromium for system specs) is built
for the Docker host's native platform, so it runs without emulation on arm64.
`make ui-image` tags it `rotten-ui-dev:linux-<arch>` and checks the built
architecture. To build and test another platform, set `UI_PLATFORM`, e.g.
`make test-ui UI_PLATFORM=linux/amd64`; its base image must already be pulled
for that platform.

## Authentication

`ROTTEN_UI_AUTH` picks the mode, `oidc` or `password`. The app refuses to boot
if it's missing or anything else. Every user is a `viewer` or an `admin`. A
user whose `active` flag is false is locked out, whatever the identity
provider says.

[`docs/ui.md`](../docs/ui.md) is the reference for deploying the app and for
every environment variable it reads, in both modes. This section describes
how each mode behaves.

### OIDC mode

Every org-specific value comes from env. None has a default in this repo.
The variables are listed in [`docs/ui.md`](../docs/ui.md#oidc-mode).

In `oidc` mode the app refuses to boot if `OIDC_ISSUER`, `OIDC_CLIENT_ID` or
`OIDC_CLIENT_SECRET` is missing or blank. The error names the missing
variables, never their values.

The login uses the authorization code flow with PKCE, a nonce and a state
parameter. `/login` shows a **Sign in** button that POSTs to
`/auth/openid_connect` with a CSRF token. A GET there does nothing.

**Redirect origins.** The Content-Security-Policy's `form-action` lets the
Sign in form lead only to this app's origin and the origin of `OIDC_ISSUER`,
and Chrome enforces that on every redirect that follows the POST. A login that
passes through any other origin is blocked in the browser with a CSP error in
the console. List those origins in `ROTTEN_UI_CSP_FORM_ACTION_ORIGINS`:

- when discovery's `authorization_endpoint` isn't on the issuer's origin. On
  Amazon Cognito, for example, the issuer is
  `https://cognito-idp.<region>.amazonaws.com/<pool>` but users sign in at
  the user pool domain, such as `https://<prefix>.auth.<region>.amazoncognito.com`;
- when the provider federates or brokers to another one and redirects the
  browser on: Okta routing rules to another IdP, Keycloak identity brokering,
  Entra ID or Google sending a federated domain to its own SSO (for example
  `https://adfs.example.com`).

Each entry must be an `http://` or `https://` origin: scheme, host and an
optional port, with no path, query, wildcard or credentials (a bare trailing
`/` is accepted). Empty entries are ignored. Any other value stops the app
from booting, with an error that names the bad entry.

**What happens at each login:**

- **Finding the user.** Match on the issuer and the OIDC `sub`, then on email,
  then create a new user. `users.provider` stores `openid_connect:<issuer>`,
  so a `sub` from one issuer never matches a user from another; changing
  `OIDC_ISSUER` doesn't hand existing users to the new issuer. Creating a user
  or linking one by email needs a verified email: the provider must send
  `email_verified` as `true` (or the string `"true"`). Anything else,
  including a missing claim, is refused with a message saying the email isn't
  verified, and nothing is written. An email match only links a user that has
  no OIDC identity yet. An email that already belongs to another identity,
  from this issuer or another, is refused rather than relinked.
- **Resync.** Name, groups and role are copied from the provider on every
  login. The email is copied too, but only when it's verified; otherwise the
  stored email is kept. `users.email` is unique, so an unverified email could
  otherwise claim someone else's address and lock its real owner out. Emails
  are stored lowercased.
- **Role, failing closed.** Admin needs membership of `ROTTEN_UI_ADMIN_GROUP`.
  Otherwise the user is a viewer if `ROTTEN_UI_VIEWER_GROUP` is unset or they're
  in it. An admin-group member doesn't also need the viewer group. Group names
  match exactly, case included.
- **Groups claim.** It may be missing, a single string, or an array. Anything
  else counts as no groups, as do array elements that aren't strings, are
  empty or very long, or contain NUL bytes, control characters, or text that
  isn't valid UTF-8. Only the first 500 usable groups count, for the role
  and in `users.groups`, so an allowed group listed after them is ignored.
  A missing claim gives a viewer at most, never an admin.
- **Odd claim values.** The `sub`, email and name get the same treatment: a
  value that isn't usable text counts as missing. Emails also may not contain
  spaces, control characters or any invisible format character (Unicode
  category Cf, such as bidi controls like U+202E, zero-width space, soft
  hyphen, byte order mark or tag characters), so a lookalike of a real address
  is refused rather than stored. The zero-width joiner and non-joiner are the
  only exceptions, since Persian and Indic addresses use them.
  `spec/security/oidc_provisioning_fuzz_spec.rb` feeds the callback hostile
  values of every kind.
- **Refusals** show a 403 page and grant no session. Any session the browser
  already had is ended too. Rotten refuses:
  - users in neither group when `ROTTEN_UI_VIEWER_GROUP` is set,
  - users with `active` set to false,
  - logins with no usable email or `sub`,
  - new users and email links without a verified email,
  - email conflicts, as above.
- **Rows on refusal.** A refused login never creates a user, and never links
  or changes a user it only found by email. If the user already exists with
  this issuer and `sub`, the resync still happens, so someone removed from the
  groups loses admin in the database straight away. That refusal also bumps
  their `session_generation`, which ends every session they already have.
  If the resync is refused because the new verified email belongs to another
  user, it's redone without the email, so the groups, role and session bump
  still apply.
- **Sessions.** The session is reset before the user ID is stored, which
  prevents session fixation. It expires after
  `ROTTEN_UI_SESSION_LIFETIME_HOURS` (12 by default), and signing in again
  re-checks the user's groups. See [Sessions](#sessions-and-revocation)
  below.
- **Failures** from the provider or OmniAuth (a denied consent, a bad state,
  an unreachable issuer) land on `/auth/failure`, which redirects to `/login`
  with a generic message. Details go to the log only.

### Identity provider app checklist

The deploy repo does the real setup with its own values. For Okta,
step by step, see [`docs/ui.md`](../docs/ui.md#pointing-oidc-at-okta). Any
provider app needs:

- **Type:** a web app (confidential client) using the authorization code
  grant. PKCE is sent, so enable it if the provider asks.
- **Sign-in redirect URI:** `https://<ui-host>/auth/openid_connect/callback`.
- **Scopes:** `openid email profile` by default. On Okta, the groups claim
  usually needs the `groups` scope too, so set
  `OIDC_SCOPES="openid email profile groups"`. On an Okta custom
  authorization server, add a `groups` scope to it first, or Okta rejects the
  login with `invalid_scope`.
- **Groups claim:** a claim named `groups`, or whatever `OIDC_GROUPS_CLAIM` is
  set to, carrying the user's group names. Filter it to the Rotten groups, so
  tokens stay small.
- **Verified email:** the provider must send the `email_verified` claim, set
  to `true`, for every user who should be able to sign in. Without it, nobody
  new can sign in, existing pre-OIDC users can't be linked, and email changes
  aren't picked up. Okta sends `email_verified` by default with the `email`
  scope. On providers where users can set their own unverified address
  (Keycloak, Auth0, Cognito), make sure verification is enforced.
- **Assignments:** the users or groups allowed to use the app. With Okta, that
  usually means a groups claim filter on the app plus group assignments.
- Put the issuer URL, client ID and client secret into `OIDC_ISSUER`,
  `OIDC_CLIENT_ID` and `OIDC_CLIENT_SECRET`, and the group names into
  `ROTTEN_UI_VIEWER_GROUP` and `ROTTEN_UI_ADMIN_GROUP`.

### Offline fake login, development only

With `RAILS_ENV=development`, `ROTTEN_UI_AUTH=oidc` and `OMNIAUTH_FAKE=1`,
`/login` adds **Sign in as fake viewer** and **Sign in as fake admin** buttons,
and the `OIDC_*` values become optional. The personas go through the same
provisioning and role rules as a real login, with groups taken from
`ROTTEN_UI_VIEWER_GROUP` and `ROTTEN_UI_ADMIN_GROUP`. If no admin group is set,
the admin persona comes out as a viewer. The dev compose stack sets all of this
up.

`OMNIAUTH_FAKE` does nothing in any other environment, or in password mode.
The route isn't drawn, and the controller refuses it as well.

### Password mode

`ROTTEN_UI_AUTH=password` needs no other settings. `/login` shows an email and
password form that POSTs to `/login`. There's no sign-up and no password reset
by email: an operator manages users with rake tasks, run where the app runs,
with the same `DATABASE_URL` and `ROTTEN_UI_AUTH=password`:

| Task | What it does |
| --- | --- |
| `bin/rails "users:create[alice@example.com,viewer]"` | Creates an active user with role `viewer` or `admin`, and prints a random 24-character password. |
| `bin/rails "users:disable[alice@example.com]"` | Sets `active` to false and ends every session the user has. |
| `bin/rails "users:enable[alice@example.com]"` | Sets `active` back to true, undoing `users:disable`. An already-enabled user stays enabled and the task still succeeds. Sessions from before the disable stay ended. |
| `bin/rails "users:reset_password[alice@example.com]"` | Sets and prints a new random password. The old one stops working, and every session the user has ends. A disabled user stays disabled. |

- **Passwords** are printed once and stored only as a bcrypt digest. Pass them
  on securely. There's no forced change at first login.
- **Audit log.** Each task that changes a user adds a row to `ui_audit_log`
  in the same transaction as the change: `user.create`, `user.disable`,
  `user.enable` or `user.reset_password`, with actor `rake` (no user id),
  the user's id, and their email (plus role and provider for `user.create`).
  Never a password. See [Audit log](#audit-log).
- **Changing your own password.** A signed-in password user can change their
  password at `/password`, linked from the home page as **Change password**.
  It asks for the current password, the new one and a confirmation. OIDC
  users, and everyone in `oidc` mode, get a 404 and no link. Every password,
  generated or chosen, must be at least 12 characters and at most 72 bytes;
  the rule lives in the
  `User` model. A wrong, missing or malformed current password gets one
  generic message and costs one bcrypt hash, like a failed login, and the
  new password is only checked after the current one. On success every
  session the user has ends, including a copy of this one's old cookie, and
  this browser gets a new session, with a new ID and a fresh lifetime, so it
  stays signed in.
  `PATCH /password` is rate limited like `POST /login`: 10 attempts per IP
  address and 5 per user in any 3 minutes, counted separately from login.
  Changes aren't written to `ui_audit_log`, which records admin actions.
- **Errors** exit non-zero with a message on stderr: a role other than
  `viewer` or `admin`, an invalid email, an email that already exists (in any
  case, OIDC users included), or an unknown user.
- **Modes.** `users:create` and `users:reset_password` refuse to run unless
  `ROTTEN_UI_AUTH=password`. `users:disable` and `users:enable` work in both
  modes, so `users:disable` is also the kill switch for OIDC users.
- **Which users can sign in.** Only users with `provider` set to `password`,
  which `users:create` sets, and `active` true. OIDC users never sign in by
  password. Emails are stored lowercased and matched without regard to case.
- **Failures** all get the same message and status: a wrong password, an
  unknown email, a disabled user and an OIDC user's email look the same. Each
  costs one bcrypt hash, so response time doesn't tell them apart either. A
  failed login ends any session the browser had.
- **Sessions.** As in OIDC mode, the session is reset before the user ID is
  stored, and `last_login_at` is updated. For password users the session also
  holds an HMAC of the password digest (keyed from `secret_key_base`, never
  the digest itself). Each request recomputes it; once the password changes it
  no longer matches, and the session is dropped and sent to `/login`, as for a
  disabled user. A password change or reset also bumps `session_generation`
  (below), which ends the same sessions; the HMAC stays as a backstop for a
  password set any other way, such as from a console.
- **Rate limits.** `POST /login` allows 10 attempts per IP address and 5 per
  email address in any 3 minutes, counting successes too. Past that, it
  answers 429 until the window ends. The counters live in `Rails.cache`, an
  in-memory store, so each app process counts on its own and restarts reset
  them. With several processes or hosts, the effective limit is multiplied by
  their number; a shared cache store would be needed to enforce it globally.
  The per-email limit means someone can lock a known email out for a few
  minutes at a time. The IP limit uses `request.remote_ip`, so behind a proxy
  make sure Rails sees the client's address, not the proxy's.

In `oidc` mode, `POST /login` returns 404 and `/login` shows no password form.
In `password` mode the OIDC routes return 404 and OmniAuth isn't installed.

### Sessions and revocation

Sessions live in the encrypted cookie, so the server can't delete one. At
sign-in the session stores the user's `users.session_generation` and an
absolute expiry, `ROTTEN_UI_SESSION_LIFETIME_HOURS` (12 by default) from
now. Every request already loads the user, and refuses the session, with no
extra query, when:

- the user is disabled, or (password users) the password HMAC doesn't match;
- the session's generation isn't the user's current one;
- the expiry has passed. Activity never extends it. Pages that need a login
  then redirect to `/login` with "Your session expired. Sign in again."

These bump the generation, in one `UPDATE ... SET
session_generation = session_generation + 1`, so concurrent bumps all count.
That ends every session the user has, on every device, copied cookies
included:

- **Signing out** ends all of the user's sessions, not just this browser's.
- **Changing or resetting a password.** The browser that changed it gets a
  new session with the new generation and stays signed in.
- **`users:disable`**, so `users:enable` can't bring old sessions back.
- **An OIDC login refused for lost group access**, for a user Rotten already
  knows. Group membership is otherwise only checked at sign-in, which the
  expiry forces at least every lifetime.

A session without a generation or an expiry, from before these existed,
counts as ended, so everyone signs in again once after upgrading.
`spec/security/session_revocation_spec.rb` and
`spec/security/session_expiry_spec.rb` cover all of this.

## Reports

`/reports` is the workbench, and any signed-in user, viewer or admin, can
use it. One form holds the dataset and the report to run on it:

- **Dataset:** a project, environment and cluster, optionally a role, and a
  **Time range**: a preset such as **Last 3 hours**, or **Custom**.
  **From** and **To** (UTC, at most 31 days apart) show only when Custom is
  picked, and picking it fills them with the window the preset had, so it
  can be nudged. Without JavaScript they stay visible, marked **Custom range
  only**. From and To sent with a preset range are ignored, and the page
  says so.
- **Report:** a list of every report with its description. The fields only
  some reports read show only for them: the utilization reports pick a
  primary and a replica role instead of one role, and the time series takes
  a fingerprint ID and a bucket width. Hidden fields are disabled, so they
  aren't sent. Without JavaScript every field shows and the server ignores
  the ones the picked report doesn't read. The role is part of the dataset: the
  utilization reports don't filter by it, but keep it (hidden) so switching
  back to another report doesn't widen the dataset.

**Run report** loads `/reports?report=<name>&...`. Every run states the
window it used, such as `2026-10-04 06:35 to 09:35 UTC (last 3 hours)`.
To switch reports, pick another one in the form and press **Run report**
again: the dataset stays as it was. The time series opened without a
fingerprint ID, as from an old bookmark, asks for one instead of failing.
An unknown report gets a 422. The old per-report
pages, `/reports/<name>?...`, redirect (301) to the workbench with every
parameter kept, so bookmarks still work.

Fingerprint IDs in the reports link to the fingerprint page. Click a column
header to sort. The form carries the sort with `sort_report`, the report
it came from, so running a different report drops it. The sort happens in
Ruby on the rows the report returned, at most 50 for the top and outlier
reports.

Each fingerprint has a page at `/fingerprints/<id>`. It shows the normalized
SQL, and after you pick a source and time range (with the same time window
behaviour as the workbench): charts of calls and total
time for each bucket, the top 10 contexts (job tag, or controller and
action), and stats for each logical source, including the per-call mean time
history kept in `fingerprint_stats`. Below them, an **All sources** row
shows the same stats over the same time range across every project,
environment, cluster and role, with the all-sources history (logical source
0 in `fingerprint_stats`), so the picked source compares with the
fingerprint's whole footprint. The charts are SVG drawn on the server;
each point carries `data-time`, `data-end` and `data-value`. A small
Stimulus controller, `app/javascript/controllers/chart_controller.js`, with
no chart library, adds to them:

- **Hover:** a vertical guide and a tooltip with the time and value of the
  nearest point, read from its data attributes, so there's no extra request.
- **Keyboard:** each chart is one Tab stop. Tab lands on the first point,
  or the one last focused, and the arrow keys, Home and End move between
  points. Each point has an `aria-label` with the tooltip's text, and
  focusing one shows its tooltip.
- **Zoom:** drag across a chart to load the page again with a **Custom**
  range from the start of the point nearest the left of the drag to the end
  of the point nearest the right, so the range stays inside the data and
  passes the usual range checks. A drag shorter than 4 pixels is a click and
  doesn't zoom. The range goes in the URL, so it can be shared and the back
  button works. The bucket stays as picked; with **Auto** it's re-picked for
  the narrower range. As with any range, only events whose observed window
  fits inside it count, so a bucket at the edge can show less than it did
  zoomed out.
- **Reset zoom:** the first zoom also puts the range from before it in
  `reset_range`, `reset_from` and `reset_to`, and later zooms keep it. The
  **Reset zoom** link goes back to that range, with the source, role and
  bucket as they are now. Those parameters get the same validation as the
  page's own; if they fail it, the page acts as if it weren't zoomed.
  Picking a range with the form starts over.

The controller only toggles classes and sets attributes and CSSOM
properties, so it works under the strict CSP with no inline script or style.
An unknown or malformed ID gets a 404. The page runs
`fingerprint_timeseries.sql`, `fingerprint_contexts.sql`,
`fingerprint_sources.sql` and `fingerprint_all_sources.sql`; the last three
aren't listed under `/reports`. They share one report timeout. The
all-sources query runs last; if it runs out of time, or less than 100ms is
left for it, the page still answers 200 with everything else and shows the
All sources row as timed out.

Each report is a SQL file in the repo's `reports/` directory, and the app
reads them from `../reports`, next to the app. `dev/docker-compose.yaml` and
`make test-ui` mount the directory at `/reports`. The production image needs
it as a named build context, run from the repo root:

```sh
docker build --build-context reports=reports -t rotten-ui ui
```

The app refuses to boot if any report file is missing.

Every parameter is checked against a whitelist before the query runs: the
source must exist in `logical_sources`, the range, bucket and sort column must
be one of the choices, and the fingerprint ID must be a positive integer.
Invalid input gets a 422 with a message. Values reach the SQL only as bound
parameters, never in its text.

Each report runs as `rotten_ui` in a read-only transaction with
`SET LOCAL statement_timeout`, so the limit ends with the transaction and
never applies to the next request on the same connection. A report that runs
past it is stopped, and the page answers 503 with a message suggesting a
shorter range or a narrower source. `ROTTEN_UI_REPORT_TIMEOUT` sets the
limit for the whole page: the fingerprint page's four queries share it, each
getting only what's left. See [`docs/ui.md`](../docs/ui.md#general).

## Pass keys

Admins manage worker pass keys at `/admin/keys`, linked from the home and
admin pages. Viewers get a 403 on every pass key route, and signed-out users
go to the login page.

- **List.** `/admin/keys` shows each key's id, name, pinned FQDN, who created
  it and when, when it was last used, and whether it's active or revoked.
  It never shows secrets; `rotten_ui` can't read `secret_hash`.
- **Create.** **New pass key** asks for a name and the FQDN of the worker
  host the key is pinned to. The name is 1 to 64 characters of letters,
  digits, `.`, `_` and `-`, starting with a letter or digit, and must be
  unused, even by a revoked key. The FQDN is lowercased, loses a trailing
  dot, and must be a host name of at most 253 characters. The response is a
  page holding the whole key, `rotten_<id>_<secret>`, which is the only time
  it's shown. It comes with `Cache-Control: no-store` and no redirect, and
  tells Turbo not to snapshot it, so the secret never goes in the flash, the
  session cookie, the browser's cache or Turbo's page cache. Only
  `sha256(secret)` is stored.
- **Revoke.** The **Revoke** button asks for confirmation. Revoking sets
  `revoked_at` and `revoked_by`. Revoking a key that's already revoked
  changes nothing and says so. The server caches pass key checks, so a
  revoked key keeps working for up to the server's fixed TTL of 30
  seconds.
- **Audit log.** Each create and revoke adds a row to `ui_audit_log`, with
  the admin's user id and email, the action (`api_key.create` or
  `api_key.revoke`), the key id, the time, and the key's name and FQDN.
  `rotten_ui` can only insert into and read that table. Admins read it at
  `/admin/audit`; see [Audit log](#audit-log).

`rotten_ui` can insert only `name`, `secret_hash`, `fqdn` and `created_by`
into `api_keys`, and update only `revoked_at` and `revoked_by`, so the models
are read-only and the writes are SQL files in `app/sql/`. The Go tests run
those same files as `rotten_ui` against the server's authenticator, and
`spec/fixtures/pass_key_vectors.json` holds known secrets with their hashes
and keys, checked by both the Go and Ruby tests. The `rotten-server keys`
CLI still works alongside the UI.

## Audit log

`rotten.ui_audit_log` records admin actions. The database stamps each row's
id and time, and `rotten_ui` can only insert and read, never change or delete.
Rows come from:

| Action | Actor | When |
| --- | --- | --- |
| `api_key.create`, `api_key.revoke` | The admin, by user id and email | An admin creates or revokes a pass key. |
| `user.create`, `user.disable`, `user.enable`, `user.reset_password` | `rake` | A `users:*` task changes a user. |
| `user.role_change` | `oidc` | An OIDC login changes a known user's role from their groups. Details hold `from` and `to`. |
| `user.access_lost` | `oidc` | A known user loses access: an OIDC login is refused because they're no longer in an allowed group, while they were active and their stored groups still gave them a role. See below. |

Creating a user at their first OIDC login, and a user changing their own
password, aren't recorded. `rotten-server keys` writes nothing here.

`user.access_lost` records the transition, not each refusal. The refused
login stores the new groups, so later refusals find the user already without
access and add nothing; a row is written again only after a login restores
access. Every refusal still ends the user's sessions. A user disabled with
`users:disable` had no access already, so losing their groups afterwards
records only any role change. Whether the stored groups gave a role is judged
by the current `ROTTEN_UI_VIEWER_GROUP` and `ROTTEN_UI_ADMIN_GROUP`, so
renaming a group in the config can hide a loss.

Admins read the log at `/admin/audit`, linked from the home and admin pages.
Viewers get a 403 and signed-out users go to the login page. It shows 50
entries a page, newest first by id, with each entry's time in UTC, actor,
action, target and details as JSON, all HTML-escaped, since details can hold
strings users chose. **Older** pages back with `?before=<id>`, keyset style on
the primary key; a `before` that isn't a positive integer of at most 18 digits
gets a 400.

## Production settings

[`docs/ui.md`](../docs/ui.md) covers the production image and its required
settings: `DATABASE_URL`, `SECRET_KEY_BASE` and `ROTTEN_UI_HOSTS`.

With `RAILS_ENV=production` the app refuses to boot without
`ROTTEN_UI_HOSTS`. A request whose `Host` or `X-Forwarded-Host` isn't listed
gets a 403. That blocks DNS rebinding, and keeps a forged host out of the URLs
the app builds, such as the OIDC redirect URI when `OIDC_REDIRECT_URI` is
unset. `/up` is exempt, so health checks can use an IP address.

## Security

`spec/security/` holds the security specs, and `make test-ui` runs them with
everything else.

- **CSRF.** Every route other than GET and HEAD is checked automatically,
  including routes added later: each must refuse a missing, made-up or
  foreign token, and a request from another origin. The OmniAuth request
  phase, `POST /auth/openid_connect`, is checked the same way.
- **Sessions.** The session cookie is encrypted, `HttpOnly` and
  `SameSite=Lax`, and `Secure` in production, where HSTS is sent too. Sign-in
  issues a new session, so a session planted before sign-in is useless.
  Sign-out ends every session the user has, including copies of the cookie,
  and every session expires a fixed time after sign-in; see
  [Sessions and revocation](#sessions-and-revocation).
- **Headers.** A strict Content-Security-Policy: everything from this origin
  only; scripts and styles also need the per-request nonce that importmap's
  tags carry; no plugins, no framing, and forms may post only to this origin,
  to the issuer's origin in `oidc` mode, and to any origins in
  `ROTTEN_UI_CSP_FORM_ACTION_ORIGINS`, for the redirects to the identity
  provider. Also
  `X-Frame-Options: DENY`, `X-Content-Type-Options: nosniff`,
  `Referrer-Policy: strict-origin-when-cross-origin`, and a
  `Permissions-Policy` that turns off camera, microphone, geolocation, payment
  and similar features. `spec/security/csp_browser_spec.rb` checks the policy
  in Chromium.
- **Static analysis.** Brakeman must report no warnings, and bundler-audit
  must find no gem with a known advisory. The specs also fail if
  `config/brakeman.ignore`, `config/brakeman.yml` or `.bundler-audit.yml`
  exists, since any of them could quietly suppress findings. bundler-audit's
  advisory database is downloaded when the dev image is built, so the specs
  run offline; rebuild the image with `--no-cache` to pick up new advisories.
