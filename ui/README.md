# Rotten UI

Rails 8.1 on Ruby 3.4. Ruby runs only in Docker: `make test-ui` runs the specs
against a migrated rotten database, and `dev/docker-compose.yaml` runs the app
at `http://localhost:3000`. `rotten-server migrate` owns the schema; Rails never
runs migrations.

## Authentication

`ROTTEN_UI_AUTH` picks the mode, `oidc` or `password`. The app refuses to boot
if it's missing or anything else. Every user is a `viewer` or an `admin`. A
user whose `active` flag is false is locked out, whatever the identity
provider says.

### OIDC mode

Every org-specific value comes from env. None has a default in this repo.

| Variable | Required | Meaning |
| --- | --- | --- |
| `OIDC_ISSUER` | yes | Issuer URL. Endpoints and keys come from its `/.well-known/openid-configuration`. |
| `OIDC_CLIENT_ID` | yes | Client ID of the app registered with the identity provider. |
| `OIDC_CLIENT_SECRET` | yes | Its client secret. |
| `OIDC_GROUPS_CLAIM` | no, default `groups` | Claim, in the ID token or userinfo, that lists the user's groups. |
| `ROTTEN_UI_VIEWER_GROUP` | no | Group whose members may sign in. If unset, any authenticated user is a viewer. |
| `ROTTEN_UI_ADMIN_GROUP` | no | Group whose members are admins. **If unset, nobody is an admin through OIDC.** |
| `OIDC_REDIRECT_URI` | no | Full callback URL. If unset, it's built from the request as `<scheme>://<host>/auth/openid_connect/callback`. Set it when the app sits behind a proxy that changes the host. |
| `OIDC_SCOPES` | no, default `openid email profile` | Space-separated scopes to request. `openid` is always added. Add `groups` when the provider needs that scope to send the groups claim, as Okta does; providers that don't know a `groups` scope, such as Google, reject the login with `invalid_scope` if it's requested. |
| `ROTTEN_UI_CSP_FORM_ACTION_ORIGINS` | no | Comma-separated extra origins, such as `https://login.example.com`, that the browser may be redirected to after **Sign in**. See below. |

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
  isn't valid UTF-8. A missing claim therefore gives a viewer at most, never
  an admin.
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
  groups loses admin in the database straight away. That doesn't end sessions
  they already have; those last until they sign out or `active` is cleared.
- **Sessions.** The session is reset before the user ID is stored, which
  prevents session fixation.
- **Failures** from the provider or OmniAuth (a denied consent, a bad state,
  an unreachable issuer) land on `/auth/failure`, which redirects to `/login`
  with a generic message. Details go to the log only.

### Identity provider app checklist

The deploy repo does the real setup with its own values. The provider app
needs:

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
| `bin/rails "users:disable[alice@example.com]"` | Sets `active` to false. The user's sessions end on their next request. |
| `bin/rails "users:reset_password[alice@example.com]"` | Sets and prints a new random password. The old one stops working, and the user's existing sessions end on their next request. A disabled user stays disabled. |

- **Passwords** are printed once and stored only as a bcrypt digest. Pass them
  on securely. There's no page yet for users to change their own password, and
  no forced change at first login.
- **Errors** exit non-zero with a message on stderr: a role other than
  `viewer` or `admin`, an invalid email, an email that already exists (in any
  case, OIDC users included), or an unknown user.
- **Modes.** `users:create` and `users:reset_password` refuse to run unless
  `ROTTEN_UI_AUTH=password`. `users:disable` works in both modes, so it's also
  the kill switch for OIDC users.
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
  disabled user. OIDC sessions carry no such check and last until the user is
  disabled or signs out.
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

## Production settings

| Variable | Required | Meaning |
| --- | --- | --- |
| `ROTTEN_UI_HOSTS` | yes, in production | Comma-separated host names the app answers to, such as `rotten.example.com`. A name starting with a dot, such as `.example.com`, also allows its subdomains. |

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
  Sign-out clears the cookie in the browser, but the session lives entirely in
  the cookie, so a copy taken before sign-out keeps working until the user is
  disabled or, for password users, the password changes. Sessions don't
  expire on their own either.
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
