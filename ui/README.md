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

In `oidc` mode the app refuses to boot if `OIDC_ISSUER`, `OIDC_CLIENT_ID` or
`OIDC_CLIENT_SECRET` is missing or blank. The error names the missing
variables, never their values.

The login uses the authorization code flow with PKCE, a nonce and a state
parameter. `/login` shows a **Sign in** button that POSTs to
`/auth/openid_connect` with a CSRF token. A GET there does nothing.

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
  empty or very long, or contain NUL bytes or invalid UTF-8. A missing claim
  therefore gives a viewer at most, never an admin.
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

`password` mode doesn't have a login yet.
