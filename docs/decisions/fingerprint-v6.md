# Fingerprints after the pg_query_go v6 upgrade.

Date: October 1, 2026. Task: 20261001-103222-6.

## Decision.

We moved from `pg_query_go/v5` v5.1.0 (Postgres 16 parser) to `pg_query_go/v6` v6.2.5 (Postgres 17 parser). We didn't use a Postgres 18 parser release.

## What changed in the golden file.

No fingerprints changed. `make golden` rewrote one line, the module header:

```
-# github.com/pganalyze/pg_query_go/v5 v5.1.0
+# github.com/pganalyze/pg_query_go/v6 v6.2.5
```

All 39 corpus cases gave the same output as v5. That includes the three parse-error cases and their error text.

## Groupings still make sense.

Every value is the same, so every grouping is the same as under v5. Here's how the corpus groups now:

- **Should share, and do.** Each of these sets shares one fingerprint: `literal_a`/`literal_b`, the three `in_list_*` cases, the three `marginalia_*` cases, `pgss_normalized_param`, `values_a`/`values_b`, `schema_a`/`schema_b`, `repack_index_a`/`_b`, `repack_table_a`/`_b`, `set_role_a`/`_b`, `cursor_declare_*`, `cursor_fetch_*`, `cursor_close_*`, `temp_table_create_*`, and `temp_table_select_*`.
- **Shouldn't share, and don't.** `literal_other_column`, `literal_other_table`, `reset_role`, `repack_named_index`, `temp_table_short_suffix`, and `multi_statement` each get their own fingerprint.
- **Parse errors.** `parse_error_garbage`, `parse_error_truncated`, and `parse_error_pgss_ellipsis` still fail with `failed to parse`.

`schema_unqualified` shares a fingerprint with `literal_a`. That was already true under v5, so it isn't new with this upgrade.

## Reflectwalk skip list.

The skip list in `fingerprint.go` still matches the v6 internals:

- All 276 generated messages in v6.2.5 have the same three private fields: `state protoimpl.MessageState`, `sizeCache protoimpl.SizeCache`, and `unknownFields protoimpl.UnknownFields`.
- In `google.golang.org/protobuf` v1.36.12, `MessageState` still embeds `NoUnkeyedLiterals`, `DoNotCompare`, and `DoNotCopy`, and it still holds `atomicMessageInfo`.
- The fields the walker rewrites (`Rolename`, `HowMany`, `Idxname`, `Relname`, `Portalname`, and `Schemaname`) appear the same number of times in v5 and v6.

## Build.

pg_query_go v6 builds natively on macOS (arm64, Go 1.27), so `make test-unit` now runs natively instead of in Docker.
