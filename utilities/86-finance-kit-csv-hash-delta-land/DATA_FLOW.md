# Data flow: Finance-kit CSV land + hash-delta

## Run order (per entity)

1. Vendor writes `landingzone/finance_kit/{entity}_{tomorrow_ds}.csv`
   (semicolon, header row, quoted newlines allowed).
2. At 23:30, Composer starts `etl_finance_kit` and fans out ten chains.
3. `list_landing_*` lists the landing prefix (soft check).
4. `copy_to_raw_*` copies to
   `rawzone/finance_kit/{entity}/{tomorrow_ds}/{entity}.csv`.
5. `load_staging_*` loads CSV → `trusted_staging.finance_kit_{entity}`
   with `WRITE_TRUNCATE` using `schema_json/finance_kit_{entity}.json`.
6. `insert_trusted_*` appends rows whose `_keyhash|_rowhash` pair is
   new in `trusted.finance_kit_{entity}`.
7. `remove_landing_*` deletes the landing object.

## Path contract

| Stage | Path / table |
|-------|----------------|
| Landing | `finance_kit/{entity}_{tomorrow_ds}.csv` |
| Rawzone | `finance_kit/{entity}/{tomorrow_ds}/{entity}.csv` |
| Schema | `schema_json/finance_kit_{entity}.json` |
| Staging | `{project}.trusted_staging.finance_kit_{entity}` |
| Trusted | `{project}.trusted.finance_kit_{entity}` |

CSV dialect: delimiter `;`, skip header, `allow_quoted_newlines=True`.

## Entities

`angaben`, `bwa`, `bwa_types`, `doctrine_migration_versions`,
`eingabe`, `import`, `kosten`, `project`, `umsatz`, `user`.

Stems match the vendor file names. Renaming them invents a second
contract — keep them unless the vendor renames the drop.

## Hash-delta semantics

```
staging (full file, truncated)
  → filter: concat(_keyhash,'|',_rowhash) not in trusted
  → stamp: _create_ts, _job_*, _sourcesystem, _valid_from/until/flag
  → APPEND trusted
```

- Identical file re-run → zero new trusted rows (idempotent append).
- Updated business row with new `_rowhash` → second trusted version;
  prior row keeps `_valid_flag=True` and far-future `_valid_until`.
- No MERGE, no prior-version close, no dbt snapshot in this DAG.

## Idempotency and recovery

| Situation | Recovery |
|-----------|----------|
| Insert failed mid-run | Landing still present; clear failed task and re-run chain |
| Need full trusted rebuild | Truncate trusted table; re-process rawzone dates or re-drop |
| Wrong tomorrow_ds / late drop | Wait for file or trigger with the matching logical date |
| Schema drift | Fix JSON on load bucket; re-run load + insert |

## Failure modes

| Failure | Effect | What to do |
|---------|--------|------------|
| Empty landing prefix | List green; copy fails | Wait for vendor; add real sensor if SLA matters |
| Bad CSV / wrong delimiter | Load fails | Fix drop or operator dialect; landing kept |
| Missing `_keyhash`/`_rowhash` | Insert SQL fails or wrong dedupe | Fix schema + upstream hash columns |
| Partial entity failures | Mixed green/red night | Re-run failed task ids only |
| Accidental early landing delete | Lost drop; need vendor re-send or rawzone replay | Never move delete before insert |

## Scale notes

Ten small master / ledger CSVs — parallelism is for isolation, not
slot pressure. If an entity grows into multi-GB nightly dumps, move
that stem to its own DAG with file-size gates rather than widening
this loop. The `NOT IN (SELECT concat…)` anti-join is fine while
trusted stays in the low millions of hash pairs; past that, prefer
a staging LEFT ANTI JOIN against a clustered hash table or a MERGE
on the hash key.
