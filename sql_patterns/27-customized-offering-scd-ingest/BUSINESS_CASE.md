# Business case: Offer Tool SCD ingest

Field sales and product specialists capture recommendations, contact
events, custom menus, and place linkages in an OLTP Offer Tool
(MySQL on Cloud SQL). Downstream analytics — Food Graph masterdata,
Adobe COF activity scores, segment change reporting — need a
warehouse copy that answers both "what is current?" and "what changed
last Tuesday?"

A nightly truncate-reload of trusted would lose that history. CDC /
Datastream was not on the table when this landed: the product DB is
shared, export IAM already existed, and ops wanted a pattern they
could reason about in Airflow without a second streaming stack.

So we ship a daily Cloud SQL CSV export per table, land it in the raw
zone, and apply classic SCD Type 2 in BigQuery using hashes computed
at extract time. Fifteen tables share one DAG so schedule, IAM, and
failure email stay in one place.

Tradeoffs I accepted:

- **Sequential Cloud SQL exports.** Fifteen concurrent dumps would
  spike I/O on the product instance during breakfast-prep for sales
  tooling. Serialize exports; let each table's BQ chain fan out.
- **Hash in MySQL, not BigQuery.** MD5 over key and payload columns
  rides with the CSV. BQ only compares hash pairs — cheaper than
  re-hashing wide comment / JSON fields after load.
- **`trigger_rule=all_done`.** A failed early table does not cancel
  later exports. Incomplete days are visible as failed tasks; we do
  not silently drop the rest of the book.
- **Parse-time `date.today()` in GCS paths.** Operational smell on
  long retries; left as production behaved. Prefer `{{ ds }}` on the
  next rewrite.

Not inventing ROI numbers — the value is operability: one Composer
DAG, auditable history, and analytics teams that stop querying the
product MySQL replica directly.
