"""
SQL helpers for the Odoo WSL invoices dual-export DAG.

Keeps the trusted-table SELECT out of the DAG body so the Composer file
stays readable and the same filter is reused for both the event ingest and
the recommender APPEND.

Source (read-only): inline SQL in dags/etl_dana_odoo_wsl_invoices_export.py
"""


class OdooWslInvoices:
    """Static builders for the trusted WSL invoice SELECT."""

    TRUSTED_TABLE = "`trusted.int_odoo_wsl_invoices`"

    @staticmethod
    def get_send_query() -> str:
        """
        Full-table send for the event API.

        Filter is uni_key IS NOT NULL only — no date delta. The trusted
        intermediate is expected to already be a current snapshot after the
        upstream dbt job. Volume grows with billing history; if cost becomes
        the constraint, add a booking_date window here rather than inventing
        CDC on Odoo.
        """
        return f"""
SELECT *
FROM {OdooWslInvoices.TRUSTED_TABLE}
WHERE uni_key IS NOT NULL
""".strip()

    @staticmethod
    def get_recommender_copy_query() -> str:
        """Same rowset as send — recommender and event bus must stay aligned."""
        return OdooWslInvoices.get_send_query()


if __name__ == "__main__":
    print("--- send ---")
    print(OdooWslInvoices.get_send_query())
    print("--- recommender ---")
    print(OdooWslInvoices.get_recommender_copy_query())
