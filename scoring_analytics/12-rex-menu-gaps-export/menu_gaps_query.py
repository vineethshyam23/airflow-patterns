"""
Query helpers for ranked menu-gaps event export.

Source tables: refined.menu_gaps_ranked_{iso} (dbt per-country models).
ISO suffix matches dbt model names (de, fr, nl, es, pl, hr, it, pt).

The DAG export path uses menu_gaps_export._build_query (adds
FARM_FINGERPRINT MOD partitioning). This class is the simpler
full-country SELECT used for smoke checks and ad-hoc backfills.
"""

from __future__ import annotations

import logging


class MenuGapsRanked:
    """Lowercase ISO codes aligned with refined.menu_gaps_ranked_<cc>."""

    country_iso_codes = ["de", "fr", "nl", "es", "pl", "hr", "it", "pt"]

    @staticmethod
    def get_send_query(iso_code_lower: str, full_load: bool = False) -> str:
        """
        Build SELECT for one country's refined table.

        When full_load=False (default), only rows updated in the last
        24h (D-1) are selected — matches the monthly DAG's filter.
        """
        if not iso_code_lower or iso_code_lower.lower() not in MenuGapsRanked.country_iso_codes:
            raise ValueError(
                f"iso_code_lower must be one of {MenuGapsRanked.country_iso_codes}, "
                f"got {iso_code_lower!r}"
            )
        cc = iso_code_lower.lower()
        query = f"""
        SELECT
            wholesale_id,
            iso_code,
            establishment_id,
            ingredient,
            `type`,
            menu_type,
            menu_item_name,
            relevance,
            branch_desc,
            article_no,
            variant_tu_key,
            department_flag,
            product_key,
            article_name,
            one_year_revenue,
            rank_,
            account_id,
            person_id,
            cardholder_key,
            customer_key,
            unique_wholesale_id,
            FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%E*S', _update_ts) AS _update_ts
        FROM `refined.menu_gaps_ranked_{cc}`
        """
        if not full_load:
            query += """
        WHERE _update_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
            """
        logging.getLogger(__name__).info(
            "MenuGapsRanked.get_send_query suffix=%s full_load=%s", cc, full_load
        )
        return query


if __name__ == "__main__":
    print(MenuGapsRanked.get_send_query("de")[:280])
