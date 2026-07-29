"""
Query helpers for independent-establishment menu-gaps event export.

Source tables: refined.menu_gaps_independent_{iso}
ISO suffix matches the refined per-country models.

Unlike pattern 12 (ranked wholesale-account gaps with article/rank
columns), this feed ships establishment address + contact + gap
fields for establishments that are *not* on the wholesale account
book — a different partner contract and Avro schema.

The DAG export path uses menu_gaps_indep_export._build_query (adds
FARM_FINGERPRINT MOD partitioning). This class is the simpler
full-country SELECT for smoke checks and ad-hoc backfills.
"""

from __future__ import annotations

import logging

# Active markets for this feed. Production started with one market;
# extend the list when refined tables + schema registration land.
ACTIVE_ISO_CODES = ["es"]


class MenuGapsIndependent:
    """Lowercase ISO codes aligned with refined.menu_gaps_independent_<cc>."""

    country_iso_codes = list(ACTIVE_ISO_CODES)

    @staticmethod
    def get_send_query(iso_code_lower: str, full_load: bool = False) -> str:
        """
        Build SELECT for one country's refined table.

        When full_load=False (default), only rows updated in the last
        24h (D-1) are selected — matches the monthly DAG's filter.
        """
        if (
            not iso_code_lower
            or iso_code_lower.lower() not in MenuGapsIndependent.country_iso_codes
        ):
            raise ValueError(
                f"iso_code_lower must be one of {MenuGapsIndependent.country_iso_codes}, "
                f"got {iso_code_lower!r}"
            )
        cc = iso_code_lower.lower()
        query = f"""
        SELECT
            establishment_id,
            iso_code,
            establishment_name,
            postal_code,
            city,
            street_name,
            street_number,
            address,
            geo_lat,
            geo_long,
            google_places_id,
            phone,
            email,
            website,
            establishment_type,
            cuisine_type,
            menu_type,
            menu_item_name,
            ingredient,
            CAST(created_at AS STRING) AS created_at,
            FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%E*S', _update_ts) AS _update_ts
        FROM `refined.menu_gaps_independent_{cc}`
        """
        if not full_load:
            query += """
        WHERE _update_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
            """
        logging.getLogger(__name__).info(
            "MenuGapsIndependent.get_send_query suffix=%s full_load=%s", cc, full_load
        )
        return query


if __name__ == "__main__":
    print(MenuGapsIndependent.get_send_query("es")[:320])
