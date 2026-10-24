"""SQL builder for daily Adobe marketing UTM attribution append.

Reads hit-level Adobe datafeed rows for one visit date and a single
website product type, then projects conversion-funnel flags, referral
channel flags, and UTM eVars into a refined append table.

Source (read-only): dags/adobe_marketing_utm.py
"""

from __future__ import annotations


def build_insert_sql(
    project_id: str,
    refined_dataset: str,
    source_table: str = "adobe_datafeed",
    target_table: str = "adobe_marketing_utm",
    product_type: str = "PLATFORM",
    visit_date_expr: str = "CURRENT_DATE()",
) -> str:
    """Return the INSERT…SELECT used by the daily attribution task.

    Production shipped with CURRENT_DATE() and bare INSERT (no
    delete/MERGE). Prefer `{{ ds }}` (quoted) for backfill-safe runs
    when you modernize — keep the default here for fidelity to source.
    """
    source = f"`{project_id}.{refined_dataset}.{source_table}`"
    target = f"`{project_id}.{refined_dataset}.{target_table}`"
    product = product_type.replace("'", "''")

    return f"""
INSERT INTO {target}
    (visit_id, visit_start_time_gmt_dt, visitor_id, page_url, pagename,
     reached_pricing_page, BT_click_request_demo, BT_click_Form_submit,
     hit_date_time, Inside_your_site, Other_web_sites, Search_engines,
     Typed_Bookmarked_no_referrer, Social_Networks, prop_traffic_page_url,
     var_interaction_marketing_source, var_interaction_marketing_medium,
     var_interaction_marketing_campaigns, visit_page_num)
SELECT
    visit_id,
    visit_start_time_gmt_dt,
    CONCAT(visid_high, "_", visid_low) AS visitor_id,
    page_url,
    pagename,
    IF(CONTAINS_SUBSTR(page_url, "pricing"), visit_id, NULL)
      AS reached_pricing_page,
    IF(var_event_name = "button_click"
       AND var_event_value = "Request Demo", visit_id, NULL)
      AS BT_click_request_demo,
    IF(CONTAINS_SUBSTR(prop_traffic_page_url, "pricing")
       AND var_event_name = "form_submit_success"
       AND var_event_value = "Submit", visit_id, NULL)
      AS BT_click_Form_submit,
    hit_date_time,
    IF(referrer_type_id = 1, visit_id, NULL) AS Inside_your_site,
    IF(referrer_type_id = 2, visit_id, NULL) AS Other_web_sites,
    IF(referrer_type_id = 3, visit_id, NULL) AS Search_engines,
    IF(referrer_type_id = 6, visit_id, NULL) AS Typed_Bookmarked_no_referrer,
    IF(referrer_type_id = 9, visit_id, NULL) AS Social_Networks,
    prop_traffic_page_url,
    var_interaction_marketing_source,
    var_interaction_marketing_medium,
    var_interaction_marketing_campaigns,
    visit_page_num
FROM {source}
WHERE visit_start_time_gmt_dt = {visit_date_expr}
  AND UPPER(var_product_type) = "{product}"
""".strip()
