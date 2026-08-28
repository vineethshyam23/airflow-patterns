"""
MySQL export SELECT statements for the Offer Tool SCD ingest.

Each query:
- projects business columns plus audit stamps
- sets `_sourcesystem` so SCD updates stay scoped to this feed
- computes `_keyhash` / `_rowhash` in MySQL (MD5) so BigQuery only
  compares hashes — no re-hash of wide JSON/text columns in BQ

Comment / free-text fields replace newlines with ` [cr] ` so the
Cloud SQL CSV export does not split rows.

Source (read-only):
  dags/horeca_digital/customized_offering_queries.py
  (export-query section only; zone / Elasticsearch queries excluded)
"""

# Product-need assessments captured by field sales in the Offer Tool.
data_product_need_query = """SELECT
            IFNULL(establishment_id,'') AS establishment_id,
            IFNULL(google_places_id, '') AS google_places_id,
            REPLACE(REPLACE(TRIM(IFNULL(comment, '')), CHAR(10), ' [cr] '), CHAR(3), ' [cr] ') AS comment,
            created_at, updated_at,
            current_timestamp AS _create_ts, '' AS _update_ts, '' AS _job_name,
            0 AS _job_id, 'OfferTool' AS _sourcesystem,
            MD5(CONCAT(IFNULL(establishment_id, ''), IFNULL(google_places_id, ''))) AS _keyhash,
            MD5(CONCAT(data, IFNULL(comment, ''), CAST(created_at AS CHAR), CAST(updated_at AS CHAR))) AS _rowhash
        FROM data_product_need"""

restaurant_contact_event_query = """SELECT
            id,
            IFNULL(establishment_id,'') AS establishment_id,
            user_id,
            IFNULL(contacted_at, '') AS contacted_date,
            created_at,
            current_timestamp AS _create_ts, '' AS _update_ts, '' AS _job_name, 0 AS _job_id,
            'OfferTool' AS _sourcesystem,
            MD5(CONCAT(IFNULL(establishment_id, ''), CAST(user_id AS CHAR))) AS _keyhash,
            MD5(CONCAT(
                        CAST(IFNULL(contacted_at , '19700101') AS CHAR),
                        CAST(created_at AS CHAR))) AS _rowhash
        FROM restaurant_contact_event"""

followup_reminder_query = """SELECT
            IFNULL(establishment_id,'') AS establishment_id,
            user_id,
            IFNULL(followup_at, '') AS followup_reminder,
            created_at,
            current_timestamp AS _create_ts, '' AS _update_ts, '' AS _job_name, 0 AS _job_id,
            'OfferTool' AS _sourcesystem,
            MD5(CONCAT(IFNULL(establishment_id, ''), CAST(user_id AS CHAR))) AS _keyhash,
            MD5(CONCAT(
                        CAST(IFNULL(followup_at , '19700101') AS CHAR),
                        CAST(created_at AS CHAR))) AS _rowhash
        FROM followup_reminder"""

error_report_query = """SELECT
            id, IFNULL(establishment_id,'') AS establishment_id,
            IFNULL(google_places_id, '') AS google_places_id, user_id, item_reference, `type`,
            `options`,
            REPLACE(REPLACE(TRIM(IFNULL(comment, '')), CHAR(10), ' [cr] '), CHAR(3), ' [cr] ') AS comment,
            IFNULL(system_config, '') AS system_config, created_at,
            current_timestamp AS _create_ts, '' AS _update_ts,
            '' AS _job_name, 0 AS _job_id, 'OfferTool' AS _sourcesystem,
            MD5(CONCAT(IFNULL(establishment_id, ''), IFNULL(google_places_id, ''), CAST(user_id AS CHAR))) AS _keyhash,
            MD5(CONCAT(item_reference, `type`, `options`, IFNULL(comment, ''), IFNULL(system_config, ''),
                       CAST(created_at AS CHAR))) AS _rowhash
        FROM error_report"""

file_query = """SELECT id, user_id, filename, mime_type, created_at, updated_at, visibility,
                       current_timestamp AS _create_ts, '' AS _update_ts, '' AS _job_name, 0 AS _job_id,
                       'OfferTool' AS _sourcesystem, MD5(CAST(id AS CHAR)) AS _keyhash,
                       MD5(CONCAT(CAST(user_id AS CHAR), filename, mime_type, CAST(created_at AS CHAR),
                                  CAST(updated_at AS CHAR), CAST(visibility AS CHAR))) AS _rowhash
                FROM file"""

state_query = """SELECT
            id, IFNULL(establishment_id,'') AS establishment_id, IFNULL(google_places_id, '') AS google_places_id,
            user_id, product_id, state, IFNULL(reason, '') AS reason, created_at, updated_at,
            current_timestamp AS _create_ts, '' AS _update_ts, '' AS _job_name, 0 AS _job_id,
            'OfferTool' AS _sourcesystem,
            MD5(CONCAT(IFNULL(establishment_id, ''), IFNULL(google_places_id, ''), CAST(user_id AS CHAR), product_id)) AS _keyhash,
            MD5(CONCAT(CAST(state AS CHAR), IFNULL(reason, ''), CAST(created_at AS CHAR),
            CAST(updated_at AS CHAR))) AS _rowhash
        FROM product_recommendation_state"""

alternatives_query = """SELECT
            IFNULL(establishment_id,'') AS establishment_id, IFNULL(google_places_id, '') AS google_places_id, user_id,
            existing_product, suggested_product, created_at,
            current_timestamp AS _create_ts, '' AS _update_ts, '' AS _job_name,
            0 AS _job_id, 'OfferTool' AS _sourcesystem,
            MD5(CONCAT(IFNULL(establishment_id, ''), IFNULL(google_places_id, ''), CAST(user_id AS CHAR))) AS _keyhash,
            MD5(CONCAT(existing_product, suggested_product, CAST(created_at AS CHAR))) AS _rowhash
        FROM product_suggested_alternative"""

comment_query = """SELECT
            id, IFNULL(establishment_id,'') AS establishment_id, IFNULL(google_places_id, '') AS google_places_id,
            user_id,
            REPLACE(REPLACE(TRIM(IFNULL(comment, '')), CHAR(10), ' [cr] '), CHAR(3), ' [cr] ') AS comment,
            visibility, created_at, updated_at,
            current_timestamp AS _create_ts, '' AS _update_ts, '' AS _job_name,
            0 AS _job_id, 'OfferTool' AS _sourcesystem,
            MD5(CONCAT(IFNULL(establishment_id, ''), IFNULL(google_places_id, ''), CAST(user_id AS CHAR))) AS _keyhash,
            MD5(CONCAT(CAST(visibility AS CHAR), IFNULL(comment, ''), CAST(created_at AS CHAR), CAST(updated_at AS CHAR))) AS _rowhash
        FROM restaurant_comment"""

comment_file_query = """SELECT comment_id, file_id,
                               current_timestamp AS _create_ts, '' AS _update_ts, '' AS _job_name,
                               0 AS _job_id, 'OfferTool' AS _sourcesystem,
                               MD5(CAST(comment_id AS CHAR)) AS _keyhash,
                               MD5(CONCAT(CAST(file_id AS CHAR))) AS _rowhash
                        FROM restaurant_comment_file"""

menu_query = """SELECT
            id, IFNULL(establishment_id,'') AS establishment_id, IFNULL(google_places_id, '') AS google_places_id,
            user_id, created_at, updated_at,
            current_timestamp AS _create_ts, '' AS _update_ts, '' AS _job_name,
            0 AS _job_id, 'OfferTool' AS _sourcesystem,
            MD5(CONCAT(IFNULL(establishment_id, ''), IFNULL(google_places_id, ''),
            CAST(user_id AS CHAR))) AS _keyhash,
            MD5(CONCAT(CAST(created_at AS CHAR), CAST(updated_at AS CHAR))) AS _rowhash
        FROM restaurant_custom_menu"""

menu_file_query = """SELECT custom_menu_id, file_id,
                            current_timestamp AS _create_ts, '' AS _update_ts, '' AS _job_name,
                            0 AS _job_id, 'OfferTool' AS _sourcesystem,
                            MD5(CAST(custom_menu_id AS CHAR)) AS _keyhash,
                            MD5(CONCAT(CAST(file_id AS CHAR))) AS _rowhash
                     FROM restaurant_custom_menu_file"""

user_query = """SELECT id, IFNULL(username, ''), IFNULL(current_language, ''), type, country, created_at,
            current_timestamp AS _create_ts, '' AS _update_ts, '' AS _job_name, 0 AS _job_id,
            'OfferTool' AS _sourcesystem,
            MD5(CONCAT(CAST(id AS CHAR), IFNULL(username, ''))) AS _keyhash,
            MD5(CONCAT(IFNULL(current_language, ''), CAST(type AS CHAR), CAST(country AS CHAR), CAST(created_at AS CHAR))) AS _rowhash
        FROM user"""

restaurant_segment_update = """
                            SELECT
                                id,
                                IFNULL(establishment_id, '') AS establishment_id,
                                IFNULL(country, '') AS country,
                                IFNULL(old_segment, '') AS old_segment,
                                IFNULL(new_segment, '') AS new_segment,
                                IFNULL(segment_group, '') AS segment_group,
                                user_id,
                                created_at,
                                updated_at,
                                current_timestamp AS _create_ts,
                                '' AS _update_ts,
                                '' AS _job_name,
                                0 AS _job_id,
                                'OfferTool' AS _sourcesystem,
                                MD5(
                                    CONCAT(
                                        CAST(id AS CHAR),
                                        IFNULL(establishment_id, ''),
                                        IFNULL(country, ''),
                                        CAST(user_id AS CHAR)
                                    )
                                ) AS _keyhash,
                                MD5(
                                    CONCAT(
                                        CAST(created_at AS CHAR),
                                        CAST(updated_at AS CHAR)
                                    )
                                ) AS _rowhash
                            FROM restaurant_segment_update
                            """

restaurant_details_update = """
                        SELECT
                            CAST(id AS CHAR) AS id,
                            user_id,
                            IFNULL(google_places_id, '') AS google_places_id,
                            IFNULL(field_id, '') AS field_id,
                            IFNULL(operation, '') AS operation,
                            IFNULL(old_value, '') AS old_value,
                            IFNULL(value, '') AS value,
                            created_at,
                            updated_at,
                            IFNULL(value_type, '') AS value_type,
                            IFNULL(establishment_id, '') AS establishment_id,
                            IFNULL(wholesale_id, '') AS wholesale_id,
                            IFNULL(country, '') AS country,
                            current_timestamp AS _create_ts,
                            '' AS _update_ts,
                            '' AS _job_name,
                            0 AS _job_id,
                            'OfferTool' AS _sourcesystem,
                            MD5(
                                CONCAT(
                                    CAST(id AS CHAR),
                                    CAST(user_id AS CHAR),
                                    IFNULL(google_places_id, ''),
                                    IFNULL(field_id, ''),
                                    IFNULL(operation, ''),
                                    IFNULL(old_value, ''),
                                    IFNULL(value, ''),
                                    IFNULL(value_type, ''),
                                    IFNULL(establishment_id, ''),
                                    IFNULL(wholesale_id, ''),
                                    IFNULL(country, '')
                                )
                            ) AS _keyhash,
                            MD5(
                                CONCAT(
                                    CAST(created_at AS CHAR),
                                    CAST(updated_at AS CHAR)
                                )
                            ) AS _rowhash
                            FROM restaurant_details_update
                            """

google_places = """
                        SELECT
    CAST(id AS CHAR) AS id,
    IFNULL(establishment_id, '') AS establishment_id,
    IFNULL(place_id, '') AS place_id,
    IFNULL(name, '') AS name,
    IFNULL(address_components, '') AS address_components,
    IFNULL(business_status, '') AS business_status,
    IFNULL(formatted_address, '') AS formatted_address,
    IFNULL(formatted_phone_number, '') AS formatted_phone_number,
    IFNULL(geometry, '') AS geometry,
    IFNULL(international_phone_number, '') AS international_phone_number,
    IFNULL(opening_hours, '') AS opening_hours,
    CAST(rating AS CHAR) AS rating,
    IFNULL(types, '') AS types,
    IFNULL(url, '') AS url,
    IFNULL(website, '') AS website,
    current_timestamp AS _create_ts,
    '' AS _update_ts,
    '' AS _job_name,
    0 AS _job_id,
    'OfferTool' AS _sourcesystem,
    MD5(
        CONCAT(
            CAST(id AS CHAR),
            IFNULL(establishment_id, '')
        )
    ) AS _keyhash,
    MD5(
        CONCAT(
            IFNULL(place_id, ''),
            IFNULL(name, ''),
            IFNULL(address_components, ''),
            IFNULL(business_status, ''),
            IFNULL(formatted_address, ''),
            IFNULL(formatted_phone_number, ''),
            IFNULL(geometry, ''),
            IFNULL(international_phone_number, ''),
            IFNULL(opening_hours, ''),
            CAST(rating AS CHAR),
            IFNULL(types, ''),
            IFNULL(url, ''),
            IFNULL(website, '')
        )
    ) AS _rowhash
FROM google_places
                            """

# Same table set for prod and non-prod — only connection / suffix differ.
TABLE_NAMES = [
    "data_product_need",
    "restaurant_contact_event",
    "followup_reminder",
    "error_report",
    "file",
    "product_recommendation_state",
    "product_suggested_alternative",
    "restaurant_comment",
    "restaurant_comment_file",
    "restaurant_custom_menu",
    "restaurant_custom_menu_file",
    "user",
    "restaurant_segment_update",
    "restaurant_details_update",
    "google_places",
]

EXPORT_QUERIES = [
    data_product_need_query,
    restaurant_contact_event_query,
    followup_reminder_query,
    error_report_query,
    file_query,
    state_query,
    alternatives_query,
    comment_query,
    comment_file_query,
    menu_query,
    menu_file_query,
    user_query,
    restaurant_segment_update,
    restaurant_details_update,
    google_places,
]

assert len(TABLE_NAMES) == len(EXPORT_QUERIES)
