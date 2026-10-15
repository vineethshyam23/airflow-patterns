"""Medallia GraphQL feedback extract → GCS CSV.

OAuth2 client-credentials against the vendor token endpoint, cursor-
paginated feedback query (100 nodes/page, newest response_date first),
hash columns for SCD2, then CSV upload to rawzone.

Credentials come from Airflow Variable `medallia_creds`
({"client_id": "...", "client_secret": "..."}). Production originally
read a JSON file from the Composer data volume.

Source (read-only):
  dags/horeca_digital/medallia.py
"""

from __future__ import annotations

import csv
import hashlib
import json
import logging
import os
import sys
from collections import OrderedDict
from datetime import date, datetime, timedelta
from time import mktime

import google.cloud.storage as storage
import pandas as pd
import requests
from oauthlib.oauth2 import BackendApplicationClient
from requests_oauthlib import OAuth2Session

# Sanitized endpoints — production used tenant-specific Medallia hosts.
QUERY_API_URL = os.environ.get(
    "MEDALLIA_QUERY_URL",
    "https://apis.example-medallia.com/data/v0/query",
)
OAUTH_TOKEN_ENDPOINT = os.environ.get(
    "MEDALLIA_TOKEN_URL",
    "https://example.medallia.eu/oauth/tenant/token",
)

TOKEN_FILE_PATH = "medallia_token.json"
TMP_LOCATION = "/tmp/"
DEFAULT_NUM_NODES_PER_REQUEST = 100
MAX_NUM_ITERATIONS = 2000


def _feedback_attributes():
    """Map vendor field ids → (warehouse_column, values|labels).

    Field ids are anonymized. The OrderedDict order is the CSV column
    order and must stay aligned with the BigQuery schema object.
    """
    attrs = OrderedDict()
    # Establishment / company identity (switched from user_id in 2026-05)
    attrs["e_establishment_id"] = ("establishment_id", "values")
    attrs["e_company_country_isocode"] = ("user_country_iso_code", "labels")
    attrs["e_user_language_iso_code"] = ("user_language_iso_code", "values")
    attrs["k_product_name"] = ("product_name", "labels")
    attrs["q_likelihood_to_recommend_scale11"] = ("nps_value", "values")
    attrs["q_promoter_main_reason_alt"] = ("promoter_reason_alt", "labels")
    attrs["q_promoter_main_reason_other_cmt"] = (
        "promoter_reason_comment",
        "values",
    )
    attrs["a_translation_to_english_promoter_main_reason_other_cmt"] = (
        "english_translation_promoter_reason_comment",
        "values",
    )
    attrs["q_detractor_main_reason_alt"] = ("detractor_reason_alt", "labels")
    attrs["q_detractor_main_reason_other_cmt"] = (
        "detractor_reason_comment",
        "values",
    )
    attrs["a_translation_to_english_detractor_main_reason_other_cmt"] = (
        "english_translation_detractor_reason_comment",
        "values",
    )
    attrs["q_additional_comment_cmt"] = ("additional_comment", "values")
    attrs["e_responsedate"] = ("response_date", "values")
    attrs["q_churn_initial_choice_alt"] = ("churn_initial_choice_alt", "labels")
    attrs["q_churn_leaving_choice_alt"] = ("churn_leaving_choice_alt", "labels")
    attrs["q_churn_leaving_choice_cmt"] = (
        "churn_leaving_choice_comment",
        "values",
    )
    attrs["a_translation_to_english_churn_leaving_choice_cmt"] = (
        "english_translation_churn_leaving_choice_cmt",
        "values",
    )
    attrs["q_churn_willingness_call_alt"] = (
        "churn_willingness_call_alt",
        "labels",
    )
    attrs["q_churn_willingness_stay_alt"] = (
        "churn_willingness_stay_alt",
        "labels",
    )
    attrs["q_downgrade_main_reason_alt"] = (
        "downgrade_main_reason_alt",
        "labels",
    )
    attrs["q_downgrade_main_reason_other_cmt"] = (
        "downgrade_main_reason_other_comment",
        "values",
    )
    attrs["a_translation_to_english_downgrade_main_reason_other_cmt"] = (
        "english_translation_downgrade_main_reason_other_cmt",
        "values",
    )
    attrs["a_translation_to_english_additional_comment_cmt"] = (
        "english_translation_additional_comment_cmt",
        "values",
    )
    attrs["a_surveyid"] = ("unique_survey_id", "values")
    # Custom parameter — production field id was a numeric Medallia id
    attrs["e_text_custom_parameter"] = ("text_custom_parameter", "values")
    return attrs


def _feedback_attributes_id():
    return _feedback_attributes().keys()


def _feedback_query(*, number_of_nodes=DEFAULT_NUM_NODES_PER_REQUEST, cursor=None):
    cursor_string = "" if cursor is None else 'after:"{}", '.format(cursor)
    feedback_args = "({}first:{}".format(cursor_string, number_of_nodes)
    feedback_args = (
        feedback_args
        + ', orderBy:{direction:DESC, fieldId: "e_responsedate"})'
    )

    attribute_ids_string = '["' + '", "'.join(_feedback_attributes_id()) + '"]'
    field_data_list = (
        " fieldDataList(fieldIds: " + attribute_ids_string + ") "
    )

    feedback_string = "feedback{}".format(feedback_args)
    return (
        "query "
        "{"
        + feedback_string
        + "{"
        " pageInfo{endCursor hasNextPage}"
        " totalCount"
        " nodes "
        " {"
        " id"
        + field_data_list
        + " {"
        " field {id name dataType description}"
        " labels"
        " values"
        " }"
        " }"
        " }"
        " }"
    )


def _access_token(*, client_id: str, client_secret: str):
    now_unix_time = mktime(datetime.now().timetuple())
    token_file = TMP_LOCATION + TOKEN_FILE_PATH
    if not os.path.exists(token_file):
        _create_initial_token_file(token_file, now_unix_time)

    with open(token_file, "r") as json_file:
        token_data = json.load(json_file)

        expiry = datetime.fromtimestamp(
            token_data.get("expires_at", now_unix_time)
        ) - timedelta(minutes=5)
        medal_access_token = token_data.get("access_token")
        if expiry <= datetime.now():
            client = BackendApplicationClient(client_id=client_id)
            oauth = OAuth2Session(client=client)
            token = oauth.fetch_token(
                token_url=OAUTH_TOKEN_ENDPOINT,
                client_id=client_id,
                client_secret=client_secret,
            )
            medal_access_token = token.get("access_token")
            with open(token_file, "w") as new_json_file:
                token_data = {
                    "expires_at": token.get("expires_at"),
                    "access_token": medal_access_token,
                }
                json.dump(token_data, new_json_file)
    return medal_access_token if medal_access_token is not None else ""


def _create_initial_token_file(json_file_path, unix_time):
    with open(json_file_path, "w+") as token_file:
        token_data = {"expires_at": unix_time, "access_token": ""}
        json.dump(token_data, token_file)


def _get_field_value_or_label(field):
    field_id = field.get("field").get("id")
    value_type = _feedback_attributes().get(field_id)[1]
    return field.get(value_type, [])


def _add_nodes_to_dataframe(df, nodes):
    rows = [node.get("fieldDataList", []) for node in nodes]
    rows = [
        [next(iter(_get_field_value_or_label(field)), None) for field in fields]
        for fields in rows
    ]
    new_df = pd.DataFrame(
        rows, columns=[col[0] for col in _feedback_attributes().values()]
    )
    # GraphQL can omit optional fields; keep CSV column stable.
    if "additional_comment" not in new_df.columns:
        new_df["additional_comment"] = ""

    current_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    new_df["_create_ts"] = current_timestamp
    new_df["_update_ts"] = ""
    new_df["_job_name"] = ""
    new_df["_job_id"] = 0
    new_df["_sourcesystem"] = "Medallia"
    new_df["_keyhash"] = (
        new_df["establishment_id"].fillna("N/A") + new_df["response_date"]
    )

    new_df["_rowhash"] = (
        new_df["user_country_iso_code"].fillna("")
        + "|"
        + new_df["user_language_iso_code"].fillna("")
        + "|"
        + new_df["product_name"].fillna("")
        + "|"
        + new_df["nps_value"].fillna("")
        + "|"
        + new_df["promoter_reason_alt"].fillna("")
        + "|"
        + new_df["promoter_reason_comment"].fillna("")
        + "|"
        + new_df["english_translation_promoter_reason_comment"].fillna("")
        + "|"
        + new_df["detractor_reason_alt"].fillna("")
        + "|"
        + new_df["detractor_reason_comment"].fillna("")
        + "|"
        + new_df["english_translation_detractor_reason_comment"].fillna("")
        + "|"
        + new_df["additional_comment"].fillna("")
        + "|"
        + new_df["churn_initial_choice_alt"].fillna("")
        + "|"
        + new_df["churn_leaving_choice_alt"].fillna("")
        + "|"
        + new_df["churn_leaving_choice_comment"].fillna("")
        + "|"
        + new_df["english_translation_churn_leaving_choice_cmt"].fillna("")
        + "|"
        + new_df["churn_willingness_call_alt"].fillna("")
        + "|"
        + new_df["churn_willingness_stay_alt"].fillna("")
        + "|"
        + new_df["downgrade_main_reason_alt"].fillna("")
        + "|"
        + new_df["downgrade_main_reason_other_comment"].fillna("")
        + "|"
        + new_df["english_translation_downgrade_main_reason_other_cmt"].fillna(
            ""
        )
        + "|"
        + new_df["english_translation_additional_comment_cmt"].fillna("")
        + "|"
        + new_df["unique_survey_id"].fillna("")
        + "|"
        + new_df["text_custom_parameter"].fillna("")
    )
    new_df["_keyhash"] = new_df["_keyhash"].apply(
        lambda x: hashlib.md5(x.encode("utf-8")).hexdigest()
    )
    new_df["_rowhash"] = new_df["_rowhash"].apply(
        lambda x: hashlib.md5(x.encode("utf-8")).hexdigest()
    )
    return pd.concat([df, new_df])


def _resolve_creds(creds: dict | None) -> tuple[str, str]:
    if creds:
        return creds.get("client_id", ""), creds.get("client_secret", "")
    try:
        from airflow.models import Variable

        payload = Variable.get("medallia_creds", deserialize_json=True)
        return payload.get("client_id", ""), payload.get("client_secret", "")
    except Exception:
        return "", ""


def extract_data(
    *,
    destination_bucket: str,
    destination_file: str,
    oldest_record_allowed: date,
    connection_json: dict | None = None,
    creds: dict | None = None,
    gcp_project: str = "dwh_project",
):
    """Paginate Medallia feedback and land a headerless CSV in GCS.

    `connection_json` is accepted for DAG parity; production extract
    used the default ADC storage client and ignored the hook extras.
    """
    del connection_json  # kept for call-site compatibility

    client_id, client_secret = _resolve_creds(creds)
    if not client_secret:
        raise ValueError(
            "Medallia client_secret missing — set Variable medallia_creds"
        )

    log_format = (
        "%(asctime)s %(levelname)s %(filename)s:%(lineno)d %(message)s"
    )
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format=log_format)
    logging.getLogger().info("Starting Medallia feedback extract")
    storage_client = storage.Client(project=gcp_project)

    has_next_page = True
    column_names = [attr[0] for attr in _feedback_attributes().values()]
    column_names.extend(
        [
            "_create_ts",
            "_update_ts",
            "_job_name",
            "_job_id",
            "_sourcesystem",
            "_keyhash",
            "_rowhash",
        ]
    )
    df = pd.DataFrame(columns=column_names)
    today = datetime.now().date()
    oldest_record_date = today

    iteration_counter = 0
    end_cursor = None
    while (
        has_next_page
        and iteration_counter < MAX_NUM_ITERATIONS
        and oldest_record_date > oldest_record_allowed
    ):
        access_token = _access_token(
            client_id=client_id, client_secret=client_secret
        )
        query = _feedback_query(cursor=end_cursor)
        query_dict = {"query": query, "variables": {}}
        response = requests.post(
            QUERY_API_URL,
            headers={
                "Authorization": "Bearer " + str(access_token),
                "Content-Type": "application/json",
            },
            data=json.dumps(query_dict),
            timeout=120,
        )
        json_dict = response.json()

        data = json_dict.get("data")
        errors = json_dict.get("errors", {})
        if data is not None:
            page_info = data.get("feedback", {}).get("pageInfo", {})
            end_cursor = page_info.get("endCursor")
            nodes = data.get("feedback", {}).get("nodes", [])
            df = _add_nodes_to_dataframe(df, nodes)
            oldest_record_date = datetime.strptime(
                min(df["response_date"]), "%Y-%m-%d %H:%M:%S"
            ).date()
            has_next_page = page_info.get("hasNextPage", False)
        else:
            raise Exception(str(errors))
        iteration_counter += 1

    df.replace(r"\n", " ", regex=True, inplace=True)

    bucket = storage_client.get_bucket(destination_bucket)
    blob = bucket.blob(destination_file)
    data_str = df.to_csv(
        encoding="utf-8", index=False, quoting=csv.QUOTE_ALL, header=False
    )
    blob.upload_from_string(data_str, "text/csv")
    logging.getLogger().info(
        "Completed Medallia extract: %s rows", df.shape[0]
    )


def main():
    """Local smoke path — requires creds + ADC; not used in Composer."""
    request_period_days = 30
    loaddate = date.today().strftime("%Y-%m-%d")
    today = datetime.now().date()
    oldest_record_allowed = today - timedelta(days=request_period_days)

    # Prefer env JSON for local runs; never hardcode secrets.
    raw = os.environ.get("MEDALLIA_CREDS_JSON", "{}")
    creds = json.loads(raw)
    extract_data(
        destination_bucket=os.environ.get("MEDALLIA_BUCKET", "rawzone_dev"),
        destination_file=f"medallia/medallia_{loaddate}.csv",
        oldest_record_allowed=oldest_record_allowed,
        creds=creds,
        gcp_project=os.environ.get("GCP_PROJECT", "dwh_project_dev"),
    )


if __name__ == "__main__":
    main()
