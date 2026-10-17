"""
Mailchimp Marketing API client for campaign analytics extraction.

Nightly pull of six report entities: campaign list, campaign reports, click
details, unsubscribes, email activity, and recipients. Each entity writes
JSONL to a Composer local path for downstream GCS / BigQuery loading.

Source (read-only):
  dags/horeca_digital/mailchimp.py

Sanitized: project IDs, table names, merge field keys, removed hardcoded keys.
"""

import json
import logging
import math
from datetime import date

import mailchimp_marketing as MailchimpMarketing
from google.cloud import bigquery
from mailchimp_marketing.api_client import ApiClientError

# Generic Mailchimp audience merge-field keys used in production.
MERGE_CUSTOMER_ID = "CUST_ID"
MERGE_SEGMENT = "SEGMENT"
MERGE_COMPANY = "COMPANY"
MERGE_ACCOUNT_MANAGER = "ACCT_MGR"
MERGE_POSTAL_CODE = "POSTCODE"
MERGE_DEVICE_ID = "DEVICE_ID"
MERGE_REGISTRATION = "REG_ID"


class MailChimp:
    """Base class for Mailchimp API calls and JSONL extraction."""

    def __init__(self, api_key, server, project_id):
        self.api_key = api_key
        self.server = server
        self.project_id = project_id

    def _conn(self):
        """Establish connection to the Mailchimp Marketing API."""
        client = MailchimpMarketing.Client()
        client.set_config({"api_key": self.api_key, "server": self.server})
        self.client = client
        return self.client

    def _query_results(self):
        """Return campaign IDs sent within the last 90 days from staging."""
        query = f"""
            SELECT DISTINCT campaign_id
            FROM `{self.project_id}.trusted_staging.mailchimp_campaign_list`
            WHERE DATE(sent_time) >= CURRENT_DATE() - 90
        """
        client = bigquery.Client(project=self.project_id)
        query_job = client.query(query)
        campaign_list = [row.campaign_id for row in query_job.result()]
        logging.info("Total number of campaigns: %s", len(campaign_list))
        return campaign_list


class CampaignList(MailChimp):
    """Fetch and flatten the campaigns list endpoint."""

    def get_campaigns_list(self, offset=0, count=100):
        try:
            client = super()._conn()
            return client.campaigns.list(count=count, offset=offset)
        except ApiClientError as error:
            return f"Error: {error.text}"

    def fetch_campaigns_list(self, campaign_list_loc, offset=0, count=100):
        try:
            with open(campaign_list_loc + "campaign_list.json", "w") as outfile:
                logging.info("Getting Campaign List")
                response = self.get_campaigns_list()
                logging.info("Total items: %s", response["total_items"])
                total_pages = math.ceil(int(response["total_items"]) / int(count))

                for _page_num in range(0, total_pages):
                    logging.info("Fetching page %s/%s", _page_num + 1, total_pages)
                    response = self.get_campaigns_list(offset=int(offset), count=int(count))
                    for row in response["campaigns"]:
                        tdata = {
                            "campaign_id": row.get("id"),
                            "web_id": row.get("web_id"),
                            "type": row.get("type"),
                            "create_time": row.get("create_time"),
                            "status": row.get("status"),
                            "email_sent": row.get("emails_sent"),
                            "sent_time": row.get("send_time"),
                            "content_type": row.get("content_type"),
                            "resendable": row.get("resendable"),
                            "list_id": row.get("recipients", {}).get("list_id"),
                            "list_is_active": row.get("recipients", {}).get("list_is_active"),
                            "list_name": row.get("recipients", {}).get("list_name"),
                            "segment_text": row.get("recipients", {}).get("segment_text"),
                            "recipient_count": row.get("recipients", {}).get("recipient_count"),
                            "open": row.get("report_summary", {}).get("opens"),
                            "unique_opens": row.get("report_summary", {}).get("unique_opens"),
                            "open_rate": row.get("report_summary", {}).get("open_rate"),
                            "clicks": row.get("report_summary", {}).get("clicks"),
                            "subscriber_clicks": row.get("report_summary", {}).get("subscriber_clicks"),
                            "click_rate": row.get("report_summary", {}).get("click_rate"),
                            "total_orders": row.get("report_summary", {})
                            .get("ecommerce", {})
                            .get("total_orders"),
                            "total_spent": row.get("report_summary", {})
                            .get("ecommerce", {})
                            .get("total_spent"),
                            "total_revenue": row.get("report_summary", {})
                            .get("ecommerce", {})
                            .get("total_revenue"),
                            "load_date": date.today().strftime("%Y-%m-%d"),
                        }
                        outfile.write(json.dumps(tdata) + "\n")
                    offset = offset + count
        except Exception as exc:
            logging.info("Error while getting Campaign list: %s", exc)


class CampaignReports(MailChimp):
    """Fetch per-campaign report summaries."""

    def get_campaign_report(self, campaign_id="abc123def4"):
        try:
            client = super()._conn()
            return client.reports.get_campaign_report(campaign_id)
        except ApiClientError as error:
            return f"Error: {error.text}"

    def fetch_campaign_report(self, campaign_reports_loc):
        counter = 1
        campaign_list = super()._query_results()

        for campaign in campaign_list:
            for attempt in range(10):
                try:
                    path = campaign_reports_loc + f"campaign_reports_{campaign}.json"
                    with open(path, "w") as outfile:
                        logging.info(
                            "Attempt %s - #%s: Campaign reports Campaign:%s",
                            attempt,
                            counter,
                            campaign,
                        )
                        row = self.get_campaign_report(campaign_id=campaign)
                        tdata = {
                            "campaign_id": row.get("id"),
                            "campaign_title": row.get("campaign_title"),
                            "type": row.get("type"),
                            "list_id": row.get("list_id"),
                            "list_is_active": row.get("list_is_active"),
                            "list_name": row.get("list_name"),
                            "subject_line": row.get("subject_line"),
                            "emails_sent": row.get("emails_sent"),
                            "abuse_reports": row.get("abuse_reports"),
                            "unsubscribed": row.get("unsubscribed"),
                            "send_time": row.get("send_time"),
                            "hard_bounces": row.get("bounces", {}).get("hard_bounces"),
                            "soft_bounces": row.get("bounces", {}).get("soft_bounces"),
                            "bounces_syntax_errors": row.get("bounces", {}).get("syntax_errors"),
                            "forwards_count": row.get("forwards", {}).get("forwards_count"),
                            "forwards_opens": row.get("forwards", {}).get("forwards_opens"),
                            "open_total": row.get("opens", {}).get("opens_total"),
                            "unique_opens": row.get("opens", {}).get("unique_opens"),
                            "open_rate": row.get("opens", {}).get("open_rate"),
                            "clicks_total": row.get("clicks", {}).get("clicks_total"),
                            "unique_clicks": row.get("clicks", {}).get("unique_clicks"),
                            "unique_subscriber_clicks": row.get("clicks", {})
                            .get("unique_subscriber_clicks"),
                            "click_rate": row.get("clicks", {}).get("click_rate"),
                            "recipient_likes": row.get("facebook_likes", {}).get("recipient_likes"),
                            "unique_likes": row.get("facebook_likes", {}).get("unique_likes"),
                            "facebook_likes": row.get("facebook_likes", {}).get("facebook_likes"),
                            "industry_type": row.get("industry_stats", {}).get("type"),
                            "industry_open_rate": row.get("industry_stats", {}).get("open_rate"),
                            "industry_click_rate": row.get("industry_stats", {}).get("click_rate"),
                            "industry_bounce_rate": row.get("industry_stats", {}).get("bounce_rate"),
                            "industry_unopen_rate": row.get("industry_stats", {}).get("unopen_rate"),
                            "industry_unsub_rate": row.get("industry_stats", {}).get("unsub_rate"),
                            "industry_abuse_rate": row.get("industry_stats", {}).get("abuse_rate"),
                            "list_sub_rate": row.get("list_stats", {}).get("sub_rate"),
                            "list_unsub_rate": row.get("list_stats", {}).get("unsub_rate"),
                            "list_open_rate": row.get("list_stats", {}).get("open_rate"),
                            "list_click_rate": row.get("list_stats", {}).get("click_rate"),
                            "load_date": date.today().strftime("%Y-%m-%d"),
                        }
                        outfile.write(json.dumps(tdata) + "\n")
                    counter = counter + 1
                except Exception as exc:
                    logging.info("Error while getting Campaign reports: %s", exc)
                else:
                    break
            else:
                logging.info("All attempts completed for campaign %s", campaign)


class ClickReport(MailChimp):
    """Fetch per-campaign URL click details."""

    def get_click_report(self, campaign_id="abc123def4", offset=0, count=1000):
        try:
            client = super()._conn()
            return client.reports.get_campaign_click_details(
                campaign_id, count=count, offset=offset
            )
        except ApiClientError as error:
            return f"Error: {error.text}"

    def fetch_click_report(self, click_report_loc):
        counter = 1
        campaign_list = super()._query_results()

        for campaign in campaign_list:
            for attempt in range(10):
                try:
                    offset = 0
                    count = 1000
                    path = click_report_loc + f"click_report_{campaign}.json"
                    with open(path, "w") as outfile:
                        response = self.get_click_report(campaign_id=campaign)
                        total_pages = math.ceil(response["total_items"] / count)
                        logging.info(
                            "Attempt %s - #%s: Click report Campaign:%s items:%s",
                            attempt,
                            counter,
                            campaign,
                            response["total_items"],
                        )
                        for _page_num in range(0, total_pages):
                            response = self.get_click_report(
                                campaign_id=campaign, offset=offset, count=count
                            )
                            for row in response["urls_clicked"]:
                                tdata = {
                                    "url_id": row.get("id"),
                                    "url": row.get("url"),
                                    "total_clicks": row.get("total_clicks"),
                                    "click_percentage": row.get("click_percentage"),
                                    "unique_clicks": row.get("unique_clicks"),
                                    "unique_click_percentage": row.get("unique_click_percentage"),
                                    "last_click": row.get("last_click"),
                                    "campaign_id": row.get("campaign_id"),
                                    "load_date": date.today().strftime("%Y-%m-%d"),
                                }
                                outfile.write(json.dumps(tdata) + "\n")
                            offset = offset + count
                    counter = counter + 1
                except Exception as exc:
                    logging.info("Error while getting Click reports: %s", exc)
                else:
                    break
            else:
                logging.info("All attempts completed for campaign %s", campaign)


class Unsubscribes(MailChimp):
    """Fetch per-campaign unsubscribe events."""

    def get_unsubscribes(self, campaign_id="abc123def4", offset=0, count=1000):
        try:
            client = super()._conn()
            return client.reports.get_unsubscribed_list_for_campaign(
                campaign_id, count=count, offset=offset
            )
        except ApiClientError as error:
            return f"Error: {error.text}"

    def fetch_unsubscribes(self, unsubscribes_loc):
        counter = 1
        campaign_list = super()._query_results()

        for campaign in campaign_list:
            for attempt in range(10):
                try:
                    offset = 0
                    count = 1000
                    path = unsubscribes_loc + f"unsubscribes_{campaign}.json"
                    with open(path, "w") as outfile:
                        response = self.get_unsubscribes(campaign_id=campaign)
                        total_pages = math.ceil(response["total_items"] / count)
                        logging.info(
                            "Attempt %s - #%s: Unsubscribes Campaign:%s items:%s",
                            attempt,
                            counter,
                            campaign,
                            response["total_items"],
                        )
                        for _page_num in range(0, total_pages):
                            response = self.get_unsubscribes(
                                campaign_id=campaign, offset=offset, count=count
                            )
                            for row in response["unsubscribes"]:
                                merge = row.get("merge_fields", {})
                                tdata = {
                                    "email_id": row.get("email_id"),
                                    "email_address": row.get("email_address"),
                                    "first_name": merge.get("FNAME"),
                                    "last_name": merge.get("LNAME"),
                                    "address": str(merge.get("ADDRESS")),
                                    "customer_id": merge.get(MERGE_CUSTOMER_ID),
                                    "segment": merge.get(MERGE_SEGMENT),
                                    "company_name": merge.get(MERGE_COMPANY),
                                    "account_manager": merge.get(MERGE_ACCOUNT_MANAGER),
                                    "postal_code": merge.get(MERGE_POSTAL_CODE),
                                    "device_id": merge.get(MERGE_DEVICE_ID),
                                    "registration_id": merge.get(MERGE_REGISTRATION),
                                    "vip": row.get("vip"),
                                    "unsubscribe_ts": row.get("timestamp"),
                                    "reason": row.get("reason"),
                                    "campaign_id": row.get("campaign_id"),
                                    "list_id": row.get("list_id"),
                                    "list_is_active": row.get("list_is_active"),
                                    "load_date": date.today().strftime("%Y-%m-%d"),
                                }
                                outfile.write(json.dumps(tdata) + "\n")
                            offset = offset + count
                    counter = counter + 1
                except Exception as exc:
                    logging.info("Error while getting Unsubscribe list: %s", exc)
                else:
                    break
            else:
                logging.info("All attempts completed for campaign %s", campaign)


class EmailActivity(MailChimp):
    """Fetch per-campaign email activity (opens, clicks, etc.)."""

    def get_email_activity(self, campaign_id="abc123def4", offset=0, count=1000):
        try:
            client = super()._conn()
            return client.reports.get_email_activity_for_campaign(
                campaign_id, count=count, offset=offset
            )
        except ApiClientError as error:
            return f"Error: {error.text}"

    def fetch_email_activity(self, email_activity_loc):
        counter = 1
        campaign_list = super()._query_results()

        for campaign in campaign_list:
            for attempt in range(10):
                try:
                    offset = 0
                    count = 1000
                    path = email_activity_loc + f"email_activity_{campaign}.json"
                    with open(path, "w") as outfile:
                        response = self.get_email_activity(campaign_id=campaign)
                        total_pages = math.ceil(response["total_items"] / count)
                        logging.info(
                            "Attempt %s - #%s: Email activity Campaign:%s items:%s",
                            attempt,
                            counter,
                            campaign,
                            response["total_items"],
                        )
                        for _page_num in range(0, total_pages):
                            response = self.get_email_activity(
                                campaign_id=campaign, offset=offset, count=count
                            )
                            for row in response["emails"]:
                                tdata = {
                                    "campaign_id": row.get("campaign_id"),
                                    "list_id": row.get("list_id"),
                                    "email_address": row.get("email_address"),
                                    "activity": row.get("activity"),
                                    "load_date": date.today().strftime("%Y-%m-%d"),
                                }
                                outfile.write(json.dumps(tdata) + "\n")
                            offset = offset + count
                    counter = counter + 1
                except Exception as exc:
                    logging.info("Error while getting Email activity: %s", exc)
                else:
                    break
            else:
                logging.info("All attempts completed for campaign %s", campaign)


class Recipients(MailChimp):
    """Fetch per-campaign recipient delivery status."""

    def get_recipients(self, campaign_id="abc123def4", offset=0, count=1000):
        try:
            client = super()._conn()
            return client.reports.get_campaign_recipients(
                campaign_id, count=count, offset=offset
            )
        except ApiClientError as error:
            return f"Error: {error.text}"

    def fetch_recipients(self, recipients_loc):
        counter = 1
        campaign_list = super()._query_results()

        for campaign in campaign_list:
            for attempt in range(10):
                try:
                    offset = 0
                    count = 1000
                    path = recipients_loc + f"recipients_{campaign}.json"
                    with open(path, "w") as outfile:
                        response = self.get_recipients(campaign_id=campaign)
                        total_pages = math.ceil(response["total_items"] / count)
                        logging.info(
                            "Attempt %s - #%s: Recipients Campaign:%s items:%s",
                            attempt,
                            counter,
                            campaign,
                            response["total_items"],
                        )
                        for _page_num in range(0, total_pages):
                            response = self.get_recipients(
                                campaign_id=campaign, offset=offset, count=count
                            )
                            for row in response["sent_to"]:
                                merge = row.get("merge_fields", {})
                                tdata = {
                                    "email_id": row.get("email_id"),
                                    "email_address": row.get("email_address"),
                                    "first_name": merge.get("FNAME"),
                                    "last_name": merge.get("LNAME"),
                                    "address": str(merge.get("ADDRESS")),
                                    "customer_id": merge.get(MERGE_CUSTOMER_ID),
                                    "segment": merge.get(MERGE_SEGMENT),
                                    "company_name": merge.get(MERGE_COMPANY),
                                    "account_manager": merge.get(MERGE_ACCOUNT_MANAGER),
                                    "postal_code": merge.get(MERGE_POSTAL_CODE),
                                    "device_id": merge.get(MERGE_DEVICE_ID),
                                    "registration_id": merge.get(MERGE_REGISTRATION),
                                    "vip": row.get("vip"),
                                    "status": row.get("status"),
                                    "open_count": row.get("open_count"),
                                    "last_open": row.get("last_open"),
                                    "absplit_group": row.get("absplit_group"),
                                    "campaign_id": row.get("campaign_id"),
                                    "list_id": row.get("list_id"),
                                    "list_is_active": row.get("list_is_active"),
                                    "load_date": date.today().strftime("%Y-%m-%d"),
                                }
                                outfile.write(json.dumps(tdata) + "\n")
                            offset = offset + count
                    counter = counter + 1
                except Exception as exc:
                    logging.info("Error while getting Recipients list: %s", exc)
                else:
                    break
            else:
                logging.info("All attempts completed for campaign %s", campaign)
