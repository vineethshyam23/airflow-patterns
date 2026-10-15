"""Mailchimp Marketing API client — campaign list + per-campaign report extracts.

Pulls six grains as JSONL onto the Composer data volume:
  campaign_list, campaign_reports, click_report, unsubscribes,
  email_activity, recipients.

Campaign-scoped reports resolve IDs from trusted_staging.mailchimp_campaign_list
(sent in the last 90 days). Each campaign call retries up to 10 times before
moving on — vendor timeouts on activity/recipients are common at scale.

Source (read-only): dags/horeca_digital/mailchimp.py
"""

from __future__ import annotations

import json
import logging
import math
from datetime import date

import mailchimp_marketing as MailchimpMarketing
from google.cloud import bigquery
from mailchimp_marketing.api_client import ApiClientError

# Max attempts per campaign for report endpoints (inherited production knob).
CAMPAIGN_FETCH_ATTEMPTS = 10


class MailChimp:
    """Shared connection + BigQuery campaign-id lookup."""

    def __init__(self, api_key: str, server: str, project_id: str):
        self.api_key = api_key
        self.server = server
        self.project_id = project_id
        self.client = None

    def _conn(self):
        client = MailchimpMarketing.Client()
        client.set_config({"api_key": self.api_key, "server": self.server})
        self.client = client
        return self.client

    def _query_results(self):
        """Campaign IDs sent in the last 90 days (drives report fan-out)."""
        query = """
            SELECT DISTINCT campaign_id
            FROM `trusted_staging.mailchimp_campaign_list`
            WHERE DATE(sent_time) >= CURRENT_DATE() - 90
        """
        client = bigquery.Client(project=self.project_id)
        campaign_list = [row.campaign_id for row in client.query(query).result()]
        logging.info("Total campaigns in window: %s", len(campaign_list))
        return campaign_list


class CampaignList(MailChimp):
    """Paginated /campaigns list → single JSONL file."""

    def get_campaigns_list(self, offset: int = 0, count: int = 100):
        try:
            return self._conn().campaigns.list(count=count, offset=offset)
        except ApiClientError as error:
            logging.error("Campaign list API error: %s", error.text)
            raise

    def fetch_campaigns_list(self, campaign_list_loc: str, offset: int = 0, count: int = 100):
        try:
            with open(campaign_list_loc + "campaign_list.json", "w") as outfile:
                logging.info("Getting campaign list")
                response = self.get_campaigns_list()
                total_items = int(response["total_items"])
                total_pages = math.ceil(total_items / int(count))
                logging.info("Total items: %s (%s pages)", total_items, total_pages)

                for page_num in range(total_pages):
                    logging.info("Campaign list page %s/%s (offset=%s)", page_num + 1, total_pages, offset)
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
                            "total_orders": row.get("report_summary", {}).get("ecommerce", {}).get("total_orders"),
                            "total_spent": row.get("report_summary", {}).get("ecommerce", {}).get("total_spent"),
                            "total_revenue": row.get("report_summary", {}).get("ecommerce", {}).get("total_revenue"),
                            "load_date": date.today().strftime("%Y-%m-%d"),
                        }
                        outfile.write(json.dumps(tdata) + "\n")
                    offset = offset + count
        except Exception as e:
            logging.exception("Error while getting campaign list: %s", e)
            raise


class CampaignReports(MailChimp):
    """Per-campaign report summary (opens/clicks/bounces vs industry stats)."""

    def get_campaign_report(self, campaign_id: str):
        try:
            return self._conn().reports.get_campaign_report(campaign_id)
        except ApiClientError as error:
            logging.error("Campaign report API error (%s): %s", campaign_id, error.text)
            raise

    def fetch_campaign_report(self, campaign_reports_loc: str):
        counter = 1
        for campaign in self._query_results():
            for attempt in range(CAMPAIGN_FETCH_ATTEMPTS):
                try:
                    with open(campaign_reports_loc + f"campaign_reports_{campaign}.json", "w") as outfile:
                        logging.info(
                            "Attempt %s — #%s campaign report: %s",
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
                            "unique_subscriber_clicks": row.get("clicks", {}).get("unique_subscriber_clicks"),
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
                    counter += 1
                except Exception as e:
                    logging.info("Error while getting campaign reports: %s", e)
                else:
                    break
            else:
                logging.info("All attempts exhausted for campaign report %s", campaign)


class ClickReport(MailChimp):
    """Per-URL click details for each campaign (paginated)."""

    def get_click_report(self, campaign_id: str, offset: int = 0, count: int = 1000):
        try:
            return self._conn().reports.get_campaign_click_details(
                campaign_id, count=count, offset=offset
            )
        except ApiClientError as error:
            logging.error("Click report API error (%s): %s", campaign_id, error.text)
            raise

    def fetch_click_report(self, click_report_loc: str):
        counter = 1
        for campaign in self._query_results():
            for attempt in range(CAMPAIGN_FETCH_ATTEMPTS):
                try:
                    offset = 0
                    count = 1000
                    with open(click_report_loc + f"click_report_{campaign}.json", "w") as outfile:
                        response = self.get_click_report(campaign_id=campaign)
                        total_pages = math.ceil(response["total_items"] / count) if response["total_items"] else 0
                        logging.info(
                            "Attempt %s — #%s click report %s (items=%s)",
                            attempt,
                            counter,
                            campaign,
                            response["total_items"],
                        )
                        for _ in range(total_pages):
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
                            offset += count
                    counter += 1
                except Exception as e:
                    logging.info("Error while getting click reports: %s", e)
                else:
                    break
            else:
                logging.info("All attempts exhausted for click report %s", campaign)


class Unsubscribes(MailChimp):
    """Per-campaign unsubscribe list with CRM merge fields."""

    def get_unsubscribes(self, campaign_id: str, offset: int = 0, count: int = 1000):
        try:
            return self._conn().reports.get_unsubscribed_list_for_campaign(
                campaign_id, count=count, offset=offset
            )
        except ApiClientError as error:
            logging.error("Unsubscribes API error (%s): %s", campaign_id, error.text)
            raise

    def fetch_unsubscribes(self, unsubscribes_loc: str):
        counter = 1
        for campaign in self._query_results():
            for attempt in range(CAMPAIGN_FETCH_ATTEMPTS):
                try:
                    offset = 0
                    count = 1000
                    with open(unsubscribes_loc + f"unsubscribes_{campaign}.json", "w") as outfile:
                        response = self.get_unsubscribes(campaign_id=campaign)
                        total_pages = math.ceil(response["total_items"] / count) if response["total_items"] else 0
                        logging.info(
                            "Attempt %s — #%s unsubscribes %s (items=%s)",
                            attempt,
                            counter,
                            campaign,
                            response["total_items"],
                        )
                        for _ in range(total_pages):
                            response = self.get_unsubscribes(
                                campaign_id=campaign, offset=offset, count=count
                            )
                            for row in response["unsubscribes"]:
                                merge = row.get("merge_fields", {}) or {}
                                tdata = {
                                    "email_id": row.get("email_id"),
                                    "email_address": row.get("email_address"),
                                    "first_name": merge.get("FNAME"),
                                    "last_name": merge.get("LNAME"),
                                    "address": str(merge.get("ADDRESS")),
                                    "account_number": merge.get("ACCOUNT_NO"),
                                    "industry_segment": merge.get("SEGMENT"),
                                    "company_name": merge.get("COMPANY"),
                                    "account_manager": merge.get("AM"),
                                    "postcode": merge.get("POSTCODE"),
                                    "asset_number": merge.get("ASSET_NO"),
                                    "registration_id": merge.get("REG_ID"),
                                    "vip": row.get("vip"),
                                    "unsubscribe_ts": row.get("timestamp"),
                                    "reason": row.get("reason"),
                                    "campaign_id": row.get("campaign_id"),
                                    "list_id": row.get("list_id"),
                                    "list_is_active": row.get("list_is_active"),
                                    "load_date": date.today().strftime("%Y-%m-%d"),
                                }
                                outfile.write(json.dumps(tdata) + "\n")
                            offset += count
                    counter += 1
                except Exception as e:
                    logging.info("Error while getting unsubscribe list: %s", e)
                else:
                    break
            else:
                logging.info("All attempts exhausted for unsubscribes %s", campaign)


class EmailActivity(MailChimp):
    """Per-recipient activity array for each campaign (opens/clicks events)."""

    def get_email_activity(self, campaign_id: str, offset: int = 0, count: int = 1000):
        try:
            return self._conn().reports.get_email_activity_for_campaign(
                campaign_id, count=count, offset=offset
            )
        except ApiClientError as error:
            logging.error("Email activity API error (%s): %s", campaign_id, error.text)
            raise

    def fetch_email_activity(self, email_activity_loc: str):
        counter = 1
        for campaign in self._query_results():
            for attempt in range(CAMPAIGN_FETCH_ATTEMPTS):
                try:
                    offset = 0
                    count = 1000
                    with open(email_activity_loc + f"email_activity_{campaign}.json", "w") as outfile:
                        response = self.get_email_activity(campaign_id=campaign)
                        total_pages = math.ceil(response["total_items"] / count) if response["total_items"] else 0
                        logging.info(
                            "Attempt %s — #%s email activity %s (items=%s)",
                            attempt,
                            counter,
                            campaign,
                            response["total_items"],
                        )
                        for _ in range(total_pages):
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
                            offset += count
                    counter += 1
                except Exception as e:
                    logging.info("Error while getting email activity: %s", e)
                else:
                    break
            else:
                logging.info("All attempts exhausted for email activity %s", campaign)


class Recipients(MailChimp):
    """Per-campaign sent-to list with open counts and CRM merge fields."""

    def get_recipients(self, campaign_id: str, offset: int = 0, count: int = 1000):
        try:
            return self._conn().reports.get_campaign_recipients(
                campaign_id, count=count, offset=offset
            )
        except ApiClientError as error:
            logging.error("Recipients API error (%s): %s", campaign_id, error.text)
            raise

    def fetch_recipients(self, recipients_loc: str):
        counter = 1
        for campaign in self._query_results():
            for attempt in range(CAMPAIGN_FETCH_ATTEMPTS):
                try:
                    offset = 0
                    count = 1000
                    with open(recipients_loc + f"recipients_{campaign}.json", "w") as outfile:
                        response = self.get_recipients(campaign_id=campaign)
                        total_pages = math.ceil(response["total_items"] / count) if response["total_items"] else 0
                        logging.info(
                            "Attempt %s — #%s recipients %s (items=%s)",
                            attempt,
                            counter,
                            campaign,
                            response["total_items"],
                        )
                        for _ in range(total_pages):
                            response = self.get_recipients(
                                campaign_id=campaign, offset=offset, count=count
                            )
                            for row in response["sent_to"]:
                                merge = row.get("merge_fields", {}) or {}
                                tdata = {
                                    "email_id": row.get("email_id"),
                                    "email_address": row.get("email_address"),
                                    "first_name": merge.get("FNAME"),
                                    "last_name": merge.get("LNAME"),
                                    "address": str(merge.get("ADDRESS")),
                                    "account_number": merge.get("ACCOUNT_NO"),
                                    "industry_segment": merge.get("SEGMENT"),
                                    "company_name": merge.get("COMPANY"),
                                    "account_manager": merge.get("AM"),
                                    "postcode": merge.get("POSTCODE"),
                                    "asset_number": merge.get("ASSET_NO"),
                                    "registration_id": merge.get("REG_ID"),
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
                            offset += count
                    counter += 1
                except Exception as e:
                    logging.info("Error while getting recipients list: %s", e)
                else:
                    break
            else:
                logging.info("All attempts exhausted for recipients %s", campaign)
