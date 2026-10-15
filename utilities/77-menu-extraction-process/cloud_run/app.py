"""Menu page renderer. Cloud Run, one Chromium per request.

Composer must not launch a browser. This service does. Caller sends an
ID token (audience = service URL). Response is the rendered DOM, not a
parsed menu.
"""

import os

from flask import Flask, jsonify, request
from playwright.sync_api import sync_playwright
from playwright_stealth import Stealth

app = Flask(__name__)


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"}), 200


@app.route("/fetch", methods=["POST"])
def fetch():
    body = request.get_json(force=True, silent=True) or {}
    url = body.get("url")
    if not url or not isinstance(url, str):
        return jsonify({"error": "url is required"}), 400
    if not url.startswith(("http://", "https://")):
        return jsonify({"error": "url must be http(s)"}), 400

    with Stealth().use_sync(sync_playwright()) as playwright:
        browser = playwright.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage"],
        )
        context = browser.new_context(viewport={"width": 1366, "height": 768})
        page = context.new_page()
        page.goto(url, wait_until="domcontentloaded", timeout=20000)
        page.wait_for_timeout(3000)
        payload = {
            "html": page.content(),
            "final_url": page.url,
            "title": page.title(),
        }
        browser.close()
    return jsonify(payload)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "8080")))
