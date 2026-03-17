"""
14 Plane Street – VRM kWh Scraper
Downloads today's 15-min kWh XLSX from Victron VRM.
Credentials are read from environment variables (for GitHub Actions secrets).
Usage:
    VRM_EMAIL=... VRM_PASSWORD=... python scraper.py
"""
import os
import re
import sys
import glob
from datetime import datetime
from playwright.sync_api import sync_playwright

# ── Config ──────────────────────────────────────────────
VRM_EMAIL = os.environ.get("VRM_EMAIL", "")
VRM_PASSWORD = os.environ.get("VRM_PASSWORD", "")
INSTALL_NAME = "14 Plane street, J-Bay"
DOWNLOAD_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "downloads")

if not VRM_EMAIL or not VRM_PASSWORD:
    print("ERROR: Set VRM_EMAIL and VRM_PASSWORD environment variables.")
    sys.exit(1)

os.makedirs(DOWNLOAD_DIR, exist_ok=True)


def run():
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()
        page.set_default_timeout(60000)

        # ── Login ───────────────────────────────────
        print("Logging in to VRM...")
        page.goto("https://vrm.victronenergy.com/login")
        page.get_by_role("textbox", name="Email address").fill(VRM_EMAIL)
        page.get_by_test_id("vrm-button").click()
        page.get_by_role("textbox", name="Password").fill(VRM_PASSWORD)
        page.get_by_test_id("vrm-button").click()
        page.wait_for_load_state("networkidle")

        # ── Dismiss any modal/popup that might appear ─
        try:
            page.wait_for_selector(".modal-close, [aria-label='Close']", timeout=5000)
            page.locator(".modal-close, [aria-label='Close']").first.click()
        except Exception:
            pass

        # ── Navigate to installation ────────────────
        print(f"Searching for '{INSTALL_NAME}'...")
        page.get_by_role("button", name="Search installations Ctrl K").click()
        page.get_by_role("textbox", name="Search").fill("Plane")
        page.get_by_label("Suggestions").get_by_role("paragraph").filter(
            has_text=re.compile(r"^14 Plane street, J-Bay$")
        ).click()
        page.wait_for_load_state("networkidle")

        # ── Go to Advanced tab ──────────────────────
        print("Opening Advanced view...")
        page.get_by_role("link", name="Advanced").click()
        page.wait_for_load_state("networkidle")

        # ── Set date range to 'Today' ───────────────
        page.locator("#vrm-advanced-controls").get_by_role("textbox").click()
        page.get_by_text("Today").click()
        page.wait_for_timeout(2000)

        # ── Open download menu & download kWh XLSX ──
        print("Downloading kWh XLSX...")
        page.locator("vrm-header a").nth(2).click()
        page.wait_for_timeout(1000)

        with page.expect_download(timeout=30000) as download_info:
            page.get_by_text("Download kWh .xlsx").nth(1).click()
        download = download_info.value

        # Save with standardised name
        today_str = datetime.now().strftime("%Y%m%d")
        filename = f"14PlanestreetJBay_kwh_{today_str}.xlsx"
        dest = os.path.join(DOWNLOAD_DIR, filename)
        download.save_as(dest)
        print(f"Saved: {dest}")

        context.close()
        browser.close()
    return dest


if __name__ == "__main__":
    path = run()
    print(f"\nDone. File: {path}")
