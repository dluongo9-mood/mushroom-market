"""
Amazon Brand Scraper — HTTP-based (no Playwright needed)

Fetches /dp/{ASIN} pages via urllib and extracts brand from #bylineInfo.
Slower but more reliable than Playwright fetch() approach.

Run:
    python3 scrape_brands_http.py
"""

import csv
import json
import re
import time
import urllib.request
from pathlib import Path

DETAILS_CSV = "amazon_details.csv"
TARGET_ASINS = "remaining_brand_asins.json"  # optional: only scrape these
PLP_CSV = "mushroom_skus_plp.csv"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "identity",
}


def extract_brand(html):
    """Extract brand from Amazon product page HTML."""
    m = re.search(r'id="bylineInfo"[^>]*>(.*?)</a>', html, re.DOTALL)
    if m:
        text = re.sub(r'<[^>]+>', '', m.group(1)).strip()
        text = re.sub(r'Visit the|Store|Brand:\s*|by\s+', '', text, flags=re.I).strip()
        text = re.sub(r'\s+', ' ', text).strip()
        return text if text else None
    # Fallback: look for "Brand" in product details table
    m = re.search(r'Brand</(?:th|td)>\s*<td[^>]*>\s*<span[^>]*>(.*?)</span>', html, re.DOTALL | re.I)
    if m:
        return m.group(1).strip() or None
    return None


def extract_date(html):
    """Extract Date First Available from product page HTML."""
    m = re.search(r'Date First Available\s*[:\s]*</(?:th|td|span)>\s*<(?:td|span)[^>]*>\s*([\w\s,]+)', html, re.I)
    if m:
        return m.group(1).strip()
    m = re.search(r'Date First Available\s*[:\u200F\s]*\s*([\w]+ \d+, \d{4})', html, re.I)
    if m:
        return m.group(1).strip()
    return None


def load_details():
    """Load existing details CSV into dict."""
    details = {}
    if Path(DETAILS_CSV).exists():
        with open(DETAILS_CSV, newline="", encoding="utf-8") as f:
            for r in csv.DictReader(f):
                details[r["asin"]] = {
                    "brand": r.get("brand") or "",
                    "dateFirstAvailable": r.get("dateFirstAvailable") or "",
                    "parentASIN": r.get("parentASIN") or "",
                }
    return details


def save_details(details):
    """Save details dict to CSV."""
    with open(DETAILS_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["asin", "brand", "dateFirstAvailable", "parentASIN"])
        writer.writeheader()
        for asin in sorted(details.keys()):
            d = details[asin]
            writer.writerow({
                "asin": asin,
                "brand": d.get("brand") or "",
                "dateFirstAvailable": d.get("dateFirstAvailable") or "",
                "parentASIN": d.get("parentASIN") or "",
            })


def fetch_product(asin):
    """Fetch a product page and extract brand + date."""
    url = f"https://www.amazon.com/dp/{asin}"
    req = urllib.request.Request(url, headers=HEADERS)
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            html = resp.read().decode("utf-8", errors="ignore")
        if len(html) < 5000 or "captcha" in html.lower():
            return None, None, None
        brand = extract_brand(html)
        date = extract_date(html)
        # Parent ASIN
        pm = re.search(r'parentASIN["\s:=]+["\']?([A-Z0-9]{10})', html, re.I)
        parent = pm.group(1) if pm else None
        return brand, date, parent
    except Exception:
        return None, None, None


def main():
    details = load_details()

    # Determine which ASINs need brand data
    if Path(TARGET_ASINS).exists():
        with open(TARGET_ASINS) as f:
            target_asins = json.load(f)
        remaining = [a for a in target_asins if not (details.get(a, {}).get("brand"))]
    else:
        all_asins = []
        with open(PLP_CSV, newline="", encoding="utf-8") as f:
            all_asins = [r["asin"] for r in csv.DictReader(f) if r.get("asin")]
        remaining = [a for a in all_asins if not (details.get(a, {}).get("brand"))]

    print(f"Total in details: {len(details)}")
    print(f"Need brand: {len(remaining)}")

    if not remaining:
        print("All done!")
        return

    brands_found = 0
    dates_found = 0
    captcha_streak = 0

    for i, asin in enumerate(remaining):
        brand, date, parent = fetch_product(asin)

        if brand:
            details.setdefault(asin, {})
            details[asin]["brand"] = brand
            if date:
                details[asin]["dateFirstAvailable"] = date
            if parent:
                details[asin]["parentASIN"] = parent
            brands_found += 1
            captcha_streak = 0
        elif date:
            details.setdefault(asin, {})
            details[asin]["dateFirstAvailable"] = date
            if parent:
                details[asin]["parentASIN"] = parent
            dates_found += 1
            captcha_streak = 0
        else:
            captcha_streak += 1

        if (i + 1) % 10 == 0:
            print(f"  {i+1}/{len(remaining)}  |  +{brands_found} brands  |  captcha streak: {captcha_streak}")
            save_details(details)

        # Adaptive delay: slow down if getting blocked
        if captcha_streak > 5:
            time.sleep(5)
        elif captcha_streak > 2:
            time.sleep(3)
        else:
            time.sleep(1.5)

        # Give up if blocked too many times in a row
        if captcha_streak >= 20:
            print(f"  Stopped: {captcha_streak} consecutive blocks. Saving progress.")
            break

    save_details(details)
    total_brands = sum(1 for d in details.values() if d.get("brand"))
    print(f"\nDone! {brands_found} new brands found this run")
    print(f"Total brands in details: {total_brands} / {len(details)}")
    print(f"Saved → {DETAILS_CSV}")


if __name__ == "__main__":
    main()
