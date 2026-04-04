"""
Target Mushroom Supplement Scraper — via Redsky API

Target's public Redsky API returns JSON product data with no auth required.
Queries multiple mushroom-related search terms, deduplicates by TCIN,
and saves to target_mushrooms.csv.

Run:
    python3 scrape_target.py
"""

import csv
import json
import re
import time
import urllib.request
import urllib.parse
from pathlib import Path

OUTPUT_CSV = "target_mushrooms.csv"

FIELDS = [
    "tcin", "brand", "productName", "price", "regPrice",
    "rating", "reviewCount", "boughtPastMonth", "formFactor", "category", "url", "searchQuery",
]

SEARCH_QUERIES = [
    "mushroom supplement",
    "lion's mane supplement",
    "reishi mushroom",
    "chaga mushroom",
    "cordyceps supplement",
    "turkey tail mushroom",
    "mushroom coffee",
    "mushroom gummies",
    "functional mushroom",
    "mushroom complex",
    "mushroom powder supplement",
]

API_KEY = "9f36aeafbe60771e321a7cc95a78140772ab3e96"
BASE_URL = "https://redsky.target.com/redsky_aggregations/v1/web/plp_search_v2"
PAGE_SIZE = 24
MAX_PAGES = 10
STORE_ID = "1392"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
    "Accept-Language": "en-US,en;q=0.9",
}

FORM_FACTOR_RULES = [
    ("Coffee",   [r"coffee", r"espresso", r"\bbrew\b"]),
    ("Gummy",    [r"gumm(?:y|ies)", r"chewable"]),
    ("Liquid",   [r"tincture", r"liquid", r"drops?", r"elixir"]),
    ("Powder",   [r"powder", r"blend", r"mix"]),
    ("Capsule",  [r"capsule", r"softgel", r"vegcap", r"tablet", r"\bct\b"]),
    ("Tea",      [r"\btea\b", r"matcha", r"\bchai\b"]),
    ("Chocolate",[r"chocolat", r"cocoa", r"cacao"]),
    ("Drink",    [r"\bdrink", r"beverage", r"\bshot\b"]),
    ("Bar",      [r"\bbar\b"]),
    ("Spray",    [r"spray"]),
]


def infer_form_factor(text):
    t = (text or "").lower()
    for label, patterns in FORM_FACTOR_RULES:
        if any(re.search(p, t) for p in patterns):
            return label
    return "Other"


def fetch_page(query, offset):
    """Fetch one page of Target search results."""
    params = {
        "key": API_KEY,
        "channel": "WEB",
        "count": PAGE_SIZE,
        "keyword": query,
        "offset": offset,
        "page": f"/s/{query}",
        "pricing_store_id": STORE_ID,
        "visitor_id": "mushroom_market_analysis",
    }
    url = f"{BASE_URL}?{urllib.parse.urlencode(params)}"

    req = urllib.request.Request(url, headers=HEADERS)
    with urllib.request.urlopen(req, timeout=20) as resp:
        return json.loads(resp.read())


def parse_products(data, query):
    """Parse products from API response."""
    search = data.get("data", {}).get("search", {})
    raw_products = search.get("products", [])

    results = []
    for p in raw_products:
        item = p.get("item", {})
        desc = item.get("product_description", {})
        price_data = p.get("price", {})
        rr = p.get("ratings_and_reviews", {}).get("statistics", {}).get("rating", {})
        brand_data = item.get("primary_brand", {})
        category = p.get("category", {})

        tcin = p.get("tcin") or p.get("original_tcin")
        title = desc.get("title", "")

        if not title:
            continue

        # Extract "bought past month" from desirability_cues
        bought = ""
        for cue in p.get("desirability_cues", []):
            if cue.get("code") == "social_proofing":
                bought = cue.get("display", "")
                break

        results.append({
            "tcin": tcin,
            "brand": brand_data.get("name", ""),
            "productName": title,
            "price": price_data.get("current_retail"),
            "regPrice": price_data.get("reg_retail"),
            "rating": rr.get("average"),
            "reviewCount": rr.get("count"),
            "boughtPastMonth": bought,
            "formFactor": infer_form_factor(title),
            "category": category.get("name", ""),
            "url": f"https://www.target.com/p/-/A-{tcin}" if tcin else "",
            "searchQuery": query,
        })

    return results


def save_csv(rows):
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows(rows)
    print(f"\n✅ Saved {len(rows):,} products → {OUTPUT_CSV}")


def main():
    # Load existing for resume
    existing_tcins = set()
    all_rows = []
    if Path(OUTPUT_CSV).exists():
        with open(OUTPUT_CSV, newline="", encoding="utf-8") as f:
            for r in csv.DictReader(f):
                existing_tcins.add(r["tcin"])
                all_rows.append(r)
        print(f"Resuming: {len(existing_tcins)} existing products")

    seen_tcins = set(existing_tcins)

    for qi, query in enumerate(SEARCH_QUERIES):
        print(f"\n[{qi+1}/{len(SEARCH_QUERIES)}] '{query}'", end="", flush=True)

        offset = 0
        query_new = 0

        for page in range(MAX_PAGES):
            try:
                data = fetch_page(query, offset)
            except Exception as e:
                print(f" ERROR: {e}")
                break

            products = parse_products(data, query)
            if not products:
                break

            for p in products:
                key = p.get("tcin") or p.get("productName")
                if key and key not in seen_tcins:
                    seen_tcins.add(key)
                    all_rows.append(p)
                    query_new += 1

            print(f".", end="", flush=True)
            offset += PAGE_SIZE
            time.sleep(0.5)

        print(f" +{query_new} new ({len(all_rows)} total)")

    save_csv(all_rows)
    total_brands = len(set(r["brand"] for r in all_rows if r.get("brand")))
    print(f"Total: {len(all_rows)} products from {total_brands} brands")


if __name__ == "__main__":
    main()
