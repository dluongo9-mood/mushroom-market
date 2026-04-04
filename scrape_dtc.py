"""
DTC Mushroom Brand Scraper — Shopify /products.json API

Shopify stores expose a public /products.json endpoint (no auth needed).
This script queries 20+ known functional mushroom brand stores, filters
to mushroom-relevant products, and saves to dtc_mushrooms.csv.

Run:
    python3 scrape_dtc.py
"""

import csv
import json
import re
import time
import urllib.request
import urllib.parse
from pathlib import Path

OUTPUT_CSV = "dtc_mushrooms.csv"

FIELDS = [
    "productId", "brand", "shopDomain", "productName", "productType",
    "price", "compareAtPrice", "formFactor", "mushroomTypes",
    "variantCount", "tags", "url",
]

# Known Shopify mushroom brand stores
SHOPS = [
    ("Four Sigmatic",       "us.foursigmatic.com"),
    ("Host Defense",        "hostdefense.com"),
    ("Real Mushrooms",      "www.realmushrooms.com"),
    ("Mushroom Revival",    "mushroomrevival.com"),
    ("Rainbo",              "rainbo.ca"),
    ("DIRTEA",              "dirteaworld.com"),
    ("MUDWTR",              "mudwtr.com"),
    ("Ryze",                "ryzesuperfoods.com"),
    ("Everyday Dose",       "everydaydose.com"),
    ("Alice Mushrooms",     "alicemushrooms.com"),
    ("Life Cykel",          "lifecykel.com"),
    ("Birch Boys",          "www.birchboys.com"),
    ("Apothekary",          "apothekary.co"),
    ("Clevr Blends",        "www.clevrblends.com"),
    ("Laird Superfood",     "www.lairdsuperfood.com"),
    ("Moon Juice",          "moonjuice.com"),
    ("Fungi Ally",          "fungially.com"),
    ("Wellbeing Nutrition", "www.wellbeingnutrition.com"),
    ("Sun Potion",          "www.sunpotion.com"),
]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
}

# Mushroom keywords for product filtering
MUSHROOM_KEYWORDS = [
    "mushroom", "fungi", "lion.?s mane", "reishi", "chaga", "cordyceps",
    "turkey tail", "maitake", "shiitake", "tremella", "agaricus",
    "oyster mushroom", "king trumpet", "ahcc", "adaptogen",
    "mycelium", "beta.glucan", "hericium", "ganoderma",
]
MUSHROOM_RE = re.compile("|".join(MUSHROOM_KEYWORDS), re.I)

MUSHROOM_TYPES = [
    ("Lion's Mane",  [r"lion.?s?\s*mane", r"hericium"]),
    ("Reishi",       [r"reishi", r"ganoderma", r"lingzhi"]),
    ("Chaga",        [r"chaga", r"inonotus"]),
    ("Cordyceps",    [r"cordyceps"]),
    ("Turkey Tail",  [r"turkey\s*tail", r"trametes", r"coriolus"]),
    ("Maitake",      [r"maitake", r"grifola"]),
    ("Shiitake",     [r"shiitake"]),
    ("Tremella",     [r"tremella"]),
    ("Oyster",       [r"oyster\s*mushroom", r"pleurotus"]),
    ("AHCC",         [r"ahcc"]),
]

FORM_FACTOR_RULES = [
    ("Coffee",   [r"coffee", r"latte", r"brew", r"espresso"]),
    ("Gummy",    [r"gumm", r"chewable"]),
    ("Liquid",   [r"tincture", r"liquid", r"drops?", r"elixir", r"extract\s+drop"]),
    ("Powder",   [r"powder", r"blend", r"mix", r"matcha", r"chai", r"cacao"]),
    ("Capsule",  [r"capsule", r"cap\b", r"softgel", r"vegcap", r"tablet"]),
    ("Tea",      [r"\btea\b", r"tea bag"]),
    ("Spray",    [r"spray"]),
    ("Whole",    [r"dried", r"whole", r"fresh", r"grow kit", r"grain"]),
]


def is_mushroom_product(text):
    return bool(MUSHROOM_RE.search(text or ""))


def extract_mushroom_types(text):
    found = []
    t = (text or "").lower()
    for name, patterns in MUSHROOM_TYPES:
        if any(re.search(p, t) for p in patterns):
            found.append(name)
    if len(found) > 3:
        return "Blend"
    return ", ".join(found) if found else "Other"


def infer_form_factor(text):
    t = (text or "").lower()
    for label, patterns in FORM_FACTOR_RULES:
        if any(re.search(p, t) for p in patterns):
            return label
    return "Other"


def fetch_all_products(domain):
    """Fetch all products from a Shopify store via /products.json pagination."""
    products = []
    page = 1
    while True:
        url = f"https://{domain}/products.json?limit=250&page={page}"
        try:
            req = urllib.request.Request(url, headers=HEADERS)
            with urllib.request.urlopen(req, timeout=15) as resp:
                data = json.loads(resp.read())
        except Exception as e:
            print(f"    Error page {page}: {e}")
            break

        batch = data.get("products", [])
        if not batch:
            break
        products.extend(batch)
        if len(batch) < 250:
            break  # last page
        page += 1
        time.sleep(0.5)

    return products


def parse_product(p, brand, domain):
    """Convert a Shopify product dict to our flat row format."""
    title = p.get("title", "")
    body = re.sub(r"<[^>]+>", " ", p.get("body_html", "") or "")
    full_text = f"{title} {p.get('product_type', '')} {' '.join(p.get('tags', []))} {body}"

    if not is_mushroom_product(full_text):
        return None

    variants = p.get("variants", [])
    prices = [float(v["price"]) for v in variants if v.get("price")]
    compare_prices = [float(v["compare_at_price"]) for v in variants if v.get("compare_at_price")]

    price = min(prices) if prices else None
    compare_at = min(compare_prices) if compare_prices else None

    handle = p.get("handle", "")
    url = f"https://{domain}/products/{handle}"
    tags = ", ".join(p.get("tags", []))[:200]

    return {
        "productId":      f"{domain}_{p.get('id', '')}",
        "brand":          brand,
        "shopDomain":     domain,
        "productName":    title,
        "productType":    p.get("product_type", ""),
        "price":          price,
        "compareAtPrice": compare_at,
        "formFactor":     infer_form_factor(title),
        "mushroomTypes":  extract_mushroom_types(full_text),
        "variantCount":   len(variants),
        "tags":           tags,
        "url":            url,
    }


def save_csv(rows):
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows(rows)
    print(f"\n✅ Saved {len(rows):,} products → {OUTPUT_CSV}")


def main():
    # Load existing to support resume
    existing_ids = set()
    all_rows = []
    if Path(OUTPUT_CSV).exists():
        with open(OUTPUT_CSV, newline="", encoding="utf-8") as f:
            for r in csv.DictReader(f):
                existing_ids.add(r["productId"])
                all_rows.append(r)
        print(f"Resuming: {len(existing_ids)} existing products")

    for shop_idx, (brand, domain) in enumerate(SHOPS):
        # Check if already done
        domain_key = domain
        already = sum(1 for r in all_rows if r.get("shopDomain") == domain_key)
        if already > 0:
            print(f"[{shop_idx+1}/{len(SHOPS)}] {brand}: skipping ({already} already saved)")
            continue

        print(f"[{shop_idx+1}/{len(SHOPS)}] {brand} ({domain})…", end=" ", flush=True)
        products = fetch_all_products(domain)

        mushroom_rows = []
        for p in products:
            row = parse_product(p, brand, domain)
            if row and row["productId"] not in existing_ids:
                mushroom_rows.append(row)
                existing_ids.add(row["productId"])

        print(f"{len(products)} total → {len(mushroom_rows)} mushroom products")
        all_rows.extend(mushroom_rows)
        time.sleep(1)

    save_csv(all_rows)
    total_brands = len(set(r["brand"] for r in all_rows))
    print(f"Total: {len(all_rows)} mushroom products from {total_brands} DTC brands")


if __name__ == "__main__":
    main()
