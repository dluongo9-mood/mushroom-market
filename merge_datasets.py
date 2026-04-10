"""
Merge Amazon PLP CSV + iHerb CSV → combined_mushrooms.csv

Normalizes both datasets to a common schema:
  source, id, brand, productName, formFactor, size/count,
  price, listPrice, discount, rating, reviewCount, soldPastMonth,
  badge, isOnSale, image, url, searchQuery

Run:
    python merge_datasets.py
"""

import csv
import re

AMAZON_CSV  = "mushroom_skus_plp.csv"
IHERB_CSV   = "iherb_mushrooms.csv"
OUTPUT_CSV  = "combined_mushrooms.csv"

# ── Shared output schema ───────────────────────────────────────────────────────
FIELDS = [
    "source",
    "id",           # asin  (Amazon) | partNumber  (iHerb)
    "brand",
    "productName",
    "formFactor",
    "size",         # count (Amazon) | size        (iHerb)
    "price",
    "listPrice",
    "discount",     # savings_pct (Amazon) | discount % (iHerb)
    "rating",
    "reviewCount",
    "soldPastMonth",   # boughtPastMonth (Amazon) | soldPastMonth (iHerb)
    "badge",           # isBestSeller/isAmazonChoice → badge (Amazon)
    "isOnSale",
    "image",
    "url",
    "searchQuery",
]


def parse_float(v):
    try:
        return float(v) if v not in (None, "", "None") else None
    except (ValueError, TypeError):
        return None


def parse_int(v):
    try:
        s = str(v).replace(",", "").strip()
        return int(float(s)) if s not in ("", "None") else None
    except (ValueError, TypeError):
        return None


def yn(v):
    """Convert True/False/1/0/'True'/'False' → 1 or 0."""
    if isinstance(v, bool):
        return int(v)
    if isinstance(v, (int, float)):
        return int(bool(v))
    return 1 if str(v).strip().lower() in ("true", "1", "yes") else 0


# ── Amazon row → common schema ─────────────────────────────────────────────────
def normalize_amazon(row: dict) -> dict:
    # Build a badge string from boolean flags
    badges = []
    if yn(row.get("isBestSeller")):
        badges.append("Best Seller")
    if yn(row.get("isAmazonChoice")):
        badges.append("Amazon's Choice")
    if yn(row.get("isSponsored")):
        badges.append("Sponsored")
    badge = ", ".join(badges) or None

    # brand is not a separate Amazon PLP field — attempt to pull from title
    title = row.get("title", "") or ""
    brand = row.get("brand") or None   # column may exist but often empty

    return {
        "source":       "amazon",
        "id":           row.get("asin"),
        "brand":        brand,
        "productName":  title,
        "formFactor":   row.get("formFactor"),
        "size":         row.get("count"),
        "price":        parse_float(row.get("price")),
        "listPrice":    parse_float(row.get("listPrice")),
        "discount":     parse_float(row.get("savings_pct")),
        "rating":       parse_float(row.get("rating")),
        "reviewCount":  parse_int(row.get("reviewCount")),
        "soldPastMonth":parse_int(row.get("boughtPastMonth")),
        "badge":        badge,
        "isOnSale":     yn(row.get("coupon") or row.get("savings_pct")),
        "image":        row.get("image"),
        "url":          row.get("url"),
        "searchQuery":  row.get("searchQuery"),
    }


# ── iHerb row → common schema ──────────────────────────────────────────────────
def normalize_iherb(row: dict) -> dict:
    return {
        "source":       "iherb",
        "id":           row.get("partNumber"),
        "brand":        row.get("brand"),
        "productName":  row.get("productName") or row.get("fullTitle"),
        "formFactor":   row.get("formFactor"),
        "size":         row.get("size"),
        "price":        parse_float(row.get("price")),
        "listPrice":    parse_float(row.get("listPrice")),
        "discount":     parse_float(row.get("discount")),
        "rating":       parse_float(row.get("rating")),
        "reviewCount":  parse_int(row.get("reviewCount")),
        "soldPastMonth":parse_int(row.get("soldPastMonth")),
        "badge":        row.get("badge"),
        "isOnSale":     yn(row.get("isOnSale")),
        "image":        row.get("image"),
        "url":          row.get("url"),
        "searchQuery":  row.get("searchQuery"),
    }


# ── Main ───────────────────────────────────────────────────────────────────────

def read_csv(path: str) -> list[dict]:
    try:
        with open(path, newline="", encoding="utf-8") as f:
            return list(csv.DictReader(f))
    except FileNotFoundError:
        print(f"  ⚠ File not found: {path} — skipping.")
        return []


def main():
    print("Reading Amazon CSV…")
    amazon_rows = read_csv(AMAZON_CSV)
    print(f"  {len(amazon_rows)} rows")

    print("Reading iHerb CSV…")
    iherb_rows = read_csv(IHERB_CSV)
    print(f"  {len(iherb_rows)} rows")

    combined = []

    for row in amazon_rows:
        combined.append(normalize_amazon(row))

    for row in iherb_rows:
        combined.append(normalize_iherb(row))

    print(f"\nTotal combined rows: {len(combined)}")

    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(combined)

    print(f"✅ Saved → {OUTPUT_CSV}")

    # Quick stats
    amazon_count = sum(1 for r in combined if r["source"] == "amazon")
    iherb_count  = sum(1 for r in combined if r["source"] == "iherb")
    print(f"\nBreakdown:")
    print(f"  Amazon : {amazon_count:,}")
    print(f"  iHerb  : {iherb_count:,}")
    print(f"  Total  : {len(combined):,}")


if __name__ == "__main__":
    main()
