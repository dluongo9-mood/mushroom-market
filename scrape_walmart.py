"""
Walmart Mushroom Supplement Scraper — Playwright-based
Extracts product data from window.__NEXT_DATA__ JSON embedded in each page.
Falls back to CSS selector extraction if JSON approach fails.
Deduplicates by itemId, saves walmart_mushrooms.csv.

Install deps (once):
    pip3 install playwright
    playwright install chromium

Run:
    python3 scrape_walmart.py
"""

import asyncio
import csv
import json
import os
import re

from playwright.async_api import async_playwright, TimeoutError as PWTimeout

# ── Search queries ─────────────────────────────────────────────────────────────

QUERIES = [
    {"url": "https://www.walmart.com/search?q=mushroom+supplement",    "query": "mushroom supplement"},
    {"url": "https://www.walmart.com/search?q=lion%27s+mane+supplement","query": "lion's mane supplement"},
    {"url": "https://www.walmart.com/search?q=reishi+mushroom",         "query": "reishi mushroom"},
    {"url": "https://www.walmart.com/search?q=chaga+mushroom",          "query": "chaga mushroom"},
    {"url": "https://www.walmart.com/search?q=cordyceps+mushroom",      "query": "cordyceps mushroom"},
    {"url": "https://www.walmart.com/search?q=turkey+tail+mushroom",    "query": "turkey tail mushroom"},
    {"url": "https://www.walmart.com/search?q=mushroom+coffee",         "query": "mushroom coffee"},
    {"url": "https://www.walmart.com/search?q=functional+mushroom",     "query": "functional mushroom"},
]

MAX_PAGES   = 10      # hard cap per query (Walmart shows ~40 products/page)
CONCURRENCY = 2       # parallel browser instances
PAGE_DELAY  = 1.0     # seconds between launching browsers
RESUME_THRESHOLD = 5  # skip a query+page combo if >= this many results already exist

OUTPUT_CSV  = "walmart_mushrooms.csv"
CHECKPOINT_FILE = "walmart_checkpoint.json"

FIELDS = [
    "itemId", "brand", "productName", "price", "wasPrice",
    "rating", "reviewCount", "formFactor", "url", "searchQuery",
]

# ── Form factor inference ──────────────────────────────────────────────────────

def infer_form_factor(text):
    t = (text or "").lower()
    for label, keywords in [
        ("Capsule", ["capsule", "vegcap", "vcap", "softgel", "gelcap", "cap "]),
        ("Tablet",  ["tablet", " tab "]),
        ("Powder",  ["powder", "pwdr"]),
        ("Liquid",  ["liquid", "tincture", "fl oz", " ml ", " oz ", "drop"]),
        ("Gummy",   ["gummy", "gummies"]),
        ("Coffee",  ["coffee", "latte", "brew", "espresso"]),
        ("Tea",     [" tea ", "teabag", "tea bag"]),
        ("Chew",    ["chew", "chewable"]),
        ("Spray",   ["spray"]),
    ]:
        if any(kw in t for kw in keywords):
            return label
    return None


# ── JavaScript extraction — reads window.__NEXT_DATA__ ────────────────────────

EXTRACT_JS = r"""
() => {
  try {
    const nd = window.__NEXT_DATA__;
    const sr = nd.props.pageProps.initialData.searchResult;
    const stacks = sr.itemStacks || [];
    const items = stacks.flatMap(s => s.items || []);
    return items.filter(i => i && i.type !== 'FEATURED_ITEM_AD').map(i => ({
      itemId:      i.itemId      || null,
      name:        i.name        || null,
      brand:       i.brand       || null,
      price:       (i.price && i.price.price     != null) ? i.price.price     : null,
      wasPrice:    (i.price && i.price.wasPrice  != null) ? i.price.wasPrice  : null,
      rating:      (i.rating && i.rating.averageRating  != null) ? i.rating.averageRating  : null,
      reviewCount: (i.rating && i.rating.numberOfRatings != null) ? i.rating.numberOfRatings : null,
      url:         i.canonicalUrl ? 'https://www.walmart.com' + i.canonicalUrl : null,
      image:       (i.imageInfo && i.imageInfo.thumbnailUrl) ? i.imageInfo.thumbnailUrl : null,
      shortDesc:   i.shortDescription || null,
    }));
  } catch(e) { return null; }
}
"""

# ── CSS selector fallback extraction ─────────────────────────────────────────

FALLBACK_JS = r"""
() => {
  try {
    const cards = Array.from(document.querySelectorAll(
      '[data-testid="list-view"], [data-item-id], [data-automation-id="product-tile"]'
    ));
    return cards.map(card => {
      const itemId = card.getAttribute('data-item-id') || null;
      const nameEl = card.querySelector('[data-automation-id="product-title"], .product-title-link, h2, h3');
      const name   = nameEl ? nameEl.textContent.trim() : null;
      const priceEl = card.querySelector('[itemprop="price"], [data-automation-id="product-price"] span');
      const priceText = priceEl ? priceEl.textContent.replace(/[^0-9.]/g, '') : null;
      const price = priceText ? parseFloat(priceText) : null;
      const linkEl = card.querySelector('a[href*="/ip/"]');
      const url = linkEl ? 'https://www.walmart.com' + linkEl.getAttribute('href') : null;
      return { itemId, name, brand: null, price, wasPrice: null, rating: null, reviewCount: null, url, image: null, shortDesc: null };
    }).filter(p => p.name || p.itemId);
  } catch(e) { return []; }
}
"""


# ── Checkpoint helpers ────────────────────────────────────────────────────────

def load_checkpoint() -> dict:
    """Returns {query_key: [list of product dicts]} from checkpoint file."""
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_checkpoint(checkpoint: dict):
    with open(CHECKPOINT_FILE, "w", encoding="utf-8") as f:
        json.dump(checkpoint, f, ensure_ascii=False, indent=2)


# ── Scrape a single page URL (fresh browser each time) ────────────────────────

async def scrape_one_page(pw, url: str, query: str, semaphore: asyncio.Semaphore) -> list[dict]:
    async with semaphore:
        browser = None
        try:
            browser = await pw.chromium.launch(
                headless=True,
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--no-sandbox",
                    "--disable-dev-shm-usage",
                ],
            )
            ctx = await browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                ),
                viewport={"width": 1440, "height": 900},
                locale="en-US",
                extra_http_headers={
                    "Accept-Language": "en-US,en;q=0.9",
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                },
            )
            await ctx.add_init_script(
                "Object.defineProperty(navigator, 'webdriver', { get: () => undefined });"
            )
            page = await ctx.new_page()

            await page.goto(url, wait_until="domcontentloaded", timeout=30_000)

            # ── CAPTCHA / bot-check detection ──────────────────────────────────
            title = (await page.title()).lower()
            if any(kw in title for kw in ("robot", "verify", "captcha", "access denied", "blocked")):
                print(f"    WARNING: Bot/CAPTCHA detected on {url!r}  (title: {title!r}) — skipping")
                return []

            # ── Wait for __NEXT_DATA__ to be populated ─────────────────────────
            # Walmart injects it synchronously, so domcontentloaded is usually enough.
            # We try a lightweight poll just in case hydration is async.
            try:
                await page.wait_for_function(
                    "() => !!(window.__NEXT_DATA__ && window.__NEXT_DATA__.props)",
                    timeout=12_000,
                )
            except PWTimeout:
                # __NEXT_DATA__ never appeared — try waiting for a product card
                try:
                    await page.wait_for_selector(
                        '[data-item-id], [data-automation-id="product-tile"]',
                        timeout=8_000,
                    )
                except PWTimeout:
                    print(f"    WARNING: No products or __NEXT_DATA__ on {url!r}  (title: {(await page.title())!r})")
                    return []

            # ── Primary extraction: window.__NEXT_DATA__ ───────────────────────
            raw = await page.evaluate(EXTRACT_JS)

            if not raw:
                # ── Fallback: try __NEXT_DATA__ via <script> tag text ──────────
                raw = await page.evaluate(r"""
                () => {
                  try {
                    const el = document.getElementById('__NEXT_DATA__');
                    if (!el) return null;
                    const nd = JSON.parse(el.textContent);
                    const sr = nd.props.pageProps.initialData.searchResult;
                    const stacks = sr.itemStacks || [];
                    const items = stacks.flatMap(s => s.items || []);
                    return items.filter(i => i && i.type !== 'FEATURED_ITEM_AD').map(i => ({
                      itemId:      i.itemId      || null,
                      name:        i.name        || null,
                      brand:       i.brand       || null,
                      price:       (i.price && i.price.price     != null) ? i.price.price     : null,
                      wasPrice:    (i.price && i.price.wasPrice  != null) ? i.price.wasPrice  : null,
                      rating:      (i.rating && i.rating.averageRating  != null) ? i.rating.averageRating  : null,
                      reviewCount: (i.rating && i.rating.numberOfRatings != null) ? i.rating.numberOfRatings : null,
                      url:         i.canonicalUrl ? 'https://www.walmart.com' + i.canonicalUrl : null,
                      image:       (i.imageInfo && i.imageInfo.thumbnailUrl) ? i.imageInfo.thumbnailUrl : null,
                      shortDesc:   i.shortDescription || null,
                    }));
                  } catch(e) { return null; }
                }
                """)

            if not raw:
                # ── Final fallback: CSS selector extraction ────────────────────
                print(f"    INFO: __NEXT_DATA__ empty on {url!r}, trying CSS fallback")
                raw = await page.evaluate(FALLBACK_JS)

            if not raw:
                return []

            items = []
            for p in raw:
                if not p.get("itemId") and not p.get("name"):
                    continue
                form_text = (p.get("name") or "") + " " + (p.get("shortDesc") or "")
                items.append({
                    "itemId":      p.get("itemId"),
                    "brand":       p.get("brand"),
                    "productName": p.get("name"),
                    "price":       p.get("price"),
                    "wasPrice":    p.get("wasPrice"),
                    "rating":      p.get("rating"),
                    "reviewCount": p.get("reviewCount"),
                    "formFactor":  infer_form_factor(form_text),
                    "url":         p.get("url"),
                    "searchQuery": query,
                })
            return items

        except PWTimeout as e:
            print(f"    ERROR: Timeout on {url!r}: {e}")
            return []
        except Exception as e:
            print(f"    ERROR: {url!r}: {e}")
            return []
        finally:
            if browser:
                await browser.close()


# ── CSV writer ─────────────────────────────────────────────────────────────────

def save_csv(products: list[dict], path: str):
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(products)
    print(f"\nSaved {len(products):,} products -> {path}")


# ── Main ───────────────────────────────────────────────────────────────────────

async def main():
    seen_ids: set[str] = set()
    all_products: list[dict] = []
    semaphore = asyncio.Semaphore(CONCURRENCY)

    # ── Resume: load checkpoint ────────────────────────────────────────────────
    checkpoint = load_checkpoint()
    for q_key, saved_products in checkpoint.items():
        for p in saved_products:
            dedup_key = str(p.get("itemId") or p.get("url") or "")
            if dedup_key and dedup_key not in seen_ids:
                seen_ids.add(dedup_key)
                all_products.append(p)
    if all_products:
        print(f"Resumed {len(all_products):,} products from checkpoint.")

    async with async_playwright() as pw:
        for cat in QUERIES:
            base_url  = cat["url"]
            query     = cat["query"]
            q_key     = query  # used as checkpoint key

            print(f"\n{'='*60}")
            print(f"  Query : {query}")
            print(f"  URL   : {base_url}")
            print(f"{'='*60}")

            # ── Resume check: skip this query if enough results already saved ──
            existing_for_query = checkpoint.get(q_key, [])
            if len(existing_for_query) >= RESUME_THRESHOLD:
                print(f"  Skipping (already have {len(existing_for_query)} results in checkpoint).")
                continue

            # Build page URLs: page 1 has no suffix, pages 2-N append &page=N
            page_urls = [base_url] + [
                f"{base_url}&page={pg}" for pg in range(2, MAX_PAGES + 1)
            ]

            # Scrape all pages in parallel (capped by semaphore)
            tasks = [
                scrape_one_page(pw, url, query, semaphore)
                for url in page_urls
            ]

            query_products: list[dict] = []
            for i, coro in enumerate(asyncio.as_completed(tasks), 1):
                items = await coro
                query_products.extend(items)
                print(f"  Completed {i}/{len(tasks)} pages  (+{len(items)})  running for query: {len(query_products)}")
                await asyncio.sleep(PAGE_DELAY)

            # ── Deduplicate and accumulate ──────────────────────────────────────
            added = 0
            unique_for_checkpoint: list[dict] = list(existing_for_query)
            for p in query_products:
                dedup_key = str(p.get("itemId") or p.get("url") or "")
                if dedup_key and dedup_key in seen_ids:
                    continue
                if dedup_key:
                    seen_ids.add(dedup_key)
                all_products.append(p)
                unique_for_checkpoint.append(p)
                added += 1
            print(f"  +{added} unique products  |  total so far: {len(all_products):,}")

            # ── Save checkpoint after each query ───────────────────────────────
            checkpoint[q_key] = unique_for_checkpoint
            save_checkpoint(checkpoint)
            print(f"  Checkpoint saved ({len(unique_for_checkpoint)} products for this query).")

    print(f"\nGrand total unique products: {len(all_products):,}")
    save_csv(all_products, OUTPUT_CSV)
    print("Next step: run  python3 merge_datasets.py  to combine with Amazon/iHerb data.")


if __name__ == "__main__":
    asyncio.run(main())
