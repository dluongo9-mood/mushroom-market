"""
Vitacost Mushroom Supplement Scraper — Playwright-based
Uses a fresh browser context per page request.
Scrapes mushroom supplement search queries, deduplicates by product URL/SKU,
and saves to vitacost_mushrooms.csv.

Install deps (once):
    pip3 install playwright
    playwright install chromium

Run:
    python3 scrape_vitacost.py
"""

import asyncio
import csv
import os
import re

from playwright.async_api import async_playwright, TimeoutError as PWTimeout

# ── Search queries ─────────────────────────────────────────────────────────────

SEARCH_QUERIES = [
    {"url": "https://www.vitacost.com/search?term=mushroom+supplement",  "query": "mushroom supplement"},
    {"url": "https://www.vitacost.com/search?term=lion%27s+mane",        "query": "lion's mane"},
    {"url": "https://www.vitacost.com/search?term=reishi+mushroom",      "query": "reishi mushroom"},
    {"url": "https://www.vitacost.com/search?term=chaga+mushroom",       "query": "chaga mushroom"},
    {"url": "https://www.vitacost.com/search?term=cordyceps",            "query": "cordyceps"},
    {"url": "https://www.vitacost.com/search?term=turkey+tail+mushroom", "query": "turkey tail mushroom"},
    {"url": "https://www.vitacost.com/search?term=mushroom+complex",     "query": "mushroom complex"},
    {"url": "https://www.vitacost.com/search?term=mushroom+coffee",      "query": "mushroom coffee"},
]

MAX_PAGES    = 8        # hard cap per query
CONCURRENCY  = 2        # parallel browser instances (semaphore)
PAGE_DELAY   = 0.8      # small delay between batch launches
TIMEOUT_MS   = 28_000   # page navigation timeout
SELECTOR_MS  = 14_000   # selector wait timeout
RESUME_MIN   = 5        # skip a query if we already have >= this many products for it

OUTPUT_CSV   = "vitacost_mushrooms.csv"

FIELDS = [
    "sku", "brand", "productName", "price", "listPrice",
    "rating", "reviewCount", "formFactor", "url", "searchQuery",
]

# ── Form factor inference ──────────────────────────────────────────────────────

def infer_form_factor(text: str):
    t = (text or "").lower()
    for label, keywords in [
        ("Capsule", ["capsule", "vegcap", "vcap", "softgel", "gelcap", "cap "]),
        ("Tablet",  ["tablet", " tab "]),
        ("Powder",  ["powder", "pwdr"]),
        ("Liquid",  ["liquid", "tincture", "fl oz", " ml ", "drops"]),
        ("Gummy",   ["gummy", "gummies"]),
        ("Coffee",  ["coffee", "latte", "brew", "instant"]),
        ("Tea",     [" tea ", "tea bag", "teabag"]),
        ("Chew",    ["chew", "chewable"]),
        ("Spray",   ["spray"]),
        ("Extract", ["extract", "tincture"]),
    ]:
        if any(kw in t for kw in keywords):
            return label
    return None


# ── JavaScript extraction (runs inside browser) ────────────────────────────────

EXTRACT_JS = r"""
(searchQuery) => {
    const clean = s => (s || '').replace(/\s+/g, ' ').trim() || null;

    // Vitacost renders product cards — try multiple container selectors.
    // The site uses dynamic class names so we cast a wide net.
    const CARD_SELECTORS = [
        '.product-thumbnail-container',
        '[class*="productThumbnail"]',
        '[class*="product-thumbnail"]',
        '[class*="ProductTile"]',
        '[class*="product-tile"]',
        '[data-product-id]',
        '[class*="search-result"] li',
        '.product-card',
    ];

    let cards = [];
    for (const sel of CARD_SELECTORS) {
        cards = Array.from(document.querySelectorAll(sel));
        if (cards.length > 0) break;
    }

    // If still nothing, try to grab any <li> that contains a price element
    if (cards.length === 0) {
        cards = Array.from(document.querySelectorAll('li')).filter(li =>
            li.querySelector('[class*="price"], [class*="Price"]')
        );
    }

    return cards.map(card => {

        // ── URL & SKU ──────────────────────────────────────────────────────────
        const linkEl = card.querySelector(
            'a[href*="/vitacost/"], a[href^="/"], a[href*="vitacost.com"]'
        ) || card.querySelector('a[href]');
        const href = linkEl
            ? (linkEl.href || (window.location.origin + linkEl.getAttribute('href')))
            : null;

        // SKU: Vitacost URLs often contain a numeric id at the end, e.g. /product-name-12345678
        let sku = null;
        if (href) {
            const m = href.match(/-(\d{6,12})(?:[/?#]|$)/);
            if (m) sku = m[1];
        }

        // ── Brand ──────────────────────────────────────────────────────────────
        const brandEl = card.querySelector(
            '[class*="brand"], [class*="Brand"], .product-brand, [itemprop="brand"]'
        );
        const brand = clean(brandEl ? brandEl.textContent : null);

        // ── Product name ───────────────────────────────────────────────────────
        const nameSelectors = [
            '[class*="product-name"] a',
            '[class*="productName"] a',
            '[class*="ProductName"] a',
            '[class*="product-name"]',
            '[class*="productName"]',
            'h2 a', 'h3 a', 'h4 a',
            'a[class*="name"]',
        ];
        let productName = null;
        for (const sel of nameSelectors) {
            const el = card.querySelector(sel);
            if (el && el.textContent.trim()) {
                productName = clean(el.textContent);
                break;
            }
        }

        // ── Price (sale / current price) ───────────────────────────────────────
        const priceSelectors = [
            '[class*="sale-price"]',
            '[class*="salePrice"]',
            '[class*="SalePrice"]',
            '[class*="special-price"]',
            '[class*="product-price"]:not([class*="old"]):not([class*="was"]):not([class*="list"])',
            '[class*="price-value"]',
            '[class*="priceValue"]',
            '[class*="current-price"]',
            '[class*="Price"]:not([class*="Old"]):not([class*="Was"]):not([class*="List"])',
            '[class*="price"]:not([class*="old"]):not([class*="was"]):not([class*="list"])',
        ];

        let price = null;
        for (const sel of priceSelectors) {
            const el = card.querySelector(sel);
            if (el) {
                const txt = clean(el.textContent).replace(/,/g, '');
                const m = txt.match(/\$?([\d]+\.[\d]{2})/);
                if (m) { price = parseFloat(m[1]); break; }
            }
        }

        // ── List / original / was-price ────────────────────────────────────────
        const listPriceSelectors = [
            '[class*="old-price"]',
            '[class*="oldPrice"]',
            '[class*="OldPrice"]',
            '[class*="was-price"]',
            '[class*="wasPrice"]',
            '[class*="WasPrice"]',
            '[class*="list-price"]',
            '[class*="listPrice"]',
            '[class*="ListPrice"]',
            '[class*="original-price"]',
            '[class*="originalPrice"]',
            'del', 's',
        ];

        let listPrice = null;
        for (const sel of listPriceSelectors) {
            const el = card.querySelector(sel);
            if (el) {
                const txt = clean(el.textContent).replace(/,/g, '');
                const m = txt.match(/\$?([\d]+\.[\d]{2})/);
                if (m) { listPrice = parseFloat(m[1]); break; }
            }
        }

        // ── Rating ─────────────────────────────────────────────────────────────
        // Try data attributes first (most reliable), then text content
        const ratingEl = card.querySelector(
            '[class*="star"][data-rating], [class*="Star"][data-rating], ' +
            '[class*="rating"][data-value], [aria-label*="out of 5"]'
        );
        let rating = null;
        if (ratingEl) {
            const raw = ratingEl.getAttribute('data-rating')
                     || ratingEl.getAttribute('data-value')
                     || ratingEl.getAttribute('aria-label');
            if (raw) {
                const m = raw.match(/([\d.]+)\s*(?:out of|\/)\s*5/i) || raw.match(/^([\d.]+)$/);
                if (m) rating = parseFloat(m[1]);
            }
        }

        // Fallback: look for a title or aria-label on any star element
        if (rating === null) {
            const starEls = card.querySelectorAll('[class*="star"], [class*="Star"]');
            for (const el of starEls) {
                const attr = el.getAttribute('title') || el.getAttribute('aria-label') || '';
                const m = attr.match(/([\d.]+)\s*(?:out of|\/)\s*5/i);
                if (m) { rating = parseFloat(m[1]); break; }
                // Some sites encode "4-5-stars" style class names
                const cls = el.className || '';
                const cm = cls.match(/(\d(?:[._]\d)?)-?star/i);
                if (cm) { rating = parseFloat(cm[1].replace('_','.')); break; }
            }
        }

        // ── Review count ───────────────────────────────────────────────────────
        const reviewSelectors = [
            '[class*="review-count"]',
            '[class*="reviewCount"]',
            '[class*="ReviewCount"]',
            '[class*="rating-count"]',
            '[class*="ratingCount"]',
            '[class*="num-review"]',
            '[class*="numReview"]',
        ];

        let reviewCount = null;
        for (const sel of reviewSelectors) {
            const el = card.querySelector(sel);
            if (el) {
                const txt = (el.textContent || '').replace(/[,()]/g, '').trim();
                const m = txt.match(/(\d+)/);
                if (m) { reviewCount = parseInt(m[1]); break; }
            }
        }

        // Fallback: scan card text for "(N reviews)" or "N reviews" patterns
        if (reviewCount === null) {
            const innerText = card.innerText || '';
            const m = innerText.match(/\(\s*([\d,]+)\s*(?:reviews?|ratings?)\s*\)/i)
                   || innerText.match(/([\d,]+)\s+(?:reviews?|ratings?)/i);
            if (m) reviewCount = parseInt(m[1].replace(/,/g, ''));
        }

        return {
            sku,
            brand,
            productName,
            price,
            listPrice,
            rating,
            reviewCount,
            url: href,
            searchQuery,
        };
    }).filter(p => p.productName || p.url);  // drop empty cards
}
"""


# ── Scrape a single page URL (fresh browser each time) ────────────────────────

async def scrape_one_page(pw, url: str, query: str, semaphore: asyncio.Semaphore):
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
            )
            await ctx.add_init_script(
                "Object.defineProperty(navigator, 'webdriver', { get: () => undefined });"
            )
            page = await ctx.new_page()
            await page.goto(url, wait_until="domcontentloaded", timeout=TIMEOUT_MS)

            # Wait for at least one product card
            CARD_SELECTORS = [
                ".product-thumbnail-container",
                "[class*='productThumbnail']",
                "[class*='product-thumbnail']",
                "[class*='ProductTile']",
                "[class*='product-tile']",
                "[data-product-id]",
                ".product-card",
            ]

            found = False
            for sel in CARD_SELECTORS:
                try:
                    await page.wait_for_selector(sel, timeout=SELECTOR_MS)
                    found = True
                    break
                except PWTimeout:
                    continue

            if not found:
                # Check if it's a no-results page vs a structural mismatch
                title = await page.title()
                text_snippet = await page.evaluate("() => document.body.innerText.slice(0, 300)")
                no_results = (
                    "no results" in text_snippet.lower()
                    or "0 results" in text_snippet.lower()
                    or "didn't find" in text_snippet.lower()
                )
                if no_results:
                    print(f"    No results on {url}")
                else:
                    print(f"    No product cards found on {url} (title: {title!r}) — trying JS extract anyway")

                if no_results:
                    return []

            # Scroll to trigger lazy loading
            await page.evaluate("""
                () => new Promise(resolve => {
                    let pos = 0;
                    const step = 400;
                    const id = setInterval(() => {
                        pos += step;
                        window.scrollTo(0, pos);
                        if (pos >= document.body.scrollHeight) {
                            clearInterval(id);
                            resolve();
                        }
                    }, 80);
                })
            """)
            await page.wait_for_timeout(500)

            raw = await page.evaluate(EXTRACT_JS, query)

            items = []
            for p in raw:
                name = p.get("productName") or ""
                p["formFactor"] = infer_form_factor(name)
                items.append(p)

            return items

        except PWTimeout:
            print(f"    Timeout on {url}")
            return []
        except Exception as e:
            print(f"    Error on {url}: {e}")
            return []
        finally:
            if browser:
                await browser.close()


# ── Build paginated URLs for one query ─────────────────────────────────────────

def build_page_urls(base_url: str, max_pages: int):
    urls = [base_url]
    for pg in range(2, max_pages + 1):
        sep = "&" if "?" in base_url else "?"
        urls.append(f"{base_url}{sep}pg={pg}")
    return urls


# ── Load existing CSV for resume support ───────────────────────────────────────

def load_existing(path: str):
    """
    Returns:
        products:       list of existing rows
        seen_keys:      set of dedup keys (url or sku)
        query_counts:   {searchQuery: count} for resume logic
    """
    products = []
    seen_keys = set()
    query_counts = {}

    if not os.path.exists(path):
        return products, seen_keys, query_counts

    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            products.append(row)
            key = row.get("url") or row.get("sku") or ""
            if key:
                seen_keys.add(key)
            q = row.get("searchQuery", "")
            query_counts[q] = query_counts.get(q, 0) + 1

    print(f"Loaded {len(products):,} existing products from {path}")
    return products, seen_keys, query_counts


# ── CSV writer ─────────────────────────────────────────────────────────────────

def save_csv(products, path: str) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(products)
    print(f"\nSaved {len(products):,} products -> {path}")


# ── Main ───────────────────────────────────────────────────────────────────────

async def main():
    all_products, seen_keys, query_counts = load_existing(OUTPUT_CSV)
    semaphore = asyncio.Semaphore(CONCURRENCY)

    async with async_playwright() as pw:
        for q_idx, entry in enumerate(SEARCH_QUERIES, 1):
            base_url = entry["url"]
            query    = entry["query"]

            # Resume: skip if this query already produced enough results
            existing_for_query = query_counts.get(query, 0)
            if existing_for_query >= RESUME_MIN:
                print(f"\nSkipping '{query}' — already have {existing_for_query} products")
                continue

            print(f"\n{'='*60}")
            print(f"  Query {q_idx}/{len(SEARCH_QUERIES)}: {query}")
            print(f"  Base URL: {base_url}")
            print(f"{'='*60}")

            page_urls = build_page_urls(base_url, MAX_PAGES)

            # Scrape all pages for this query in batches (semaphore limits concurrency)
            tasks = [scrape_one_page(pw, url, query, semaphore) for url in page_urls]

            batch_results = []
            for batch_i, coro in enumerate(asyncio.as_completed(tasks), 1):
                items = await coro
                batch_results.extend(items)
                # Deduplicate and add to master list
                added = 0
                for p in items:
                    key = p.get("url") or p.get("sku") or ""
                    if not key or key in seen_keys:
                        continue
                    seen_keys.add(key)
                    all_products.append(p)
                    added += 1

                print(f"  Batch {batch_i}/{len(tasks)} | +{added} products | total: {len(all_products):,}")
                await asyncio.sleep(PAGE_DELAY)

            # Stop early if a page returned nothing (end of pagination)
            # Group by page index to detect gaps
            # (as_completed reorders, so we track via accumulated empties heuristically)
            # Save after each query so we can resume
            save_csv(all_products, OUTPUT_CSV)

    print(f"\nGrand total unique products: {len(all_products):,}")
    print(f"Output saved to: {OUTPUT_CSV}")


if __name__ == "__main__":
    asyncio.run(main())
