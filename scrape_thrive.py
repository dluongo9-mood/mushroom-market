"""
Thrive Market Mushroom Supplement Scraper — Playwright-based
Uses a fresh browser context per page request.
Intercepts Thrive's internal API responses for clean JSON data;
falls back to HTML extraction from product cards if API interception yields nothing.
Deduplicates by productId, saves thrive_mushrooms.csv.

Install deps (once):
    pip3 install playwright
    playwright install chromium

Run:
    python3 scrape_thrive.py
"""

import asyncio
import csv
import json
import re
import os

from playwright.async_api import async_playwright, TimeoutError as PWTimeout

# ── URLs to scrape ─────────────────────────────────────────────────────────────
# Mix of category pages and search pages.
SOURCES = [
    {"url": "https://thrivemarket.com/l/supplements/mushrooms",                  "query": "mushrooms"},
    {"url": "https://thrivemarket.com/l/supplements/adaptogen-herbs-mushrooms",  "query": "adaptogen mushrooms"},
    {"url": "https://thrivemarket.com/l/supplements/nootropics-brain-health",    "query": "nootropics brain health"},
    {"url": "https://thrivemarket.com/search?q=mushroom+supplement",             "query": "mushroom supplement"},
    {"url": "https://thrivemarket.com/search?q=lion%27s+mane",                  "query": "lion's mane"},
    {"url": "https://thrivemarket.com/search?q=reishi",                         "query": "reishi"},
    {"url": "https://thrivemarket.com/search?q=mushroom+coffee",                "query": "mushroom coffee"},
]

MAX_PAGES   = 8        # hard cap per source URL
CONCURRENCY = 2        # parallel browser instances
PAGE_DELAY  = 0.5      # seconds between launching tasks

OUTPUT_CSV  = "thrive_mushrooms.csv"

# Minimum existing results for a URL to be considered already scraped (resume)
RESUME_THRESHOLD = 5

FIELDS = [
    "productId", "brand", "productName", "retailPrice", "memberPrice",
    "rating", "reviewCount", "formFactor", "url", "searchQuery",
]

# ── User-agent string ──────────────────────────────────────────────────────────
UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)


# ── Form factor inference ──────────────────────────────────────────────────────

def parse_form_factor(text):
    t = (text or "").lower()
    for label, keywords in [
        ("Capsule", ["capsule", "vegcap", "vcap", "softgel", "gelcap", "cap "]),
        ("Tablet",  ["tablet", " tab "]),
        ("Powder",  ["powder", "pwdr"]),
        ("Liquid",  ["liquid", "tincture", "fl oz", " ml ", " oz ", "drops"]),
        ("Gummy",   ["gummy", "gummies"]),
        ("Coffee",  ["coffee", "latte", "brew", "espresso"]),
        ("Tea",     [" tea ", "teabag", "tea bag"]),
        ("Chew",    ["chew", "chewable"]),
        ("Spray",   ["spray"]),
    ]:
        if any(kw in t for kw in keywords):
            return label
    return None


# ── Normalise a raw price string → float or None ──────────────────────────────

def parse_price(raw):
    if raw is None:
        return None
    if isinstance(raw, (int, float)):
        return float(raw)
    cleaned = re.sub(r"[^\d.]", "", str(raw))
    try:
        return float(cleaned) if cleaned else None
    except ValueError:
        return None


# ── Parse products out of a Thrive API JSON payload ──────────────────────────

def extract_from_api_payload(payload, query):
    """
    Thrive's catalog API returns a variety of shapes depending on endpoint.
    We try several known structures and extract what we can.
    Returns a list of product dicts (un-deduplicated).
    """
    products = []

    # Flatten nested structures to find the product list
    candidates = []

    def find_lists(obj, depth=0):
        if depth > 6:
            return
        if isinstance(obj, list) and len(obj) > 0 and isinstance(obj[0], dict):
            candidates.append(obj)
        elif isinstance(obj, dict):
            for v in obj.values():
                find_lists(v, depth + 1)

    find_lists(payload)

    # Use the largest list of dicts as the product array
    if not candidates:
        return []
    product_list = max(candidates, key=len)

    for item in product_list:
        # productId — try various key names
        pid = (
            item.get("id")
            or item.get("productId")
            or item.get("product_id")
            or item.get("sku")
            or item.get("variantId")
        )

        # brand
        brand = (
            item.get("brand")
            or item.get("brandName")
            or item.get("brand_name")
            or (item.get("brand_attributes") or {}).get("name")
            or None
        )
        if isinstance(brand, dict):
            brand = brand.get("name") or brand.get("title")

        # product name
        name = (
            item.get("name")
            or item.get("title")
            or item.get("productName")
            or item.get("product_name")
        )

        # prices
        retail_raw = (
            item.get("retailPrice")
            or item.get("retail_price")
            or item.get("compareAtPrice")
            or item.get("compare_at_price")
            or item.get("originalPrice")
            or item.get("msrp")
        )
        member_raw = (
            item.get("memberPrice")
            or item.get("member_price")
            or item.get("price")
            or item.get("salePrice")
            or item.get("sale_price")
            or item.get("finalPrice")
        )
        # Thrive sometimes nests prices under a "pricing" key
        pricing = item.get("pricing") or item.get("prices") or {}
        if isinstance(pricing, dict):
            retail_raw = retail_raw or pricing.get("retail") or pricing.get("retailPrice")
            member_raw = member_raw or pricing.get("member") or pricing.get("memberPrice") or pricing.get("sale")

        retail_price = parse_price(retail_raw)
        member_price = parse_price(member_raw)

        # rating
        rating_raw = (
            item.get("rating")
            or item.get("averageRating")
            or item.get("average_rating")
            or (item.get("reviews") or {}).get("averageRating")
        )
        try:
            rating = float(rating_raw) if rating_raw is not None else None
        except (ValueError, TypeError):
            rating = None

        # review count
        rc_raw = (
            item.get("reviewCount")
            or item.get("review_count")
            or item.get("reviewsCount")
            or item.get("numReviews")
            or (item.get("reviews") or {}).get("count")
            or (item.get("reviews") or {}).get("total")
        )
        try:
            review_count = int(rc_raw) if rc_raw is not None else None
        except (ValueError, TypeError):
            review_count = None

        # URL slug
        slug = (
            item.get("url")
            or item.get("slug")
            or item.get("urlSlug")
            or item.get("url_slug")
            or item.get("handle")
            or item.get("path")
        )
        if slug and not slug.startswith("http"):
            slug = "https://thrivemarket.com" + ("/" if not slug.startswith("/") else "") + slug
        elif not slug and pid:
            slug = f"https://thrivemarket.com/p/{pid}"

        # Extract productId from URL if not yet found
        if not pid and slug:
            m = re.search(r"/p/([^/?#]+)", slug)
            if m:
                pid = m.group(1)

        # Skip clearly non-product entries
        if not name and not pid:
            continue

        products.append({
            "productId":    str(pid) if pid else None,
            "brand":        str(brand).strip() if brand else None,
            "productName":  str(name).strip() if name else None,
            "retailPrice":  retail_price,
            "memberPrice":  member_price,
            "rating":       rating,
            "reviewCount":  review_count,
            "url":          slug,
            "searchQuery":  query,
            "formFactor":   parse_form_factor(str(name) if name else ""),
        })

    return products


# ── JavaScript extraction — HTML fallback (runs inside browser) ───────────────

EXTRACT_JS = r"""
(searchQuery) => {
    const clean = s => (s || '').replace(/\s+/g,' ').trim() || null;

    // Try a broad set of selectors for Thrive product cards
    const CARD_SELECTORS = [
        '[data-testid="product-card"]',
        '[class*="ProductCard"]',
        '[class*="product-card"]',
        '[class*="ProductTile"]',
        '[class*="product-tile"]',
        '[class*="ProductItem"]',
        'li[class*="product"]',
        'div[class*="product"][data-id]',
    ];

    let cards = [];
    for (const sel of CARD_SELECTORS) {
        const found = Array.from(document.querySelectorAll(sel));
        if (found.length > 0) { cards = found; break; }
    }

    return cards.map(card => {

        // ── URL / productId ──
        const linkEl = card.querySelector('a[href*="/p/"]') || card.querySelector('a[href]');
        const href   = linkEl ? linkEl.href : null;

        let productId = card.getAttribute('data-id')
                     || card.getAttribute('data-product-id')
                     || card.getAttribute('data-sku')
                     || null;
        if (!productId && href) {
            const m = href.match(/\/p\/([^/?#]+)/);
            if (m) productId = m[1];
        }

        // ── Brand ──
        const BRAND_SELS = [
            '[class*="brand" i]',
            '[class*="Brand"]',
            '[data-testid*="brand" i]',
        ];
        let brand = null;
        for (const sel of BRAND_SELS) {
            const el = card.querySelector(sel);
            if (el) { brand = clean(el.textContent); break; }
        }

        // ── Product name ──
        const NAME_SELS = [
            '[class*="productName" i]',
            '[class*="product-name" i]',
            '[class*="ProductName"]',
            '[class*="title" i]',
            '[data-testid*="name" i]',
            'h2', 'h3', 'h4',
        ];
        let productName = null;
        for (const sel of NAME_SELS) {
            const el = card.querySelector(sel);
            if (el) { productName = clean(el.textContent); break; }
        }

        // ── Prices ──
        // Look for two price elements: retail (struck through) and member
        const allPriceEls = Array.from(card.querySelectorAll(
            '[class*="price" i], [class*="Price"]'
        ));

        let retailPrice = null, memberPrice = null;

        // A del/s/strike element typically holds the retail (crossed-out) price
        const strikeEl = card.querySelector('del, s, strike, [class*="retail" i], [class*="compare" i], [class*="original" i], [class*="crossed" i]');
        if (strikeEl) {
            const m = (strikeEl.textContent || '').replace(/,/g,'').match(/[\d]+\.?[\d]*/);
            if (m) retailPrice = parseFloat(m[0]);
        }

        // Member / sale price — prefer an element labelled "member" or primary price
        const memberEl = card.querySelector('[class*="member" i], [class*="sale" i], [class*="Member"]');
        if (memberEl) {
            const m = (memberEl.textContent || '').replace(/,/g,'').match(/[\d]+\.?[\d]*/);
            if (m) memberPrice = parseFloat(m[0]);
        }

        // If we only found one price so far, try harder
        if (memberPrice === null && allPriceEls.length > 0) {
            for (const el of allPriceEls) {
                const m = (el.textContent || '').replace(/,/g,'').match(/[\d]+\.?[\d]*/);
                if (m) { memberPrice = parseFloat(m[0]); break; }
            }
        }

        // If prices are identical but one should be retail, keep memberPrice as-is
        // and leave retailPrice null unless a distinct struck price was found.

        // ── Rating ──
        let rating = null, reviewCount = null;
        const ratingEl = card.querySelector(
            '[class*="rating" i], [class*="Rating"], [class*="stars" i], [aria-label*="star" i]'
        );
        if (ratingEl) {
            const ariaLabel = ratingEl.getAttribute('aria-label') || '';
            const rm = ariaLabel.match(/([\d.]+)\s*(out of|\/)\s*5/i) ||
                       ariaLabel.match(/([\d.]+)\s*stars?/i);
            if (rm) rating = parseFloat(rm[1]);

            // Also try inner text
            if (rating === null) {
                const rtm = (ratingEl.textContent || '').match(/([\d.]+)/);
                if (rtm && parseFloat(rtm[1]) <= 5) rating = parseFloat(rtm[1]);
            }
        }

        // Review count — often in format "(123)" or "123 reviews"
        const reviewEl = card.querySelector(
            '[class*="review" i], [class*="Review"], [class*="count" i]'
        );
        if (reviewEl) {
            const rtxt = reviewEl.textContent || '';
            const cm = rtxt.replace(/,/g,'').match(/\(?([\d]+)\)?/);
            if (cm) reviewCount = parseInt(cm[1]);
        }
        // Fallback: look for parenthetical number near stars
        if (reviewCount === null) {
            const txt = card.textContent || '';
            const cm = txt.match(/\(([\d,]+)\)\s*$/m);
            if (cm) reviewCount = parseInt(cm[1].replace(/,/g,''));
        }

        return {
            productId,
            brand,
            productName,
            retailPrice,
            memberPrice,
            rating,
            reviewCount,
            url: href,
            searchQuery,
        };
    }).filter(p => p.productId || p.productName);
}
"""


# ── Dismiss any signup / modal overlays ───────────────────────────────────────

async def dismiss_modals(page):
    """Attempt to close cookie banners, newsletter modals, and login gates."""
    # Press Escape to close any open modal
    try:
        await page.keyboard.press("Escape")
        await page.wait_for_timeout(300)
    except Exception:
        pass

    # Click common close/dismiss buttons
    CLOSE_SELS = [
        '[aria-label="Close"]',
        '[aria-label="close"]',
        'button[class*="close" i]',
        'button[class*="dismiss" i]',
        '[data-testid="modal-close"]',
        '[class*="modal-close" i]',
        '[class*="overlay-close" i]',
        'button:has-text("No thanks")',
        'button:has-text("Not now")',
        'button:has-text("Maybe later")',
        'button:has-text("Skip")',
        'button:has-text("Continue browsing")',
        'button:has-text("Continue as guest")',
        'button:has-text("Browse as guest")',
    ]
    for sel in CLOSE_SELS:
        try:
            el = page.locator(sel).first
            if await el.is_visible(timeout=500):
                await el.click(timeout=500)
                await page.wait_for_timeout(300)
                break
        except Exception:
            continue


# ── Scroll to load infinite-scroll content ────────────────────────────────────

async def scroll_to_load(page, max_scrolls=8):
    """Scroll down incrementally to trigger lazy loads / infinite scroll."""
    try:
        await page.evaluate("""
            () => new Promise(resolve => {
                let last = -1, count = 0;
                const id = setInterval(() => {
                    window.scrollBy(0, 600);
                    const h = document.body.scrollHeight;
                    if (h === last || count++ > 25) { clearInterval(id); resolve(); }
                    last = h;
                }, 200);
            })
        """)
        await page.wait_for_timeout(800)
    except Exception:
        pass


# ── Check for redirect to login/signup ────────────────────────────────────────

def is_auth_wall(url_str, title):
    patterns = ["login", "signup", "sign-up", "register", "join", "membership"]
    combined = (url_str + " " + title).lower()
    return any(p in combined for p in patterns)


# ── Scrape a single page URL ──────────────────────────────────────────────────

async def scrape_one_page(pw, url, query, semaphore):
    """
    Open URL in a fresh browser.  Intercept Thrive API JSON responses.
    Falls back to HTML extraction if API interception yields nothing.
    Returns a list of product dicts.
    """
    async with semaphore:
        browser = None
        api_products = []
        api_event    = asyncio.Event()

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
                user_agent=UA,
                viewport={"width": 1440, "height": 900},
                locale="en-US",
                extra_http_headers={
                    "Accept-Language": "en-US,en;q=0.9",
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                },
            )
            await ctx.add_init_script(
                "Object.defineProperty(navigator, 'webdriver', { get: () => undefined });"
            )
            page = await ctx.new_page()

            # ── Intercept API responses ────────────────────────────────────────
            API_PATTERNS = [
                "/api/products",
                "/api/v1/catalog",
                "/api/v2/catalog",
                "/api/search",
                "/catalog/",
                "/products.json",
                "algolia",
                "search/results",
            ]

            async def handle_response(resp):
                try:
                    rurl = resp.url.lower()
                    ct   = (resp.headers.get("content-type") or "").lower()
                    if "json" not in ct:
                        return
                    if not any(pat in rurl for pat in API_PATTERNS):
                        return
                    body = await resp.json()
                    extracted = extract_from_api_payload(body, query)
                    if extracted:
                        api_products.extend(extracted)
                        api_event.set()
                        print(f"    [API] intercepted {len(extracted)} products from {resp.url[:80]}")
                except Exception:
                    pass

            page.on("response", handle_response)

            # ── Navigate ───────────────────────────────────────────────────────
            try:
                await page.goto(url, wait_until="domcontentloaded", timeout=30_000)
            except PWTimeout:
                print(f"    Timeout navigating to {url}")
                return []

            # Check for auth wall
            cur_url = page.url
            title   = await page.title()
            if is_auth_wall(cur_url, title):
                print(f"    WARNING: auth wall detected at {cur_url!r}  — skipping")
                return []

            # Wait for React to render
            await page.wait_for_timeout(2000)

            # Dismiss any modal overlays
            await dismiss_modals(page)

            # Scroll to trigger lazy loads / infinite scroll
            await scroll_to_load(page)

            # Allow a little more time for API calls to complete
            await page.wait_for_timeout(1000)

            # ── Decide: use API results or fall back to HTML ───────────────────
            if api_products:
                items = api_products
                print(f"    [API mode] {len(items)} raw items from {url}")
            else:
                # HTML fallback
                raw = await page.evaluate(EXTRACT_JS, query)
                items = raw
                print(f"    [HTML mode] {len(items)} raw items from {url}")

            # ── Post-process ───────────────────────────────────────────────────
            results = []
            for p in items:
                # Skip entries with no identifying info
                if not p.get("productId") and not p.get("productName"):
                    continue
                # Normalise prices
                p["retailPrice"] = parse_price(p.get("retailPrice"))
                p["memberPrice"] = parse_price(p.get("memberPrice"))
                # Infer form factor from product name if not set
                if not p.get("formFactor"):
                    p["formFactor"] = parse_form_factor(
                        (p.get("productName") or "") + " " + (p.get("brand") or "")
                    )
                # Ensure URL is absolute
                url_val = p.get("url") or ""
                if url_val and not url_val.startswith("http"):
                    p["url"] = "https://thrivemarket.com" + ("" if url_val.startswith("/") else "/") + url_val
                # Ensure searchQuery is set
                p["searchQuery"] = query
                results.append(p)

            return results

        except Exception as e:
            print(f"    Error on {url}: {e}")
            return []
        finally:
            if browser:
                await browser.close()


# ── Build paginated URL list for a source ────────────────────────────────────

def build_page_urls(base_url, max_pages):
    """
    Generate up to max_pages URLs for a source.
    Thrive uses ?page=N for category pages and appends &page=N for search.
    """
    urls = [base_url]
    for pg in range(2, max_pages + 1):
        if "?" in base_url:
            urls.append(f"{base_url}&page={pg}")
        else:
            urls.append(f"{base_url}?page={pg}")
    return urls


# ── CSV helpers ───────────────────────────────────────────────────────────────

def load_existing_csv(path):
    """Return (set of productIds, dict of query -> count) from existing CSV."""
    seen_ids = set()
    query_counts = {}
    if not os.path.exists(path):
        return seen_ids, query_counts
    try:
        with open(path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                pid = row.get("productId") or ""
                if pid:
                    seen_ids.add(pid)
                q = row.get("searchQuery") or ""
                query_counts[q] = query_counts.get(q, 0) + 1
    except Exception:
        pass
    return seen_ids, query_counts


def save_csv(products, path):
    """Write all products to CSV, overwriting if exists."""
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(products)
    print(f"\nSaved {len(products):,} products -> {path}")


# ── Main ───────────────────────────────────────────────────────────────────────

async def main():
    # Load any existing results for resume support
    existing_ids, query_counts = load_existing_csv(OUTPUT_CSV)
    all_products_map = {}  # productId/name -> dict  (for dedup)

    # If existing CSV has data, we load it into memory so we can re-save a
    # merged file at the end.
    if existing_ids:
        print(f"Resuming: found {len(existing_ids):,} existing product IDs in {OUTPUT_CSV}")
        with open(OUTPUT_CSV, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                key = row.get("productId") or row.get("productName") or ""
                if key:
                    all_products_map[key] = row

    semaphore = asyncio.Semaphore(CONCURRENCY)

    async with async_playwright() as pw:
        for source in SOURCES:
            base_url = source["url"]
            query    = source["query"]

            # Resume: skip if we already have enough results for this query
            existing_count = query_counts.get(query, 0)
            if existing_count >= RESUME_THRESHOLD:
                print(f"\nSkipping {query!r} — {existing_count} results already in CSV")
                continue

            print(f"\n{'='*62}")
            print(f"  Query   : {query}")
            print(f"  Base URL: {base_url}")
            print(f"{'='*62}")

            page_urls = build_page_urls(base_url, MAX_PAGES)

            # Scrape pages concurrently (capped by semaphore)
            tasks = [scrape_one_page(pw, u, query, semaphore) for u in page_urls]
            source_new = 0

            for i, coro in enumerate(asyncio.as_completed(tasks), 1):
                items = await coro

                # Deduplicate against all_products_map
                added = 0
                for p in items:
                    key = p.get("productId") or p.get("productName") or ""
                    if not key:
                        continue
                    if key in all_products_map:
                        continue
                    all_products_map[key] = p
                    added += 1
                    source_new += 1

                print(f"  Batch {i}/{len(tasks)} done  +{added} new  |  total unique: {len(all_products_map):,}")
                await asyncio.sleep(PAGE_DELAY)

            print(f"  => {source_new} new products from {query!r}")

            # Stop paginating if first page returned nothing (no more pages)
            # (already handled: tasks complete even if empty)

    all_products = list(all_products_map.values())
    print(f"\nGrand total unique products: {len(all_products):,}")

    if all_products:
        save_csv(all_products, OUTPUT_CSV)
    else:
        print("No products found. CSV not written.")

    print("Next step: run  python3 merge_datasets.py  to combine with other sources.")


if __name__ == "__main__":
    asyncio.run(main())
