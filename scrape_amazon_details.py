"""
Amazon Product Details Scraper — Brand + Date First Available

Fetches /dp/{ASIN} via same-origin fetch() inside a Playwright browser
and extracts brand (from #bylineInfo) and date (from detail bullets).

Outputs: amazon_details.csv (asin, brand, dateFirstAvailable)
Supports resume — skips ASINs already in the output CSV.

Run:
    python3 scrape_amazon_details.py
"""

import asyncio
import csv
import json
import re
import time

from playwright.async_api import async_playwright

AMAZON_CSV  = "mushroom_skus_plp.csv"
OUTPUT_CSV  = "amazon_details.csv"
BATCH_SIZE  = 10
CONCURRENCY = 2
TIMEOUT_MS  = 15000

EXTRACT_JS = """
(asins) => {
    const concurrency = 2;
    const results = [];
    let idx = 0, finished = 0;

    return new Promise(resolve => {
        function next() {
            while (idx < asins.length && (idx - finished) < concurrency) {
                const asin = asins[idx++];
                const controller = new AbortController();
                const timer = setTimeout(() => controller.abort(), 15000);

                fetch('/dp/' + asin, {
                    headers: { 'Accept': 'text/html' },
                    signal: controller.signal,
                })
                .then(r => { clearTimeout(timer); return r.text(); })
                .then(html => {
                    if (html.length < 5000) {
                        results.push({ asin, brand: null, date: null, parentASIN: null });
                        return;
                    }
                    const doc = new DOMParser().parseFromString(html, 'text/html');

                    // Brand from bylineInfo
                    const byline = doc.querySelector('#bylineInfo, a#bylineInfo');
                    let brand = null;
                    if (byline) {
                        brand = byline.textContent
                            .replace(/Visit the|Store|Brand:|by/gi, '')
                            .replace(/\\s+/g, ' ')
                            .trim() || null;
                    }

                    // Parent ASIN
                    const pm = html.match(/parentASIN["\\s:=]+["']?([A-Z0-9]{10})/i);
                    const parentASIN = pm ? pm[1] : null;

                    // Date from detail bullets
                    const bullets = Array.from(doc.querySelectorAll(
                        '#detailBullets_feature_div li, #detailBulletsWrapper_feature_div li'
                    ));
                    const dateItem = bullets.find(li =>
                        /date first available/i.test(li.textContent)
                    );
                    let date = null;
                    if (dateItem) {
                        const m = dateItem.textContent
                            .replace(/[^\\w\\s,]/g, ' ')
                            .replace(/\\s+/g, ' ')
                            .match(/Date First Available\\s+(.+)/i);
                        if (m) date = m[1].trim();
                    }

                    doc.documentElement.remove();
                    results.push({ asin, brand, date, parentASIN });
                })
                .catch(() => {
                    clearTimeout(timer);
                    results.push({ asin, brand: null, date: null });
                })
                .finally(() => {
                    finished++;
                    if (finished === asins.length) resolve(results);
                    else next();
                });
            }
        }
        next();
    });
}
"""


def load_asins():
    with open(AMAZON_CSV, newline="", encoding="utf-8") as f:
        return [r["asin"] for r in csv.DictReader(f) if r.get("asin")]


def load_existing():
    try:
        with open(OUTPUT_CSV, newline="", encoding="utf-8") as f:
            return {r["asin"]: r for r in csv.DictReader(f)}
    except FileNotFoundError:
        return {}


def save_results(results):
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["asin", "brand", "dateFirstAvailable", "parentASIN"])
        writer.writeheader()
        for asin in sorted(results.keys()):
            r = results[asin]
            writer.writerow({
                "asin": asin,
                "brand": r.get("brand") or "",
                "dateFirstAvailable": r.get("dateFirstAvailable") or r.get("date") or "",
                "parentASIN": r.get("parentASIN") or "",
            })


async def main():
    all_asins = load_asins()
    existing = load_existing()
    # Retry ASINs that have no brand (may have been blocked on previous run)
    remaining = [a for a in all_asins
                 if a not in existing or not existing[a].get("brand")]
    print(f"Total ASINs: {len(all_asins):,}")
    print(f"Already scraped: {len(existing):,}")
    print(f"Remaining: {len(remaining):,}")

    if not remaining:
        print("All done!")
        return

    # Merge existing into results
    results = {}
    for asin, row in existing.items():
        results[asin] = {
            "brand": row.get("brand") or None,
            "dateFirstAvailable": row.get("dateFirstAvailable") or None,
            "parentASIN": row.get("parentASIN") or None,
        }

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=True,
            args=["--disable-blink-features=AutomationControlled", "--no-sandbox"],
        )
        ctx = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1440, "height": 900},
        )
        await ctx.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', { get: () => undefined });"
        )
        page = await ctx.new_page()

        print("Establishing session…")
        await page.goto("https://www.amazon.com/s?k=mushroom+supplement",
                        wait_until="domcontentloaded", timeout=30_000)
        await page.wait_for_timeout(2000)
        print("Session ready.\n")

        total_batches = (len(remaining) + BATCH_SIZE - 1) // BATCH_SIZE
        start_time = time.time()
        brands_found = 0
        dates_found = 0

        for batch_num in range(total_batches):
            batch_start = batch_num * BATCH_SIZE
            batch_asins = remaining[batch_start:batch_start + BATCH_SIZE]

            try:
                batch_results = await page.evaluate(EXTRACT_JS, batch_asins)
            except Exception as e:
                print(f"  Batch {batch_num+1} error: {e}")
                save_results(results)
                continue

            for r in batch_results:
                asin = r["asin"]
                brand = r.get("brand")
                date = r.get("date")
                parent = r.get("parentASIN")
                results[asin] = {"brand": brand, "dateFirstAvailable": date, "parentASIN": parent}
                if brand:
                    brands_found += 1
                if date:
                    dates_found += 1

            done = batch_start + len(batch_asins)
            elapsed = time.time() - start_time
            rate = done / elapsed if elapsed > 0 else 0
            eta = (len(remaining) - done) / rate if rate > 0 else 0

            bf = sum(1 for r in batch_results if r.get("brand"))
            df = sum(1 for r in batch_results if r.get("date"))

            print(f"  Batch {batch_num+1}/{total_batches}  |  "
                  f"+{bf} brands, +{df} dates  |  "
                  f"{done:,}/{len(remaining):,}  |  "
                  f"totals: {brands_found} brands, {dates_found} dates  |  "
                  f"ETA {eta:.0f}s")

            if batch_num % 5 == 0:
                save_results(results)

            await asyncio.sleep(2)

        await browser.close()

    save_results(results)
    total_brands = sum(1 for r in results.values() if r.get("brand"))
    total_dates = sum(1 for r in results.values() if r.get("dateFirstAvailable"))
    print(f"\nDone! {total_brands:,} brands, {total_dates:,} dates out of {len(results):,} ASINs")
    print(f"Saved → {OUTPUT_CSV}")


if __name__ == "__main__":
    asyncio.run(main())
