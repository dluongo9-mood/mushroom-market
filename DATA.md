# Data Files

CSV data files are not tracked in git. Run the scrapers below to generate them.

## Scraping Order

Run in this order (each step depends on the previous):

### 1. Amazon Products
```bash
python3 scrape_mushrooms.py
```
**Output:** `mushroom_skus_plp.csv` (~2,800 products)
**Requires:** Playwright (`pip install playwright && playwright install chromium`)

### 2. Amazon Product Details (brands, dates)
```bash
python3 scrape_amazon_details.py
```
**Output:** `amazon_details.csv`, `amazon_dates.csv`
**Requires:** Playwright, existing `mushroom_skus_plp.csv`

### 3. iHerb
```bash
python3 scrape_iherb.py
```
**Output:** `iherb_mushrooms.csv` (~500 products)

### 4. Faire
```bash
python3 scrape_faire.py
```
**Output:** `faire_mushrooms.csv` (~2,300 products, ~900 pass consumable filter)
**Requires:** Playwright

### 5. DTC Brands
```bash
python3 scrape_dtc.py
```
**Output:** `dtc_mushrooms.csv` (~1,000 products)
**Requires:** Playwright for blocked Shopify stores

### 6. Keepa Historical Data
```bash
python3 scrape_keepa.py YOUR_KEEPA_API_KEY
```
**Output:** `keepa_history.csv` (1.9M+ data points)
**Requires:** Keepa API key ($21+/mo at keepa.com), existing `mushroom_skus_plp.csv`
**Note:** Rate-limited. At 3 tokens/min, full scrape takes ~2 hours for 2,600 ASINs.

### 7. Merge & Normalize
```bash
python3 merge_datasets.py
```
**Output:** `combined_mushrooms.csv` (all sources merged, form factors normalized)

### 8. Build Dashboard
```bash
python3 build_dashboard.py
```
**Output:** `mushroom_dashboard.html`, `mushroom_brand_directory.csv`
**Requires:** All CSVs above + `brightfield_hemp_thc.xlsx` (if available)

## Quick Start (minimal)

If you just want to rebuild the dashboard from existing data:
```bash
python3 build_dashboard.py
open mushroom_dashboard.html
```

## Dependencies
```bash
pip install playwright plotly pandas matplotlib matplotlib-venn
playwright install chromium
```
