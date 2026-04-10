---
date: 2026-04-07
time: "16:00"
tags: [mushroom, dashboard, form-factor, keepa, data-export, team-feedback]
status: complete
---

# Mushroom Dashboard v2 — Team Feedback

## Goal

Iterate on the mushroom market dashboard based on team feedback. Rework form factor segmentation, add raw data exports, exclude non-functional products, and research additional data sources.

## Team Feedback (verbatim)

### Data Access
- Need underlying raw data outside of product explorer
- Specifically want: Marimekko market map data, review growth by brand and form factor, product launches by form factor

### Form Factor Rework
Current segmentation is too generic. Team's ideal segmentation:

| New Category | Definition | Current Categories Affected |
|---|---|---|
| Coffee | powder, ground, beans, instant | Coffee (keep as-is) |
| Mushroom Powder | pure supplement powder | Split from Powder |
| Other Drink Powders | tea, chocolate, matcha mixes | Split from Powder, Tea, Chocolate |
| RTD Beverages | cans, bottles | Split from Drink |
| Gummies | (keep as-is) | Gummy |
| Chocolates | bars, not drink mixes | Split from Chocolate |
| Liquids | tinctures, liquid supplements | Liquid (remove RTD) |
| All Other | everything else | Capsule, Tablet, Topical, etc. |

Key changes:
- Split Powder into "Mushroom Powder (pure supplement)" vs "Other Drink Powders (tea, chocolate, etc.)"
- Tea → bucket into Drink Powder or RTD
- Liquids should be tinctures/supplements only, move RTD cans → RTD Beverages
- Chocolates = bars only, not drink mixes

### Data to Exclude
- Phillips Farm (culinary mushrooms in "Other")
- Dearmine (not functional mushroom)
- Guyaki (Yerba Mate, not mushroom)

### Additional Data Sources
- Other ways to triangulate growth for DTC-heavy brands (outside Amazon)?
- Easy ways to scrape for DTC Amanita sales?

## Plan

### Step 1: Rework form factor classification
Modify `FORM_FACTOR_RULES` in `build_dashboard.py`:

```python
NEW_FORM_FACTORS = {
    "Coffee": r"coffee|espresso|\bbrew\b|k[\s-]*cup|ground.*coffee|instant.*coffee|coffee.*bean",
    "Mushroom Powder": r"mushroom\s+powder|lions?\s*mane\s+powder|reishi\s+powder|chaga\s+powder|cordyceps\s+powder",
    "Other Drink Powder": r"matcha|chocolate\s+mix|cocoa\s+mix|hot\s+chocolate|latte\s+mix|chai\s+mix|protein.*mushroom|drink\s+mix",
    "RTD Beverage": r"sparkling|seltzer|can\b|cans\b|\bshot\b|ready\s*to\s*drink|rtd|\bbroth\b",
    "Gummy": r"gumm(?:y|ies)",
    "Chocolate": r"chocolate\s+bar|cocoa\s+bar|cacao\s+bar|\bbar\b.*chocolate|chocolate.*\bbar\b",
    "Liquid": r"tincture|liquid\s+(?:supplement|extract|drops)|drops?\b(?!.*sparkling)",
    "Capsule/Tablet": r"capsule|softgel|tablet|caplet|vegcap|vcap|liposomal|\d+\s*ct\b",
}
```

Priority order matters — check Coffee before Drink Powder, check Mushroom Powder before generic Powder.

### Step 2: Add data exclusions
Add to EXCLUDE_PATTERNS or brand exclusion:
- "Phillips Farm" or "Phillips Gourmet" → culinary mushroom brand
- "Dearmine" → not functional mushroom
- "Guyaki" → Yerba Mate

### Step 3: Add raw data export buttons
Add downloadable CSV links to the dashboard:
- Market map data (form factor × brand × revenue)
- Review growth by brand (Keepa time series)
- Review growth by form factor (Keepa time series)
- Product launches by form factor (from dateFirstAvailable)

### Step 4: Research additional data sources
- SimilarWeb/Zyla traffic for DTC mushroom brands
- Google Trends for Amanita + mushroom supplement keywords
- Reddit/social listening for DTC brand mentions

## What Was Done

### Form factor rework
- Updated `FORM_FACTOR_RULES` in `build_dashboard.py` to new 8-category segmentation
- Updated `FF_NORMALIZE` dict to map raw Amazon values to new categories
- **Normalized CSVs directly** (`mushroom_skus_plp.csv` and `combined_mushrooms.csv`) so Capsule + Tablet are merged at the data level, not just the viz layer
- Increased Marimekko chart to show top 10 form factors (was 8) so Chocolate is visible as its own category

### Exclusions
- Added Phillips Farm, Dearmine, Guyaki to `EXCLUDE_PATTERNS`

### CSV export
- Added "Export Market Map CSV" button above Marimekko chart
- Fixed JS syntax error (extra closing brace was breaking all scripts)
- Export downloads form factor × brand × revenue data

### Deployment
- Hosted at https://dluongo9-mood.github.io/supplement-market-dashboard/mushroom.html
- Deploy from `/tmp/supplement-deploy/` repo

## Files Modified

| File | Changes |
|------|---------|
| `build_dashboard.py` | Form factor rework, exclusions, CSV export, top 10 categories |
| `mushroom_skus_plp.csv` | Form factors normalized (Capsule/Tablet merged) |
| `combined_mushrooms.csv` | Form factors normalized (Capsule/Tablet merged) |

## Context for Future Sessions
- Mushroom project at `/Users/davidluongo/amazon-paapi/`
- 21 charts in dashboard, 5 use Keepa data (1.9M data points)
- 3,228 combined products across Amazon, iHerb, Faire, DTC, Target
- Dashboard has "Ask AI" section with Claude API integration
- Form factor classification is in `FORM_FACTOR_RULES` dict (lines 63-90)
- Key function: `infer_form_factor()` at line 273
