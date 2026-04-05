"""
Interactive Mushroom Market Dashboard — Plotly HTML

Reads the 3 marketplace CSVs, normalizes (reusing logic from build_market_analysis.py),
and generates a self-contained HTML dashboard with 6 interactive charts.

Run:
    python3 build_dashboard.py
    open mushroom_dashboard.html
"""

import csv
import re
import json
import statistics
from collections import defaultdict, Counter
from pathlib import Path

import plotly.graph_objects as go
from plotly.subplots import make_subplots

# ── Config ─────────────────────────────────────────────────────────────────────
AMAZON_CSV   = "mushroom_skus_plp.csv"
IHERB_CSV    = "iherb_mushrooms.csv"
FAIRE_CSV    = "faire_mushrooms.csv"
DATES_CSV    = "amazon_dates.csv"
DETAILS_CSV  = "amazon_details.csv"
KEEPA_CSV    = "keepa_history.csv"
OUTPUT_HTML  = "mushroom_dashboard.html"
DTC_CSV      = "dtc_mushrooms.csv"
TARGET_CSV   = "target_mushrooms.csv"

COLORS = {
    "Amazon":       "#FF9900",
    "iHerb":        "#6BBE45",
    "Faire":        "#5B63FE",
    "DTC":           "#9B59B6",   # purple — direct-to-consumer
    "Target":        "#CC0000",   # Target red
}

# ── Reuse normalization helpers from build_market_analysis.py ──────────────────

MUSHROOM_KEYWORDS = [
    ("Lion's Mane",   [r"lion.?s?\s*mane", r"hericium"]),
    ("Reishi",        [r"reishi", r"ganoderma", r"lingzhi"]),
    ("Chaga",         [r"chaga", r"inonotus"]),
    ("Cordyceps",     [r"cordyceps"]),
    ("Turkey Tail",   [r"turkey\s*tail", r"trametes", r"coriolus"]),
    ("Maitake",       [r"maitake", r"grifola"]),
    ("Shiitake",      [r"shiitake", r"lentinula"]),
    ("Tremella",      [r"tremella"]),
    ("Agaricus",      [r"agaricus", r"blazei"]),
    ("AHCC",          [r"ahcc"]),
    ("King Trumpet",  [r"king\s*trumpet", r"eryngii"]),
    ("Oyster",        [r"oyster\s*mushroom", r"pleurotus"]),
]

MULTI_BLEND_PATTERNS = [
    r"\d+[\s-]*in[\s-]*\d+", r"multi\s*mushroom",
    r"mushroom\s*(complex|blend|mix|multi|spectrum)", r"full[\s-]*spectrum",
]

FORM_FACTOR_RULES = [
    # Coffee MUST come before Tea/Powder so "mushroom coffee powder" → Coffee
    ("Coffee",      [r"coffee", r"espresso", r"\bbrew\b", r"k[\s-]*cup"]),
    # Tea: matcha, chai, latte (but NOT if already matched Coffee above)
    ("Tea",         [r"\btea\b", r"matcha", r"\bchai\b", r"\blatte\b"]),
    ("Capsule",     [r"capsule", r"vegcap", r"vcap", r"vegicap", r"plantcap",
                     r"softgel", r"gelcap", r"veggie\s*cap", r"caplet",
                     r"liposomal", r"\d+\s*ct\b", r"\d+\s*count"]),
    ("Tablet",      [r"tablet", r"lozenge"]),
    ("Powder",      [r"powder", r"pwdr", r"granule"]),  # non-coffee powder
    ("Gummy",       [r"gumm(?:y|ies)"]),
    ("Liquid",      [r"liquid", r"tincture", r"drops?", r"syrup", r"elixir",
                     r"fl\s*oz", r"\bml\b"]),
    ("Chocolate",   [r"chocolat", r"cocoa", r"cacao"]),
    ("Drink",       [r"\bdrink", r"beverage", r"\bshot\b", r"\bjuice\b",
                     r"sparkling", r"seltzer", r"\bbroth\b"]),
    ("Chew",        [r"\bchew", r"chewable"]),
    ("Pouch",       [r"pouch"]),
    ("Bar",         [r"\bbar\b", r"snack\s*bar", r"protein\s*bar"]),
    ("Snack",       [r"\bchips?\b", r"crisp", r"jerky"]),
    ("Topical",     [r"cream", r"serum", r"balm", r"topical", r"salve"]),
    ("Spray",       [r"spray"]),
    ("Grow Kit",    [r"grow\s*kit", r"spawn", r"substrate"]),
    ("Whole/Dried", [r"dried\s+\w*\s*mushroom", r"sliced\s+\w*\s*mushroom",
                     r"whole\s+mushroom", r"\bfresh\s+\w*\s*mushroom",
                     r"organic\s+(shiitake|oyster|maitake|porcini|cremini|baby\s*bella)\b",
                     r"frozen\s+.*mushroom", r"mushroom\s+medley"]),
]

# ── Junk / non-supplement filter ───────────────────────────────────────────────
# Products matching these are excluded from the dataset entirely
EXCLUDE_PATTERNS = [
    # Merch / apparel
    r"\bmug\b", r"\bmugs\b", r"\btumbler\b(?!.*supplement)", r"\bhat\b(?!.*shiitake)", r"\bbeanie\b",
    r"\bshirt\b", r"\bt-shirt\b", r"\btee\b(?!.*tea)", r"\bhoodie\b", r"\bsweatshirt\b",
    r"\bsweater\b", r"\bpullover\b", r"\bcardigan\b", r"\bsweatpants?\b", r"\bpajama\b",
    r"\bpants?\b(?!.*plant)", r"\bshorts\b", r"\blegging\b", r"\bboxer\b",
    r"\bunderwear\b", r"\bsocks?\b", r"\bcrew sock\b",
    r"\bonesie\b", r"\bbodysuit\b", r"\bromper\b", r"\boverall\b",
    r"\binfant clothing\b", r"\bbaby clothing\b",
    r"\btoy\b(?!.*turkey)", r"\bplush\b(?!.*mushroom.*supplement)", r"\bstuffed\b", r"\bsquishy\b",
    r"\brubber mushroom\b", r"\bllb\b.*\btoy",
    r"\bposter\b", r"\bcanvas\b(?!.*bag)", r"\bwall art\b", r"\bart print\b",
    r"\bframed\b", r"\bsticker\b(?!.*mushroom.*supplement)", r"\bdecal\b",
    r"\bmagnet\b", r"\benamel pin\b", r"\bpatch\b(?!.*nicotine|.*glutathione)",
    r"\bcurtain\b", r"\bshower\b(?!.*gel)", r"\bbedding\b", r"\bpillow\b", r"\bhook pillow\b",
    r"\bcushion\b", r"\bblanket\b", r"\bcomforter\b", r"\bduvet\b", r"\bquilt\b",
    r"\bphone case\b", r"\biphone\b",
    r"\bearring\b", r"\bnecklace\b", r"\bjewelry\b", r"\bbracelet\b",
    r"\bring\s*hoop\b", r"\bcharm\b(?!.*charcoal)", r"\bkeychain\b",
    r"\bnovelty\b", r"\bcostume\b", r"\bhalloween\b",
    r"\bfabric\b", r"\bstainless steel\b",
    r"\btrademark fine art\b", r"\b3drose\b", r"\bambesonne\b", r"\bbuyenlarge\b", r"\blunarable\b",
    r"\bscience source\b", r"\bcharting nature\b", r"\bvintage inky\b",
    r"\bapron\b", r"\btote\b", r"\bpurse\b", r"\bwallet\b", r"\bumbrella\b",
    r"\bbackpack\b", r"\bclutch bag\b",
    r"\bfunny\b.*\bhunting\b", r"\bvintage\s+inky\b",
    # Home decor / garden / non-consumable
    r"\bcandle\b", r"\bincense\b", r"\bbath bomb\b",
    r"\bsoap\b(?!.*berry)", r"\bguest soap\b",
    r"\btowel\b", r"\bnapkin\b", r"\btablecloth\b", r"\bplacemat\b",
    r"\bcoaster\b", r"\bornament\b", r"\bfigurine\b",
    r"\bceramic\b(?!.*cup.*tea)", r"\bporcelain\b(?!.*cup)",
    r"\bglass mushroom\b", r"\bwood\w*\s*mushroom\b", r"\bwooden\s*mushroom\b",
    r"\bcrystal mushroom\b", r"\bgemstone\s*mushroom\b", r"\bamethyst\s*mushroom\b",
    r"\bhealing\s*crystal\b", r"\bchakra\s*crystal\b",
    r"\bresin mushroom\b", r"\bartificial\b.*\bmushroom\b",
    r"\bcarved\s*mushroom\b", r"\bmushroom\s*wood\s*base\b",
    r"\bmushroom\s*lamp\b", r"\blight\s*set\b",
    r"\bglow in the dark\b",
    r"\bgarden\b(?!.*of.*life)", r"\bplanter\b", r"\bwatering\b",
    r"\bpot\b(?!.*potent)", r"\bgnome\b", r"\bsasquatch\b",
    r"\bpuzzle\b", r"\bcard game\b", r"\bplaying card\b", r"\bboard game\b",
    r"\bgilt.edged\b.*\bcard", r"\bmini card\b", r"\btarot\b", r"\bdeck\b",
    r"\bcoloring\b", r"\bjournal\b(?!.*supplement)", r"\blogbook\b",
    r"\bheadband\b", r"\bgrip sock\b", r"\bfuzzy\b.*\bsock\b",
    r"\bteapot\b", r"\bplate\b(?!.*gold)", r"\bbowl\b(?!.*supplement)",
    # Books (not cookbooks)
    r"\bbook\b(?!.*cook)",
    r"\bdemystified\b", r"\bexplorer\b(?!.*mushroom.*extract)", r"\bmanual\b(?!.*extract)",
    r"\bguide\b(?!.*supplement|.*usage|.*dosage)",
    # Grow kits / spawn (not supplements)
    r"\bgrow\s*kit\b", r"\bspawn\b", r"\bsubstrate\b",
    # Food / grocery / non-supplement
    r"\bpopcorn\b", r"\bjerky\b", r"\bchips?\b(?!.*mushroom.*supplement)",
    r"\bseasoning\b", r"\bspice\b(?!.*mushroom)", r"\bcooking with\b",
    r"\btrout fillet\b", r"\blobster mushroom\b",
    r"\bmalt ball\b", r"\bpoop\b",
    r"\bsoup\b", r"\bramen\b", r"\bnoodle\b", r"\bpasta\b(?!.*supplement)",
    r"\bravioli\b", r"\brisotto\b", r"\bpizza\b", r"\balfredo\b",
    r"\bpasta sauce\b", r"\bspaghetti sauce\b", r"\bcream of mushroom\b",
    r"\bcondensed.*mushroom\b", r"\bstir.fry\b", r"\bmeal kit\b",
    r"\bfrozen.*(?:bowl|pizza|ravioli|medley|entree|meal|patties?)\b",
    r"\bfresh.*mushrooms?\b", r"\bsliced.*mushrooms?\b", r"\bwhole.*mushrooms?\b",
    r"\bbaby bella\b", r"\bcremini\b", r"\bportobello\b(?!.*extract)",
    r"\bwhite mushrooms?\b", r"\benoki\b(?!.*supplement)", r"\bbeech mushroom\b",
    r"\bstems\s*(?:&|and)\s*pieces\b", r"\bpieces\s*(?:&|and)\s*stems\b",
    r"\bsquishmallow\b", r"\bmocchi\b", r"\bnintendo\b", r"\bsuper mario\b",
    r"\baurora\b.*\bstuffed\b",
    r"\bbone broth\b", r"\bpork chop\b", r"\bbeef patt", r"\bchicken.*rice\b",
    r"\bdel monte\b", r"\bforest to fork\b(?!.*extract)", r"\bshiloh farms\b",
    r"\bfigurine\b", r"\bsagebrook\b", r"\bfield guide\b",
    r"\b(?:hardcover|paperback)\b",
    # Dried cooking mushrooms / grocery produce
    r"\bdried.*(?:porcini|chanterelle|morel|wood\s*ear|black fungus)(?!.*extract|.*supplement)",
    r"\bdried.*mushrooms?.*for cooking\b",
    r"\bdehydrated.*(?:gourmet|morchella|edible).*mushroom",
    r"\bedible.*(?:black fungus|wood ear)",
    r"\bfor cooking\b(?!.*supplement)",
    r"\bcooking cream\b", r"\bparmalat\b",
    r"\bvigor(?:ous)?\s*mountains\b(?!.*supplement|.*extract|.*capsule)",
    r"\broland foods\b", r"\bonetang\b", r"\bkopabana\b",
    r"\bplug spawn\b", r"\bmycelium.*plug\b",
    r"\bvelvet\s+mushroom\b", r"\bvelvet\s+antler\s+mushroom\b",
    r"\bbai\s+hua\s+rong\b",
    r"\b365 by whole\b",
    r"\bproduce\b.*(?:bella|cremini|mushroom)",
    r"\bsmallhold\b.*fresh",
    # Pet
    r"\bfor dogs?\b", r"\bfor cats?\b", r"\bfor pets?\b",
    r"\bcanine\b", r"\bfeline\b", r"\bpet supplement\b", r"\bpet chew\b",
    r"\bdog treat\b", r"\bcat treat\b", r"\bdog health\b",
    r"\bbark\s*&\s*whiskers\b", r"\bpet defenders\b",
    r"\bmycodog\b", r"\bpets are kids\b", r"\bfifth & fido\b", r"\bnaturvet\b",
]

# Consumable product indicators — used to filter Faire where search results
# contain many non-supplement items (decor, merch, gifts, garden, etc.)
CONSUMABLE_PATTERNS = [
    r"supplement", r"capsule", r"tincture", r"extract", r"powder",
    r"gumm(?:y|ies)", r"tablet", r"liquid", r"drops?(?!.*ear)", r"coffee",
    r"\btea\b", r"latte", r"elixir", r"blend", r"complex",
    r"tonic", r"syrup", r"chocolat", r"cocoa", r"cacao",
    r"drink", r"beverage", r"broth", r"honey",
    r"serum", r"cream(?!.*ice)", r"balm", r"topical", r"salve",
    r"\d+\s*(?:ct|count|capsule|cap|oz|ml|mg|gram|serving)",
    r"immune", r"wellness", r"adaptogen", r"nootropic",
    r"lion.?s\s*mane", r"reishi", r"chaga", r"cordyceps", r"turkey\s*tail",
    r"maitake", r"tremella", r"agaricus", r"mycelium",
    r"beta.glucan", r"functional",
    r"energy.*support", r"focus.*support", r"calm\b", r"sleep\b", r"stress\b",
    r"mushroom.*(?:support|formula|boost|daily|super)",
    r"organic.*mushroom.*(?:oz|mg|ct)",
    r"infused", r"superfood", r"probiotic",
    r"grow\s*kit", r"spawn", r"substrate",  # grow kits are niche but relevant
]

def is_excluded(title):
    t = title.lower()
    return any(re.search(p, t) for p in EXCLUDE_PATTERNS)

def is_consumable(title):
    """Check if a product title indicates a consumable/supplement product."""
    t = title.lower()
    return any(re.search(p, t) for p in CONSUMABLE_PATTERNS)

# Mushroom keywords — Faire products must mention mushrooms to be included
MUSHROOM_REQUIRED = [
    r"mushroom", r"fungi", r"lion.?s.mane", r"reishi", r"chaga",
    r"cordyceps", r"cordy", r"turkey.tail", r"maitake", r"shiitake", r"tremella",
    r"agaricus", r"hericium", r"ganoderma", r"trametes", r"mycelium", r"myco",
    r"beta.glucan", r"ahcc", r"shroom", r"mycolog", r"adaptogen",
]

def has_mushroom_keyword(title):
    t = title.lower()
    return any(re.search(p, t) for p in MUSHROOM_REQUIRED)


def extract_mushroom_types(text):
    if not text:
        return []
    t = text.lower()
    found = []
    for name, patterns in MUSHROOM_KEYWORDS:
        for pat in patterns:
            if re.search(pat, t, re.IGNORECASE):
                found.append(name)
                break
    is_multi = any(re.search(p, t, re.IGNORECASE) for p in MULTI_BLEND_PATTERNS)
    if is_multi and "Multi-Blend" not in found:
        found.append("Multi-Blend")
    if len(found) >= 3 and "Multi-Blend" not in found:
        found.append("Multi-Blend")
    return sorted(set(found))


# Clean whitelist for Amazon's raw formFactor field
CLEAN_FORM_FACTORS = {
    "capsule", "capsules", "powder", "gummy", "liquid", "softgel",
    "tablet", "drop", "drops", "chewable", "ground", "packet",
    "bag", "instant", "caplet", "syrup",
}

# Map messy Amazon values to clean labels
FF_NORMALIZE = {
    "capsule": "Capsule", "capsules": "Capsule", "softgel": "Capsule",
    "caplet": "Capsule", "liposomal": "Capsule",
    "powder": "Powder", "granule": "Powder", "packet": "Powder",
    "gummy": "Gummy",
    "liquid": "Liquid", "drop": "Liquid", "drops": "Liquid", "syrup": "Liquid",
    "tablet": "Tablet", "chewable": "Chew",
    "ground": "Coffee", "instant": "Coffee",
    "matcha": "Tea", "bag": "Tea",
    "whole bean": "Coffee",
    "dark chocolate": "Chocolate", "chocolate": "Chocolate",
    "fabric": None, "stainless steel": None,  # non-supplement junk
}


def infer_form_factor(text):
    if not text:
        return None
    t = text.lower()
    for label, patterns in FORM_FACTOR_RULES:
        for pat in patterns:
            if re.search(pat, t):
                return label
    return None


def clean_amazon_form_factor(raw_ff, title):
    """Infer form factor from title first, fall back to cleaned Amazon field."""
    # Always try title first — most reliable
    from_title = infer_form_factor(title)
    if from_title:
        return from_title
    # Fall back to Amazon's field only if it's in the clean whitelist
    if raw_ff:
        key = raw_ff.strip().lower()
        if key in FF_NORMALIZE:
            return FF_NORMALIZE[key]
    return None


def parse_float(v):
    try:
        return float(v) if v not in (None, "", "None") else None
    except (ValueError, TypeError):
        return None

def parse_int(v):
    if v in (None, "", "None"):
        return None
    s = str(v).replace(",", "").strip()
    # Handle Amazon format: "4K+ bought in past month", "800+ bought in past month"
    m = re.match(r'([\d.]+)\s*[Kk]\+?', s)
    if m:
        return int(float(m.group(1)) * 1000)
    m = re.match(r'([\d.]+)', s)
    if m:
        return int(float(m.group(1)))
    return None

def read_csv(path):
    try:
        with open(path, newline="", encoding="utf-8") as f:
            return list(csv.DictReader(f))
    except FileNotFoundError:
        print(f"  Warning: {path} not found")
        return []


# ── Amazon brand extraction ────────────────────────────────────────────────────

# Prefix → canonical brand name (longest match wins)
BRAND_PREFIXES = {
    "om mushroom superfood": "Om Mushrooms",
    "om mushroom matrix": "Om Mushrooms",
    "om mushroom": "Om Mushrooms",
    "om mushrooms": "Om Mushrooms",
    "om master blend": "Om Mushrooms",
    "om lion's mane": "Om Mushrooms",
    "om cordyceps": "Om Mushrooms",
    "om turkey tail": "Om Mushrooms",
    "om reishi": "Om Mushrooms",
    "om chaga": "Om Mushrooms",
    "real mushrooms": "Real Mushrooms",
    "host defense": "Host Defense",
    "four sigmatic": "Four Sigmatic",
    "ryze superfoods": "RYZE",
    "ryze ": "RYZE",
    "micro ingredients": "Micro Ingredients",
    "freshcap mushrooms": "FreshCap",
    "freshcap ": "FreshCap",
    "laird superfood": "Laird Superfood",
    "clean nutraceuticals": "Clean Nutraceuticals",
    "double wood supplements": "Double Wood",
    "double wood ": "Double Wood",
    "nature's way": "Nature's Way",
    "new chapter": "New Chapter",
    "garden of life": "Garden of Life",
    "ancient nutrition": "Ancient Nutrition",
    "mushroom wisdom": "Mushroom Wisdom",
    "the genius brand": "Genius",
    "genius mushrooms": "Genius",
    "nutricost ": "Nutricost",
    "nootropics depot": "Nootropics Depot",
    "everyday dose": "Everyday Dose",
    "peak performance": "Peak Performance",
    "force factor": "Force Factor",
    "maryruth's": "MaryRuth's",
    "maryruth ": "MaryRuth's",
    "mary ruth": "MaryRuth's",
    "north spore": "North Spore",
    "dr. moritz": "Dr. Moritz",
    "mn mother nature": "MN Mother Nature",
    "mother nature": "MN Mother Nature",
    "pella nutrition": "Pella Nutrition",
    "wellness labsrx": "Wellness LabsRx",
    "plant people": "Plant People",
    "gaia herbs": "Gaia Herbs",
    "life cykel": "Life Cykel",
    "lifecykel": "Life Cykel",
    "piping rock": "Piping Rock",
    "bulksupplements.com": "BulkSupplements",
    "hawaii pharm": "Hawaii Pharm",
    "terrasoul superfoods": "Terrasoul Superfoods",
    "kiki green": "Kiki Green",
    "anima mundi": "Anima Mundi",
    "tiger milk": "Tiger Milk",
    "rainbo ": "Rainbo",
    "dr. emil": "Dr. Emil Nutrition",
    "pure original ingredients": "Pure Original Ingredients",
    "oldsoul superfood": "OldSoul Superfood",
    "longevity botanicals": "Longevity Botanicals",
    "ganoone ": "GanoOne",
    "drops of nature": "Drops of Nature",
    "organic traditions": "Organic Traditions",
    "now foods": "NOW Foods",
    "addall focus": "AddAll",
    "vitacup ": "VitaCup",
    "grateful earth": "Grateful Earth",
    "cuppa coffee": "Cuppa Coffee",
    "la republica": "La Republica",
    "harmonic grounds": "Harmonic Grounds",
    "hifas da terra": "Hifas da Terra",
    "vigorous mountains": "Vigorous Mountains",
    "rich gano": "Rich Gano",
    "auri nutrition": "Auri Nutrition",
    "andrew lessman": "Andrew Lessman",
    "bio krauter": "Bio Krauter",
    "hippie chicks": "Hippie Chicks",
    "orgain ": "Orgain",
    "vimergy ": "Vimergy",
    "mudwtr": "MUD\\WTR",
    "mud/wtr": "MUD\\WTR",
    "noomadic": "Noomadic",
    "horbäach": "Horbaach",
    "horbaach": "Horbaach",
    "venture pal": "Venture Pal",
    "naturealm": "Naturealm",
    "proprietary nootropic": "Proprietary Nootropic",
    "365 by whole": "365 by Whole Foods",
    "solaray ": "Solaray",
    # Additional brands found from high-sold unknowns
    "quality of life": "Quality of Life",
    "nutriflair": "NutriFlair",
    "nutrivein": "Nutrivein",
    "country farms": "Country Farms",
    "stonehenge health": "Stonehenge Health",
    "carlyle ": "Carlyle",
    "effective nutra": "Effective Nutra",
    "swanson ": "Swanson",
    "standard process": "Standard Process",
    "pureshrooms": "PureShrooms",
    "iqjoe ": "IQJOE",
    "shroondna": "ShroomDNA",
    "shroomdna": "ShroomDNA",
    "aceworkz": "ACEWORKZ",
    "fungies ": "Fungies",
    "alice mushrooms": "Alice Mushrooms",
    "cadane ": "Cadane",
    "noomist": "NooMist",
    "noomost": "NooMost",
    "powdervitamin": "PowderVitamin",
    "organixx": "Organixx",
    "sacred lotus": "Sacred Lotus",
    "health concerns": "Health Concerns",
    "pristine's": "Pristine's",
    "vita green": "Vita Green",
    "kaya naturals": "Kaya Naturals",
    "dr. mercola": "Dr. Mercola",
    "naturvet": "NaturVet",
    "back to the roots": "Back to the Roots",
    "wild & organic": "Wild & Organic",
    "satoomi": "Satoomi",
    "fidus ": "Fidus",
    "prescribed for life": "Prescribed for Life",
    "cuppa ": "Cuppa",
    "mw polar": "MW Polar",
    "pets are kids": "Pets Are Kids",
    "fifth & fido": "Fifth & Fido",
    "mycodog": "MycoDog",
    "moon juice": "Moon Juice",
    "brēz ": "BRĒZ",
    "brez ": "BRĒZ",
    "luv health": "LUV Health",
    "california gold nutrition": "California Gold Nutrition",
    "hpd rx": "HPD Rx",
    # Brands from Chrome scraping round 2
    "onnit ": "ONNIT",
    "juni ": "JUNI",
    "grüns ": "Grüns", "gruns ": "Grüns",
    "bulletproof ": "Bulletproof",
    "odyssey ": "Odyssey Elixir", "odyssey elixir": "Odyssey Elixir",
    "magic mind": "Magic Mind",
    "lucid ": "Lucid",
    "terra elmnt": "TERRA ELMNT",
    "melting forest": "Melting Forest",
    "new age ": "NEW AGE",
    "purify life": "Purify Life",
    "guayaki ": "Guayaki",
    "nutraharmony": "NutraHarmony",
    "deal supplement": "DEAL SUPPLEMENT",
    "trip ": "TRIP",
    "shroomology": "Shroomology",
    "zhugua": "ZhuGuaCo",
    "betterbrand": "BetterBrand",
    "bioptimizers": "BIOptimizers",
    "sundafone": "SUNDAFONE",
    "supermush": "SuperMush",
    "ellie's best": "Ellie's Best",
    "moment ": "Moment",
    "spring valley": "Spring Valley",
    "umeken ": "Umeken",
    "american biosciences": "American BioSciences",
    "parker naturals": "Parker Naturals",
    "nature restore": "Nature Restore",
    "fresh nutrition": "Fresh Nutrition",
    "hellomush": "Hellomush",
    "eco-taste": "ECO-TASTE",
    "lifeboost": "Lifeboost",
    "kalba ": "Kalba",
    "organo ": "ORGANO",
    "javita ": "Javita",
    "mycern ": "MyCern",
    "activatedyou": "ActivatedYou",
    "gano coffee": "Gano Coffee",
    "lifeable ": "Lifeable",
    "evlution ": "Evlution",
    "herbamama": "HERBAMAMA",
    "research labs": "Research Labs",
    "liquidhealth": "LiquidHealth",
    "north american herb": "North American Herb",
    "designs for health": "Designs for Health",
    "natura health": "Natura Health Products",
    "mojo ": "Mojo",
    "vitamatic ": "Vitamatic",
    # Round 3 — extracted from remaining unknowns
    "nature's truth": "Nature's Truth",
    "nature's bounty": "Nature's Bounty",
    "nutra champs": "NutraChamps", "nutrachamps": "NutraChamps",
    "tribe organics": "TRIBE ORGANICS", "tribe ": "TRIBE",
    "frontier co-op": "Frontier Co-op",
    "premier research": "Premier Research Labs",
    "sayan ": "Sayan",
    "hodgins harvest": "Hodgins Harvest",
    "wild brew": "Wild Brew",
    "ra hygge": "Rå Hygge", "rå hygge": "Rå Hygge",
    "sayuu ": "SAYUU",
    "cherie sweet heart": "Cherie Sweet Heart",
    "charlotte's web": "Charlotte's Web",
    "life extension": "Life Extension",
    "naturalma ": "Naturalma",
    "ganoherb": "GANOHERB",
    "nuyu ": "Nuyu",
    "atlas+": "Atlas+",
    "energinseng": "Energinseng",
    "dodjivi": "Dodjivi",
    "zatural ": "Zatural",
    "gano excel": "Gano Excel",
    "muse ": "Muse",
    "fully joe": "Fully Joe",
    "mycobrew": "MycoBrew",
    "planetary herbals": "Planetary Herbals",
    "gobiotix": "GOBIOTIX",
    "brainmd": "BrainMD",
    "vbot ": "Vbot",
    "saga serenity": "SAGA Serenity",
    "fantastic fungi": "Fantastic Fungi",
    "megafood": "MegaFood",
    "immortal mycelium": "Immortal Mycelium",
    "happie fungi": "Happie Fungi Fusion",
    "rave mood": "RAVE Mood",
    "daytrip ": "Daytrip",
    "chroma ": "CHROMA",
    "more. longevity": "More. Longevity",
    "lyfe ": "LYFE",
    "pescience": "PEScience",
    "herbtonics": "Herbtonics",
    "smnutrition": "SMNutrition",
    "troomy": "Troomy",
    "lifesource vitamins": "LifeSource Vitamins",
    "herbana manufacturing": "HERBANA",
    "fatcaps": "FatCaps",
    "best earth naturals": "Best Earth Naturals",
    "rise-n-shine": "Rise-N-Shine",
    "toniiq": "Toniiq",
    "lyfefuel": "LyfeFuel",
    "san zentori": "SAN ZENTORI",
    "fungi botanica": "Fungi Botanica",
    "dmoose": "DMoose",
    "happy fox": "Happy Fox",
    "sakoon": "Sakoon",
    "lunakai": "LUNAKAI",
    "mycolove": "Mycolove",
    "mycl ": "MYCL",
    "goldmine supershroom": "Goldmine",
    "highvibe": "HighVibe",
    "unbeetabrew": "UNBEETABREW",
    "live conscious": "Live Conscious",
    "sun potion": "Sun Potion",
    "eversio wellness": "Eversio Wellness",
    "mind master": "Mind Master",
    "troop ": "Troop",
    "econugenics": "EcoNugenics",
    "wonderday": "WonderDay",
    "revive md": "Revive MD",
    "hiwell": "HiWell",
    "naturejam": "Naturejam",
    "sollo ": "Sollo",
    "waiora": "Waiora",
    "fresh healthcare": "Fresh Healthcare",
    "zolotus": "Zolotus",
    "road trip ": "Road Trip",
    "pure essence labs": "Pure Essence Labs", "pure essence": "Pure Essence Labs",
    "putrizen": "PUTRIZEN",
    "mycomedix": "MYCOMedix",
    "omogs ": "OMOGS",
    "fito medic": "FITO MEDIC'S Lab",
    "canlist": "CANLIST",
    "shaney": "SHANEY",
    "groovy bee": "Groovy Bee",
    "acappella": "ACAPPELLA",
    "nature target": "Nature Target",
    "shizam": "SHIZAM",
    "this little house": "This Little House of Mine",
    "the enclare": "Enclare Nutrition",
    "noto master": "NOTO",
    "purica ": "PURICA",
    "cancer sciences": "Cancer Sciences",
    "matcha dna": "Matcha DNA",
    "yege ": "YEGE",
    "leafhaven": "LEAFHAVEN",
    "way beyond": "WAY BEYOND",
    "sbg salveo": "SBG Salveo",
    "herbal hive": "Herbal Hive",
    "z natural foods": "Z Natural Foods",
    "bioschwartz": "BioSchwartz",
    "bioemblem": "BioEmblem",
    "profine truffle": "PROFINE",
    "shroomi ": "Shroomi",
    "shroomo ": "Shroomo",
    "ohmyes": "OhmYes",
    "bare organics": "BareOrganics", "bareorganics": "BareOrganics",
    "zhou ": "ZHOU",
    "arbonne ": "Arbonne",
    "joybrü": "JOYBRÜ", "joybru": "JOYBRÜ",
    "spice appeal": "Spice Appeal",
    "golden field": "Golden Field",
    "plntbsd": "PLNTBSD",
    "boostup": "Boostup",
    "karkze": "KARKZE",
    "margxo": "Margxo",
    "edara wellness": "Edara Wellness",
    "muuk": "MUUK",
    "fabula ": "Fabula",
    "raaka ": "Raaka",
    "holistic bin": "Holistic Bin",
    "naturelo": "NATURELO",
    "wixar ": "Wixar",
    "wild foods": "Wild Foods",
    "fire dept. coffee": "Fire Dept. Coffee",
    "oldsoul": "OldSoul Superfood",
    "kiki green": "Kiki Green",
    "cymbiotika": "CYMBIOTIKA",
    "auraShroom": "AuraShroom", "aurashroom": "AuraShroom",
    "fidus ": "Fidus",
    "bluebonnet": "BlueBonnet",
    "swanson ": "Swanson",
}

# ── Brand database ────────────────────────────────────────────────────────────
# Load canonical brand database (built from all marketplace sources)
BRAND_DB_PATH = Path("brand_database.json")

def _load_brand_db():
    """Load brand database and build lookup structures."""
    import json
    db = {}
    if BRAND_DB_PATH.exists():
        with open(BRAND_DB_PATH, encoding="utf-8") as f:
            db = json.load(f)

    # Build lowercase → canonical mapping
    lower_to_canonical = {}
    for canonical, data in db.items():
        lower_to_canonical[canonical.lower()] = canonical
        for alias in data.get("aliases", []):
            lower_to_canonical[alias.lower()] = canonical

    # Also add all BRAND_PREFIXES as overrides (hand-curated = higher priority)
    for prefix, canonical in BRAND_PREFIXES.items():
        lower_to_canonical[prefix.strip().lower()] = canonical

    # Build sorted list for scanning titles (longest first to avoid partial matches)
    scan_entries = sorted(lower_to_canonical.items(), key=lambda x: -len(x[0]))

    # Filter scan entries: skip names that are too short or too generic for mid-title matching
    UNSAFE_SCAN = {"the", "real", "now", "pure", "om", "raw", "one", "vibe", "glow",
        "rise", "trip", "flow", "fire", "dawn", "hive", "core", "zen", "muse",
        "food", "foods", "health", "nutrition", "coffee", "tea", "cocoa",
        "chocolate", "supplement", "organic", "natural", "wellness", "powder",
        "complex", "blend", "formula", "focus", "energy", "sleep", "calm",
        "immune", "daily", "complete", "generic", "japanese", "chinese",
        "ashwagandha", "liposomal", "cognitive function", "fruiting bodies",
        "chanterelle", "porcini", "morel", "inner elevate", "180g",
        # Home decor / non-supplement brands (shouldn't match supplement titles)
        "ambesonne", "3drose", "lunarable", "buyenlarge", "trademark fine art",
        "science source", "vintage inky cap", "charting nature",
        "3drose large", "vintage edible", "vintage chanterelles",
        "chinese edible fungi", "edible american", "superfoods company"}
    scan_safe = [(k, v) for k, v in scan_entries if len(k) >= 4 and k not in UNSAFE_SCAN]

    return lower_to_canonical, scan_entries, scan_safe

_BRAND_LOOKUP, _BRAND_SCAN_ALL, _BRAND_SCAN_SAFE = _load_brand_db()

# Sort prefixes by length descending so longest prefix matches first
_SORTED_PREFIXES = sorted(BRAND_PREFIXES.items(), key=lambda x: -len(x[0]))

_GENERIC_STARTS = frozenset({"mushroom", "mushrooms", "organic", "lions", "lion's",
    "turkey", "10", "10-in-1", "functional", "11in1", "adaptogenic",
    "instant", "decaf", "premium", "natural", "herbal", "pure", "super",
    "advanced", "extra", "high", "vegan", "usda", "freeze-dried",
    "next-gen", "world's", "certified", "supplement", "capsule", "capsules",
    "powder", "gummies", "gummy", "extract", "tincture", "liquid", "drops",
    "complex", "blend", "formula", "coffee", "tea", "cocoa", "chocolate",
    "immunity", "immune", "brain", "focus", "energy", "sleep", "calm",
    "cordyceps", "reishi", "chaga", "maitake", "shiitake", "tremella",
    "16x", "2", "100", "4200mg", "8", "14", "25-in-1", "strength"})


def extract_amazon_brand(title):
    if not title:
        return None
    t_lower = title.lower().strip()

    # 1. Known prefix → canonical name (hand-curated, highest confidence)
    for prefix, canonical in _SORTED_PREFIXES:
        if t_lower.startswith(prefix):
            return canonical

    # 2. Delimiter-based extraction
    for sep in [" - ", " – ", " — ", " | "]:
        if sep in title:
            c = title.split(sep)[0].strip()
            words = c.lower().split()
            # Short prefix: take as-is if it looks like a brand
            if (len(words) <= 4 and len(c) < 40
                    and words[0] not in _GENERIC_STARTS):
                return c
            # Longer prefix: extract first 1-3 brand-like words
            brand_words = []
            for w in c.split():
                wl = w.lower().rstrip(",;:")
                if wl in _GENERIC_STARTS or wl.startswith(
                    ("mushroom", "organic", "lion", "turkey", "cordycep",
                     "reishi", "chaga", "supplement", "capsule", "powder",
                     "gumm", "extract", "tincture", "liquid", "blend",
                     "complex", "coffee", "chocolate", "cocoa", "10-in",
                     "11in", "13in", "19in", "20in")):
                    break
                if w[0].isupper() or w.isupper():
                    brand_words.append(w)
                else:
                    break
                if len(brand_words) >= 3:
                    break
            if brand_words:
                candidate = " ".join(brand_words)
                if 3 <= len(candidate) < 40:
                    return candidate
            break  # only try first delimiter found

    # 3. "by BrandName" pattern (e.g. "Mushroom Blend by Four Sigmatic")
    by_match = re.search(r'\bby\s+([A-Z][A-Za-zé&\'.]+(?:\s+[A-Z][A-Za-zé&\'.]+){0,3})', title)
    if by_match:
        candidate = by_match.group(1).strip()
        if 3 <= len(candidate) < 35 and candidate.lower().split()[0] not in _GENERIC_STARTS:
            return candidate

    # 4. Comma split (less reliable)
    if "," in title:
        c = title.split(",")[0].strip()
        words = c.lower().split()
        if (len(words) <= 3 and len(c) < 30
                and words[0] not in _GENERIC_STARTS):
            return c

    # 5. Brand database scan — search for known brand names anywhere in the title
    #    Only use "safe" entries (4+ chars, not generic words) to avoid false matches
    for brand_lower, canonical in _BRAND_SCAN_SAFE:
        # Word-boundary match to avoid partial matches (e.g. "om" in "lemon")
        if re.search(r'(?<![a-zA-Z])' + re.escape(brand_lower) + r'(?![a-zA-Z])', t_lower):
            return canonical

    return None


# ── Load & normalize ──────────────────────────────────────────────────────────

def load_product_details():
    """Load ASIN → {brand, dateFirstAvailable, parentASIN} from scraped product pages.
    Propagates brand from parent to children via parentASIN."""
    details = {}
    # Legacy dates file
    for r in read_csv(DATES_CSV):
        if r.get("dateFirstAvailable"):
            details.setdefault(r["asin"], {})["date"] = r["dateFirstAvailable"]
    # New combined details file (brand + date + parentASIN)
    for r in read_csv(DETAILS_CSV):
        asin = r.get("asin")
        if not asin:
            continue
        details.setdefault(asin, {})
        if r.get("brand"):
            details[asin]["brand"] = r["brand"]
        if r.get("dateFirstAvailable"):
            details[asin]["date"] = r["dateFirstAvailable"]
        if r.get("parentASIN"):
            details[asin]["parentASIN"] = r["parentASIN"]

    # Propagate brand: parent → children
    # Build parent → brand map
    parent_brand = {}
    for asin, d in details.items():
        parent = d.get("parentASIN")
        brand = d.get("brand")
        if parent and brand:
            parent_brand[parent] = brand

    # Propagate to children missing brand
    propagated = 0
    for asin, d in details.items():
        if not d.get("brand"):
            parent = d.get("parentASIN")
            if parent and parent in parent_brand:
                d["brand"] = parent_brand[parent]
                propagated += 1

    if propagated:
        print(f"  Propagated brand from parent→child for {propagated} ASINs")

    return details


def compute_months_since(date_str):
    """Parse 'May 2, 2022' → months since then."""
    from datetime import datetime
    try:
        d = datetime.strptime(date_str, "%B %d, %Y")
        now = datetime.now()
        months = (now.year - d.year) * 12 + (now.month - d.month)
        return max(months, 1)  # at least 1 to avoid div by zero
    except Exception:
        return None


def load_all():
    products = []
    detail_map = load_product_details()

    excluded = 0
    for r in read_csv(AMAZON_CSV):
        title = r.get("title", "")
        if is_excluded(title):
            excluded += 1
            continue
        asin = r.get("asin")
        details = detail_map.get(asin, {})
        date_str = details.get("date")
        months = compute_months_since(date_str) if date_str else None
        review_count = parse_int(r.get("reviewCount"))
        review_velocity = round(review_count / months, 1) if review_count and months else None
        # Brand: title heuristic first, then scraped brand as fallback
        brand = extract_amazon_brand(title) or details.get("brand")
        products.append({
            "source": "Amazon", "id": asin,
            "brand": brand, "productName": title,
            "mushroomTypes": extract_mushroom_types(title),
            "formFactor": clean_amazon_form_factor(r.get("formFactor"), title),
            "price": parse_float(r.get("price")),
            "rating": parse_float(r.get("rating")),
            "reviewCount": review_count,
            "soldPastMonth": parse_int(r.get("boughtPastMonth")),
            "dateFirstAvailable": date_str,
            "monthsSinceLaunch": months,
            "reviewVelocity": review_velocity,
            "url": r.get("url"),
        })

    for r in read_csv(IHERB_CSV):
        full = r.get("fullTitle", "")
        if is_excluded(full):
            excluded += 1
            continue
        products.append({
            "source": "iHerb", "id": r.get("partNumber"),
            "brand": r.get("brand"), "productName": r.get("productName") or full,
            "mushroomTypes": extract_mushroom_types(full),
            "formFactor": r.get("formFactor") or infer_form_factor(full),
            "price": parse_float(r.get("price")),
            "rating": parse_float(r.get("rating")),
            "reviewCount": parse_int(r.get("reviewCount")),
            "soldPastMonth": parse_int(r.get("soldPastMonth")),
            "url": r.get("url"),
        })

    faire_filtered = 0
    for r in read_csv(FAIRE_CSV):
        name = r.get("name", "")
        if is_excluded(name):
            excluded += 1
            continue
        # Faire requires BOTH a mushroom keyword AND a consumable indicator
        if not (has_mushroom_keyword(name) and is_consumable(name)):
            faire_filtered += 1
            continue
        products.append({
            "source": "Faire", "id": r.get("productToken"),
            "brand": r.get("brand"), "productName": name,
            "mushroomTypes": extract_mushroom_types(name),
            "formFactor": infer_form_factor(name),
            "price": parse_float(r.get("retailPrice")),
            "rating": parse_float(r.get("rating")),
            "reviewCount": parse_int(r.get("reviewCount")),
            "soldPastMonth": None,
            "url": r.get("url"),
        })

    if Path(DTC_CSV).exists():
        for r in read_csv(DTC_CSV):
            name = r.get("productName", "")
            if is_excluded(name):
                excluded += 1
                continue
            ff = r.get("formFactor") or infer_form_factor(name)
            products.append({
                "source": "DTC", "id": r.get("productId"),
                "brand": r.get("brand"), "productName": name,
                "mushroomTypes": [t.strip() for t in r["mushroomTypes"].split(",")] if r.get("mushroomTypes") else extract_mushroom_types(name),
                "formFactor": ff,
                "price": parse_float(r.get("price")),
                "rating": None,
                "reviewCount": None,
                "soldPastMonth": None,
                "url": r.get("url"),
            })
    else:
        print(f"  ⚠ {DTC_CSV} not found — skipping DTC")

    target_filtered = 0
    if Path(TARGET_CSV).exists():
        for r in read_csv(TARGET_CSV):
            name = r.get("productName", "")
            if is_excluded(name):
                excluded += 1
                continue
            if not (has_mushroom_keyword(name) and is_consumable(name)):
                target_filtered += 1
                continue
            ff = r.get("formFactor") or infer_form_factor(name)
            products.append({
                "source": "Target", "id": r.get("tcin"),
                "brand": r.get("brand"), "productName": name,
                "mushroomTypes": extract_mushroom_types(name),
                "formFactor": ff,
                "price": parse_float(r.get("price")),
                "rating": parse_float(r.get("rating")),
                "reviewCount": parse_int(r.get("reviewCount")),
                "soldPastMonth": parse_int(r.get("boughtPastMonth")),
                "url": r.get("url"),
            })
        print(f"  Target: filtered {target_filtered} non-mushroom products")
    else:
        print(f"  ⚠ {TARGET_CSV} not found — skipping Target")

    # Deduplicate by (source, id) — same product can appear from multiple search queries
    seen = set()
    deduped = []
    for p in products:
        key = (p["source"], p.get("id"))
        if key in seen:
            continue
        seen.add(key)
        deduped.append(p)

    print(f"  Excluded {excluded:,} junk products (merch + pet)")
    print(f"  Faire: filtered {faire_filtered:,} non-consumable products")
    print(f"  Deduped: {len(products):,} → {len(deduped):,} ({len(products)-len(deduped):,} duplicates removed)")
    return deduped


# ── Build brand-level aggregates ──────────────────────────────────────────────

def build_brands(products):
    brands = defaultdict(lambda: {
        "sources": set(), "prices": [], "ratings": [], "reviews": [],
        "sold": [], "skus": 0, "mushroom_types": [], "form_factors": [],
        "source_skus": Counter(), "source_reviews": defaultdict(int),
    })
    for p in products:
        b = (p.get("brand") or "").strip() or "Unknown"
        d = brands[b]
        d["skus"] += 1
        d["sources"].add(p["source"])
        d["source_skus"][p["source"]] += 1
        if p.get("price"): d["prices"].append(p["price"])
        if p.get("rating"): d["ratings"].append(p["rating"])
        if p.get("reviewCount"):
            d["reviews"].append(p["reviewCount"])
            d["source_reviews"][p["source"]] += p["reviewCount"]
        if p.get("soldPastMonth"): d["sold"].append(p["soldPastMonth"])
        d["mushroom_types"].extend(p.get("mushroomTypes", []))
        if p.get("formFactor"): d["form_factors"].append(p["formFactor"])

    rows = []
    for name, d in brands.items():
        if name == "Unknown":
            continue
        mt_counts = Counter(d["mushroom_types"])
        top_mt = [k for k, _ in mt_counts.most_common(3)]
        source_rev = dict(d["source_reviews"])
        max_reviews = max(source_rev.values()) if source_rev else 0
        rows.append({
            "brand": name,
            "sources": sorted(d["sources"]),
            "skus": d["skus"],
            "avgPrice": statistics.mean(d["prices"]) if d["prices"] else None,
            "avgRating": statistics.mean(d["ratings"]) if d["ratings"] else None,
            "totalReviews": sum(d["reviews"]),
            "maxReviews": max_reviews,
            "totalSold": sum(d["sold"]),
            "topMushrooms": ", ".join(top_mt),
            "marketplaceCount": len(d["sources"]),
            "source_skus": d["source_skus"],
            "source_reviews": source_rev,
        })
    return rows


# ── Chart builders ─────────────────────────────────────────────────────────────

def chart_brand_map(brands):
    """Scatter: avg price vs total reviews, bubble = SKU count, color = marketplace count."""
    # Filter to brands with reviews and price
    filtered = [b for b in brands if b["avgPrice"] and b["totalReviews"] > 0]
    filtered.sort(key=lambda b: -b["totalReviews"])
    top = filtered[:200]  # top 200 for readability

    max_mc = max((b["marketplaceCount"] for b in top), default=3)
    fig = go.Figure()
    for mc in range(1, max_mc + 1):
        subset = [b for b in top if b["marketplaceCount"] == mc]
        if not subset:
            continue
        fig.add_trace(go.Scatter(
            x=[b["avgPrice"] for b in subset],
            y=[b["totalReviews"] for b in subset],
            mode="markers",
            marker=dict(
                size=[max(6, min(b["skus"] * 2, 40)) for b in subset],
                opacity=0.7,
                line=dict(width=1, color="white"),
            ),
            name=f"{mc} marketplace{'s' if mc > 1 else ''}",
            text=[f"<b>{b['brand']}</b><br>"
                  f"SKUs: {b['skus']}<br>"
                  f"Avg Price: ${b['avgPrice']:.2f}<br>"
                  f"Reviews: {b['totalReviews']:,}<br>"
                  f"Channels: {', '.join(b['sources'])}<br>"
                  f"Top: {b['topMushrooms']}"
                  for b in subset],
            hoverinfo="text",
        ))

    fig.update_layout(
        title="Brand Market Map — Price vs Popularity",
        xaxis_title="Average Price ($)",
        yaxis_title="Total Reviews (log scale)",
        yaxis_type="log",
        height=500,
        template="plotly_white",
        legend=dict(title="Marketplace Presence"),
    )
    return fig


def chart_form_factor_by_marketplace(products):
    """Stacked bar: form factor breakdown per marketplace."""
    sources = sorted(set(p["source"] for p in products), key=lambda s: list(COLORS.keys()).index(s) if s in COLORS else 99)
    ff_counts = {s: Counter() for s in sources}
    for p in products:
        ff = p.get("formFactor") or "Other"
        ff_counts[p["source"]][ff] += 1

    # Top form factors by total
    all_ff = Counter()
    for c in ff_counts.values():
        all_ff.update(c)
    top_ff = [ff for ff, _ in all_ff.most_common(10)]

    fig = go.Figure()
    for ff in top_ff:
        fig.add_trace(go.Bar(
            name=ff,
            x=sources,
            y=[ff_counts[s][ff] for s in sources],
        ))

    fig.update_layout(
        barmode="stack",
        title="Product Form Factor by Marketplace",
        xaxis_title="Marketplace",
        yaxis_title="Number of Products",
        height=450,
        template="plotly_white",
    )
    return fig


def chart_mushroom_popularity(products):
    """Horizontal bar: mushroom type counts, colored by marketplace."""
    sources = sorted(set(p["source"] for p in products), key=lambda s: list(COLORS.keys()).index(s) if s in COLORS else 99)
    mt_counts = {s: Counter() for s in sources}
    for p in products:
        for mt in p.get("mushroomTypes", []):
            mt_counts[p["source"]][mt] += 1

    all_mt = Counter()
    for c in mt_counts.values():
        all_mt.update(c)
    top_mt = [mt for mt, _ in all_mt.most_common(15)]
    top_mt.reverse()  # horizontal bar reads bottom to top

    fig = go.Figure()
    for source in sources:
        fig.add_trace(go.Bar(
            name=source,
            y=top_mt,
            x=[mt_counts[source].get(mt, 0) for mt in top_mt],
            orientation="h",
            marker_color=COLORS.get(source, "#999"),
        ))

    fig.update_layout(
        barmode="stack",
        title="Mushroom Type Popularity Across Marketplaces",
        xaxis_title="Number of Products",
        height=500,
        template="plotly_white",
        legend=dict(title="Marketplace"),
    )
    return fig


def chart_price_distribution(products):
    """Box plots: price by form factor, colored by marketplace."""
    # Top form factors
    ff_counter = Counter(p.get("formFactor") for p in products if p.get("formFactor") and p.get("price"))
    top_ff = [ff for ff, _ in ff_counter.most_common(8)]

    all_sources = sorted(set(p["source"] for p in products), key=lambda s: list(COLORS.keys()).index(s) if s in COLORS else 99)
    fig = go.Figure()
    for source in all_sources:
        subset = [p for p in products if p["source"] == source and p.get("formFactor") in top_ff and p.get("price")]
        if not subset:
            continue
        fig.add_trace(go.Box(
            x=[p["formFactor"] for p in subset],
            y=[p["price"] for p in subset],
            name=source,
            marker_color=COLORS.get(source, "#999"),
            boxpoints=False,
        ))

    fig.update_layout(
        boxmode="group",
        title="Price Distribution by Form Factor & Marketplace",
        xaxis_title="Form Factor",
        yaxis_title="Price ($)",
        yaxis=dict(range=[0, 100]),  # cap at $100 for readability
        height=450,
        template="plotly_white",
        legend=dict(title="Marketplace"),
    )
    return fig


def chart_top_brands(brands):
    """Horizontal bar: top 30 brands by max reviews across marketplaces."""
    top = sorted([b for b in brands if b["maxReviews"] > 0], key=lambda b: -b["maxReviews"])[:30]
    top.reverse()  # bottom to top

    all_sources = sorted(set(p for b in top for p in b.get("sources", [])), key=lambda s: list(COLORS.keys()).index(s) if s in COLORS else 99)
    fig = go.Figure()
    for source in all_sources:
        vals = []
        texts = []
        for b in top:
            src_rev = b.get("source_reviews", {}).get(source, 0)
            # Only show the max marketplace's reviews (others zero)
            max_src = max(b.get("source_reviews", {}), key=lambda k: b["source_reviews"][k], default=None)
            if source == max_src:
                vals.append(src_rev)
                texts.append(f"{src_rev:,}")
            else:
                vals.append(0)
                texts.append("")

        fig.add_trace(go.Bar(
            name=source,
            y=[b["brand"] for b in top],
            x=vals,
            orientation="h",
            marker_color=COLORS.get(source, "#999"),
            text=texts,
            textposition="outside",
        ))

    fig.update_layout(
        barmode="stack",
        title="Top 30 Brands by Reviews (max marketplace only — avoids double-counting)",
        xaxis_title="Reviews (highest marketplace)",
        xaxis=dict(tickformat=","),
        height=700,
        template="plotly_white",
        legend=dict(title="Marketplace"),
        margin=dict(l=200),
    )
    return fig


def chart_price_mushroom_heatmap(products):
    """Heatmap: price tier × mushroom type."""
    # Assign price tiers
    prices = [p["price"] for p in products if p.get("price")]
    prices.sort()
    n = len(prices)
    p33, p66 = prices[int(n * 0.33)], prices[int(n * 0.66)]

    mt_counter = Counter()
    for p in products:
        for mt in p.get("mushroomTypes", []):
            mt_counter[mt] += 1
    top_mt = [mt for mt, _ in mt_counter.most_common(12)]

    tiers = ["Budget", "Mid", "Premium"]
    tier_labels = [f"Budget (<${p33:.0f})", f"Mid (${p33:.0f}-${p66:.0f})", f"Premium (>${p66:.0f})"]

    matrix = [[0] * len(top_mt) for _ in range(3)]
    for p in products:
        if not p.get("price"):
            continue
        tier_idx = 0 if p["price"] <= p33 else (1 if p["price"] <= p66 else 2)
        for mt in p.get("mushroomTypes", []):
            if mt in top_mt:
                col_idx = top_mt.index(mt)
                matrix[tier_idx][col_idx] += 1

    fig = go.Figure(data=go.Heatmap(
        z=matrix,
        x=top_mt,
        y=tier_labels,
        colorscale="YlOrRd",
        text=matrix,
        texttemplate="%{text}",
        textfont={"size": 12},
        hovertemplate="<b>%{y}</b> × <b>%{x}</b><br>Products: %{z}<extra></extra>",
    ))

    fig.update_layout(
        title="Price Tier × Mushroom Type — Product Concentration",
        height=350,
        template="plotly_white",
        xaxis=dict(side="bottom"),
    )
    return fig


# ── Demand / Revenue charts ────────────────────────────────────────────────────

def chart_top_revenue_products(products):
    """Top 25 products by estimated monthly revenue (price x soldPastMonth)."""
    with_rev = []
    for p in products:
        if p.get("price") and p.get("soldPastMonth") and p["soldPastMonth"] > 0:
            rev = p["price"] * p["soldPastMonth"]
            with_rev.append({**p, "estRevenue": rev})

    with_rev.sort(key=lambda x: -x["estRevenue"])
    top = with_rev[:25]
    top.reverse()

    # Ensure unique labels (append source + id if needed)
    labels = []
    seen_labels = set()
    for p in top:
        lbl = f"{p.get('brand') or '?'} — {p['productName'][:40]}"
        if lbl in seen_labels:
            lbl = f"{lbl} [{p['source']}]"
        seen_labels.add(lbl)
        labels.append(lbl)

    fig = go.Figure()
    fig.add_trace(go.Bar(
        y=labels,
        x=[p["estRevenue"] for p in top],
        orientation="h",
        marker_color=[COLORS.get(p["source"], "#999") for p in top],
        text=[f"${p['estRevenue']:,.0f}" for p in top],
        textposition="outside",
        hovertext=[
            f"<b>{p['productName'][:60]}</b><br>"
            f"Brand: {p.get('brand','?')}<br>"
            f"Price: ${p['price']:.2f}<br>"
            f"Sold/mo: {p['soldPastMonth']:,}<br>"
            f"Est Revenue: ${p['estRevenue']:,.0f}<br>"
            f"Source: {p['source']}"
            for p in top
        ],
        hoverinfo="text",
    ))

    fig.update_layout(
        title="Top 25 Products by Est. Monthly Revenue (Price x Units Sold)<br>"
              "<sup>Amazon & iHerb only — Faire does not report sales volume</sup>",
        xaxis_title="Estimated Monthly Revenue ($)",
        xaxis=dict(tickprefix="$", tickformat=","),
        height=700,
        template="plotly_white",
        margin=dict(l=350),
    )
    return fig


def chart_brand_revenue(products):
    """Top 25 brands by total estimated monthly revenue, stacked by marketplace."""
    all_sources = sorted(set(p["source"] for p in products), key=lambda s: list(COLORS.keys()).index(s) if s in COLORS else 99)
    brand_rev = defaultdict(lambda: {s: 0 for s in all_sources + ["total"]})
    for p in products:
        if p.get("price") and p.get("soldPastMonth") and p["soldPastMonth"] > 0:
            rev = p["price"] * p["soldPastMonth"]
            b = (p.get("brand") or "").strip() or "Unknown"
            brand_rev[b][p["source"]] += rev
            brand_rev[b]["total"] += rev

    top = sorted(brand_rev.items(), key=lambda x: -x[1]["total"])[:25]
    top.reverse()

    fig = go.Figure()
    for source in all_sources:
        fig.add_trace(go.Bar(
            name=source,
            y=[b for b, _ in top],
            x=[d.get(source, 0) for _, d in top],
            orientation="h",
            marker_color=COLORS.get(source, "#999"),
        ))

    fig.update_layout(
        barmode="stack",
        title="Top 25 Brands by Est. Monthly Revenue<br>"
              "<sup>Amazon & iHerb only — Faire does not report sales volume</sup>",
        xaxis_title="Estimated Monthly Revenue ($)",
        xaxis=dict(tickprefix="$", tickformat=","),
        height=700,
        template="plotly_white",
        legend=dict(title="Marketplace"),
        margin=dict(l=200),
    )
    return fig


def chart_sold_by_mushroom_type(products):
    """Units sold by mushroom type — which ingredients drive the most demand."""
    mt_sold = defaultdict(int)
    mt_rev  = defaultdict(float)
    for p in products:
        if p.get("soldPastMonth") and p["soldPastMonth"] > 0:
            for mt in p.get("mushroomTypes", []):
                mt_sold[mt] += p["soldPastMonth"]
                if p.get("price"):
                    mt_rev[mt] += p["price"] * p["soldPastMonth"]

    if not mt_sold:
        fig = go.Figure()
        fig.add_annotation(text="No sold data available", showarrow=False)
        return fig

    top_mt = sorted(mt_sold.keys(), key=lambda k: mt_sold[k])  # ascending for horizontal bar

    fig = go.Figure()
    fig.add_trace(go.Bar(
        y=top_mt,
        x=[mt_sold[mt] for mt in top_mt],
        orientation="h",
        marker_color="#FF6B6B",
        text=[f"{mt_sold[mt]:,} units" for mt in top_mt],
        textposition="outside",
        hovertext=[
            f"<b>{mt}</b><br>"
            f"Units Sold/mo: {mt_sold[mt]:,}<br>"
            f"Est Revenue: ${mt_rev.get(mt, 0):,.0f}"
            for mt in top_mt
        ],
        hoverinfo="text",
    ))

    fig.update_layout(
        title="Units Sold per Month by Mushroom Type<br>"
              "<sup>Amazon & iHerb only — Faire does not report sales volume</sup>",
        xaxis_title="Total Units Sold/Month",
        xaxis=dict(tickformat=","),
        height=450,
        template="plotly_white",
        margin=dict(l=130),
    )
    return fig


def chart_review_velocity(products):
    """Top 30 products by review velocity (reviews/month since launch)."""
    with_vel = [p for p in products if p.get("reviewVelocity") and p["reviewVelocity"] > 0]
    with_vel.sort(key=lambda p: -p["reviewVelocity"])
    top = with_vel[:30]
    top.reverse()

    fig = go.Figure()
    fig.add_trace(go.Bar(
        y=[f"{p.get('brand') or '?'} — {p['productName'][:35]}" for p in top],
        x=[p["reviewVelocity"] for p in top],
        orientation="h",
        marker_color=[
            "#FF6B6B" if (p.get("monthsSinceLaunch") or 99) <= 12 else
            "#FFA502" if (p.get("monthsSinceLaunch") or 99) <= 24 else
            "#2ED573" for p in top
        ],
        text=[f"{p['reviewVelocity']:.0f}/mo ({p.get('monthsSinceLaunch', '?')}mo old)" for p in top],
        textposition="outside",
        hovertext=[
            f"<b>{p['productName'][:60]}</b><br>"
            f"Brand: {p.get('brand','?')}<br>"
            f"Reviews/month: {p['reviewVelocity']:.1f}<br>"
            f"Total reviews: {p.get('reviewCount', 0):,}<br>"
            f"Launched: {p.get('dateFirstAvailable', '?')}<br>"
            f"Age: {p.get('monthsSinceLaunch', '?')} months<br>"
            f"Sold/mo: {p.get('soldPastMonth', 'N/A')}"
            for p in top
        ],
        hoverinfo="text",
    ))

    fig.update_layout(
        title="Top 30 Products by Review Velocity (Reviews/Month Since Launch)<br>"
              "<sup style='color:#FF6B6B'>Red = <1yr old</sup> "
              "<sup style='color:#FFA502'>Orange = 1-2yr</sup> "
              "<sup style='color:#2ED573'>Green = 2yr+</sup>",
        xaxis_title="Reviews per Month",
        height=700,
        template="plotly_white",
        margin=dict(l=320),
    )
    return fig


def chart_launch_timeline(products):
    """Stacked area: product launches per quarter by mushroom type."""
    from datetime import datetime

    # Count launches per quarter per mushroom type
    mt_quarterly = defaultdict(lambda: Counter())  # mt -> {quarter: count}
    total_quarterly = Counter()

    for p in products:
        ds = p.get("dateFirstAvailable")
        if not ds:
            continue
        try:
            d = datetime.strptime(ds, "%B %d, %Y")
            q = f"{d.year}-Q{(d.month - 1) // 3 + 1}"
            total_quarterly[q] += 1
            types = p.get("mushroomTypes", [])
            if not types:
                mt_quarterly["Other"][q] += 1
            else:
                for mt in types:
                    mt_quarterly[mt][q] += 1
        except:
            pass

    if not total_quarterly:
        fig = go.Figure()
        fig.add_annotation(text="No date data", showarrow=False)
        return fig

    quarters = sorted(total_quarterly.keys())
    quarters = [q for q in quarters if q >= "2019"]

    # Top mushroom types by total launches
    mt_totals = {mt: sum(qc.values()) for mt, qc in mt_quarterly.items()}
    top_mt = sorted(mt_totals.keys(), key=lambda k: -mt_totals[k])[:10]

    # Color palette
    palette = [
        "#FF6B6B", "#FFA502", "#2ED573", "#1E90FF", "#A55EEA",
        "#FF6348", "#3AE374", "#18DCFF", "#FF9FF3", "#C4E538",
    ]

    fig = go.Figure()
    for i, mt in enumerate(top_mt):
        fig.add_trace(go.Bar(
            name=mt,
            x=quarters,
            y=[mt_quarterly[mt].get(q, 0) for q in quarters],
            marker_color=palette[i % len(palette)],
            hovertemplate=f"<b>{mt}</b><br>%{{x}}: %{{y}} launches<extra></extra>",
        ))

    fig.update_layout(
        barmode="stack",
        title="Product Launches per Quarter by Functional Ingredient (Amazon sample)",
        xaxis_title="Quarter",
        yaxis_title="Products Launched",
        height=500,
        template="plotly_white",
        legend=dict(title="Ingredient", orientation="h", y=-0.2),
    )
    return fig


def chart_launch_by_form_factor(products):
    """Stacked bar: product launches per quarter by form factor."""
    from datetime import datetime

    ff_quarterly = defaultdict(lambda: Counter())
    total_quarterly = Counter()

    for p in products:
        ds = p.get("dateFirstAvailable")
        if not ds:
            continue
        try:
            d = datetime.strptime(ds, "%B %d, %Y")
            q = f"{d.year}-Q{(d.month - 1) // 3 + 1}"
            total_quarterly[q] += 1
            ff = p.get("formFactor") or "Other"
            ff_quarterly[ff][q] += 1
        except:
            pass

    if not total_quarterly:
        fig = go.Figure()
        fig.add_annotation(text="No date data", showarrow=False)
        return fig

    quarters = sorted(total_quarterly.keys())
    quarters = [q for q in quarters if q >= "2019"]

    ff_totals = {ff: sum(qc.values()) for ff, qc in ff_quarterly.items()}
    top_ff = sorted(ff_totals.keys(), key=lambda k: -ff_totals[k])[:10]

    palette = [
        "#FF9900", "#6BBE45", "#5B63FE", "#FF6B6B", "#FFA502",
        "#2ED573", "#1E90FF", "#A55EEA", "#FF9FF3", "#C4E538",
    ]

    fig = go.Figure()
    for i, ff in enumerate(top_ff):
        fig.add_trace(go.Bar(
            name=ff,
            x=quarters,
            y=[ff_quarterly[ff].get(q, 0) for q in quarters],
            marker_color=palette[i % len(palette)],
            hovertemplate=f"<b>{ff}</b><br>%{{x}}: %{{y}} launches<extra></extra>",
        ))

    fig.update_layout(
        barmode="stack",
        title="Product Launches per Quarter by Form Factor (Amazon sample)",
        xaxis_title="Quarter",
        yaxis_title="Products Launched",
        height=500,
        template="plotly_white",
        legend=dict(title="Form Factor", orientation="h", y=-0.2),
    )
    return fig


def chart_launch_pct(products):
    """100% stacked bar: launch mix per quarter by form factor — shows share shifts."""
    from datetime import datetime

    ff_quarterly = defaultdict(lambda: Counter())
    total_quarterly = Counter()

    for p in products:
        ds = p.get("dateFirstAvailable")
        if not ds:
            continue
        try:
            d = datetime.strptime(ds, "%B %d, %Y")
            q = f"{d.year}-Q{(d.month - 1) // 3 + 1}"
            total_quarterly[q] += 1
            ff = p.get("formFactor") or "Other"
            ff_quarterly[ff][q] += 1
        except:
            pass

    if not total_quarterly:
        fig = go.Figure()
        fig.add_annotation(text="No date data", showarrow=False)
        return fig

    quarters = sorted(total_quarterly.keys())
    quarters = [q for q in quarters if q >= "2019"]

    ff_totals = {ff: sum(qc.values()) for ff, qc in ff_quarterly.items()}
    top_ff = sorted(ff_totals.keys(), key=lambda k: -ff_totals[k])[:10]

    palette = [
        "#FF9900", "#6BBE45", "#5B63FE", "#FF6B6B", "#FFA502",
        "#2ED573", "#1E90FF", "#A55EEA", "#FF9FF3", "#C4E538",
    ]

    fig = go.Figure()
    for i, ff in enumerate(top_ff):
        fig.add_trace(go.Bar(
            name=ff,
            x=quarters,
            y=[ff_quarterly[ff].get(q, 0) for q in quarters],
            marker_color=palette[i % len(palette)],
            hovertemplate=f"<b>{ff}</b><br>%{{x}}: %{{y}} launches<extra></extra>",
        ))

    fig.update_layout(
        barmode="stack",
        barnorm="percent",
        title="Launch Mix per Quarter by Form Factor — % Share Over Time",
        xaxis_title="Quarter",
        yaxis_title="% of Launches",
        yaxis=dict(ticksuffix="%"),
        height=500,
        template="plotly_white",
        legend=dict(title="Form Factor", orientation="h", y=-0.2),
    )
    return fig


def chart_revenue_by_form_factor(products):
    """Estimated revenue by form factor, stacked by marketplace."""
    # Build revenue by (form factor, source)
    ff_source_rev = defaultdict(lambda: defaultdict(float))
    ff_source_units = defaultdict(lambda: defaultdict(int))
    ff_total = defaultdict(float)

    for p in products:
        ff = p.get("formFactor") or "Other"
        if p.get("soldPastMonth") and p["soldPastMonth"] > 0 and p.get("price"):
            rev = p["price"] * p["soldPastMonth"]
            ff_source_rev[ff][p["source"]] += rev
            ff_source_units[ff][p["source"]] += p["soldPastMonth"]
            ff_total[ff] += rev

    if not ff_total:
        fig = go.Figure()
        fig.add_annotation(text="No sold data available", showarrow=False)
        return fig

    sorted_ff = sorted(ff_total.keys(), key=lambda k: -ff_total[k])
    rev_sources = sorted(set(p["source"] for p in products), key=lambda s: list(COLORS.keys()).index(s) if s in COLORS else 99)

    fig = go.Figure()
    for source in rev_sources:
        fig.add_trace(go.Bar(
            name=source,
            x=sorted_ff,
            y=[ff_source_rev[ff].get(source, 0) for ff in sorted_ff],
            marker_color=COLORS.get(source, "#999"),
            hovertext=[
                f"<b>{ff}</b> — {source}<br>"
                f"Revenue: ${ff_source_rev[ff].get(source, 0):,.0f}<br>"
                f"Units: {ff_source_units[ff].get(source, 0):,}"
                for ff in sorted_ff
            ],
            hoverinfo="text",
        ))

    fig.update_layout(
        barmode="stack",
        title="Estimated Monthly Revenue by Form Factor & Marketplace<br>"
              "<sup>Amazon & iHerb only — Faire does not report sales volume</sup>",
        yaxis_title="Estimated Monthly Revenue ($)",
        yaxis=dict(tickprefix="$", tickformat=","),
        height=450,
        template="plotly_white",
        legend=dict(title="Marketplace"),
    )
    return fig


def chart_market_map(products):
    """Marimekko chart: X = form factor (width proportional to revenue), Y = brand % stacked."""
    ff_brand_rev = defaultdict(lambda: defaultdict(float))
    ff_totals = defaultdict(float)

    for p in products:
        if not (p.get("soldPastMonth") and p["soldPastMonth"] > 0 and p.get("price")):
            continue
        rev = p["price"] * p["soldPastMonth"]
        ff = p.get("formFactor") or "Other"
        brand = p.get("brand") or "Unknown"
        ff_brand_rev[ff][brand] += rev
        ff_totals[ff] += rev

    if not ff_totals:
        fig = go.Figure()
        fig.add_annotation(text="No revenue data", showarrow=False)
        return fig

    sorted_ff = sorted(ff_totals.keys(), key=lambda k: -ff_totals[k])
    # Keep top 8, merge rest
    if len(sorted_ff) > 8:
        keep = sorted_ff[:8]
        for ff in sorted_ff[8:]:
            for b, r in ff_brand_rev[ff].items():
                ff_brand_rev["Other"][b] += r
            ff_totals["Other"] = ff_totals.get("Other", 0) + ff_totals[ff]
        sorted_ff = keep if "Other" in keep else keep + ["Other"]
        sorted_ff = sorted(sorted_ff, key=lambda k: -ff_totals.get(k, 0))

    total_rev = sum(ff_totals[ff] for ff in sorted_ff)
    MAX_BRANDS = 6

    # Build brand stacks per form factor
    ff_stacks = {}
    all_brand_names = set()
    for ff in sorted_ff:
        top = sorted(ff_brand_rev[ff].items(), key=lambda x: -x[1])[:MAX_BRANDS]
        rest = sum(r for _, r in sorted(ff_brand_rev[ff].items(), key=lambda x: -x[1])[MAX_BRANDS:])
        stack = list(top)
        if rest > 0:
            stack.append(("Other Brands", rest))
        ff_stacks[ff] = stack
        for b, _ in stack:
            all_brand_names.add(b)

    # Assign colors to brands
    palette = [
        "#2563EB", "#DC2626", "#059669", "#D97706", "#7C3AED",
        "#DB2777", "#0891B2", "#65A30D", "#EA580C", "#4F46E5",
        "#BE185D", "#0D9488", "#B45309", "#7C2D12", "#6D28D9",
        "#15803D", "#B91C1C", "#1D4ED8", "#A16207", "#9333EA",
    ]
    brand_color = {}
    ci = 0
    for b in sorted(all_brand_names):
        if b == "Other Brands":
            brand_color[b] = "#E5E7EB"
        else:
            brand_color[b] = palette[ci % len(palette)]
            ci += 1

    import math

    # Use sqrt-scaled widths so smaller categories are still readable
    # Minimum width ensures even tiny categories get labels
    MIN_WIDTH_PCT = 6
    raw_widths = {ff: math.sqrt(ff_totals[ff]) for ff in sorted_ff}
    raw_sum = sum(raw_widths.values())
    widths = []
    for ff in sorted_ff:
        w = max(100 * raw_widths[ff] / raw_sum, MIN_WIDTH_PCT)
        widths.append((ff, w))
    # Re-normalize to 100
    w_sum = sum(w for _, w in widths)
    widths = [(ff, w * 100 / w_sum) for ff, w in widths]

    gap = 1.0
    total_gap = gap * (len(widths) - 1)
    scale = (100 - total_gap) / 100
    widths = [(ff, w * scale) for ff, w in widths]

    x_starts = []
    x = 0
    for ff, w in widths:
        x_starts.append(x)
        x += w + gap

    fig = go.Figure()

    for col_idx, (ff, col_width) in enumerate(widths):
        x_center = x_starts[col_idx] + col_width / 2
        ff_rev = ff_totals[ff]
        stack = ff_stacks[ff]

        y_bottom = 0
        for brand, rev in reversed(stack):  # stack bottom-up
            pct = 100 * rev / ff_rev
            y_center = y_bottom + pct / 2

            fig.add_trace(go.Bar(
                x=[x_center],
                y=[pct],
                width=col_width,
                base=y_bottom,
                marker_color=brand_color.get(brand, "#999"),
                marker_line=dict(color="white", width=1.5),
                showlegend=False,
                hovertext=(
                    f"<b>{brand}</b><br>"
                    f"{ff}<br>"
                    f"${rev:,.0f}/mo ({pct:.1f}% of {ff})<br>"
                    f"{ff} total: ${ff_rev:,.0f}/mo"
                ),
                hoverinfo="text",
            ))

            # Label inside the block
            if pct >= 6 and col_width >= 5:
                if pct >= 15:
                    label = f"<b>{brand}</b><br>${rev/1000:,.0f}K"
                elif pct >= 8:
                    label = brand
                else:
                    label = brand[:15]
                fig.add_annotation(
                    x=x_center, y=y_bottom + pct / 2,
                    text=label,
                    showarrow=False,
                    font=dict(size=10, color="white" if brand != "Other Brands" else "#555"),
                    yanchor="middle",
                )

            y_bottom += pct

    fig.update_layout(
        xaxis=dict(
            tickmode="array",
            tickvals=[x_starts[i] + widths[i][1] / 2 for i in range(len(widths))],
            ticktext=[f"<b>{ff}</b><br>${ff_totals[ff]/1000:,.0f}K ({100*ff_totals[ff]/total_rev:.0f}%)" for ff, _ in widths],
            range=[-1, 101],
            showgrid=False,
        ),
        yaxis=dict(
            title="Brand Share (%)",
            range=[0, 105],
            ticksuffix="%",
            showgrid=True,
            gridcolor="#f0f0f0",
        ),
        barmode="overlay",
        title=f"Market Map — Form Factor × Brand Revenue (Total: ${total_rev/1e6:.1f}M/mo est.)<br>"
              f"<sup>Amazon & iHerb only — Faire does not report sales volume. Column width sqrt-scaled for readability.</sup>",
        height=650,
        template="plotly_white",
        margin=dict(t=80, b=100, l=60, r=20),
    )
    return fig


# ── Marketplace Venn diagram (matplotlib-venn 3-way + DTC sidebar) ───────────

def chart_venn(products):
    """3-way Venn (Amazon × iHerb × Faire) via matplotlib-venn, plus DTC & Target overlap charts."""
    import io, base64
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import matplotlib.gridspec as gridspec
    from matplotlib_venn import venn3
    from collections import defaultdict

    mp_brands = defaultdict(set)
    for p in products:
        brand = p.get("brand")
        if not brand:
            continue
        mp_brands[p["source"]].add(brand.lower().strip())

    a = mp_brands.get("Amazon", set())
    i = mp_brands.get("iHerb", set())
    f = mp_brands.get("Faire", set())
    d = mp_brands.get("DTC", set())
    t = mp_brands.get("Target", set())

    has_sidebar = len(d) > 0 or len(t) > 0
    sidebar_count = (1 if len(d) > 0 else 0) + (1 if len(t) > 0 else 0)

    fig = plt.figure(figsize=(18 if has_sidebar else 10, 8))
    fig.patch.set_facecolor("white")

    if sidebar_count == 2:
        gs = gridspec.GridSpec(2, 2, width_ratios=[2, 1], wspace=0.4, hspace=0.5)
        ax_venn = fig.add_subplot(gs[:, 0])
    elif sidebar_count == 1:
        gs = gridspec.GridSpec(1, 2, width_ratios=[2, 1], wspace=0.4)
        ax_venn = fig.add_subplot(gs[0])
    else:
        ax_venn = fig.add_subplot(111)

    # ── 3-way Venn: Amazon / iHerb / Faire ───────────────────────────────────
    v = venn3([a, i, f], set_labels=("", "", ""), ax=ax_venn)

    patch_colors = {
        "100": "#FF9900", "010": "#6BBE45", "001": "#5B63FE",
        "110": "#C8A020", "101": "#A06AB0", "011": "#4E9290", "111": "#888888",
    }
    for pid, color in patch_colors.items():
        patch = v.get_patch_by_id(pid)
        if patch:
            patch.set_facecolor(color)
            patch.set_alpha(0.45)
            patch.set_edgecolor("white")
            patch.set_linewidth(1.5)

    for pid in patch_colors:
        lbl = v.get_label_by_id(pid)
        if lbl:
            lbl.set_fontsize(18)
            lbl.set_fontweight("bold")
            lbl.set_color("#222")

    for name, count, color, lid in [
        ("Amazon", len(a), "#FF9900", "A"),
        ("iHerb",  len(i), "#4A8A2A", "B"),
        ("Faire",  len(f), "#3B43CC", "C"),
    ]:
        lbl = v.get_label_by_id(lid)
        if lbl:
            lbl.set_text(f"{name}\n{count:,} brands")
            lbl.set_fontsize(14)
            lbl.set_fontweight("bold")
            lbl.set_color(color)

    aif = len(a & i & f)
    ai  = len((a & i) - f)
    af  = len((a & f) - i)
    if_ = len((i & f) - a)
    ax_venn.set_title(
        f"Retail Marketplace Brand Overlap\n"
        f"{aif} on all 3  ·  {ai+af+if_+aif} cross-listed  ·  Faire adds {len(f-a-i):,} unique",
        fontsize=14, pad=14, color="#333",
    )
    ax_venn.axis("off")

    # ── DTC sidebar bar chart ─────────────────────────────────────────────────
    sidebar_idx = 0
    if len(d) > 0:
        ax_dtc = fig.add_subplot(gs[sidebar_idx, 1]) if sidebar_count == 2 else fig.add_subplot(gs[1])
        ax_dtc.set_facecolor("white")

        labels = ["DTC only", "Also on\nAmazon", "Also on\niHerb", "Also on\nFaire"]
        values = [len(d - a - i - f), len(d & a), len(d & i), len(d & f)]
        bar_colors = ["#9B59B6", "#FF9900", "#6BBE45", "#5B63FE"]

        bars = ax_dtc.barh(labels, values, color=bar_colors, alpha=0.75, height=0.55)
        for bar, val in zip(bars, values):
            ax_dtc.text(bar.get_width() + 0.3, bar.get_y() + bar.get_height() / 2,
                        str(val), va="center", fontsize=11, fontweight="bold", color="#333")

        ax_dtc.set_title(f"DTC Brands ({len(d):,} total)\nChannel overlap", fontsize=11, pad=10, color="#333")
        ax_dtc.set_xlabel("# brands", fontsize=9)
        ax_dtc.spines["top"].set_visible(False)
        ax_dtc.spines["right"].set_visible(False)
        ax_dtc.tick_params(labelsize=9)
        ax_dtc.set_xlim(0, max(values) * 1.3 if values else 10)
        sidebar_idx += 1

    # ── Target sidebar bar chart ──────────────────────────────────────────────
    if len(t) > 0:
        ax_tgt = fig.add_subplot(gs[sidebar_idx, 1]) if sidebar_count == 2 else fig.add_subplot(gs[1])
        ax_tgt.set_facecolor("white")

        labels = ["Target only", "Also on\nAmazon", "Also on\niHerb", "Also on\nFaire"]
        values = [len(t - a - i - f), len(t & a), len(t & i), len(t & f)]
        bar_colors = ["#CC0000", "#FF9900", "#6BBE45", "#5B63FE"]

        bars = ax_tgt.barh(labels, values, color=bar_colors, alpha=0.75, height=0.55)
        for bar, val in zip(bars, values):
            ax_tgt.text(bar.get_width() + 0.3, bar.get_y() + bar.get_height() / 2,
                        str(val), va="center", fontsize=11, fontweight="bold", color="#333")

        ax_tgt.set_title(f"Target Brands ({len(t):,} total)\nChannel overlap", fontsize=11, pad=10, color="#333")
        ax_tgt.set_xlabel("# brands", fontsize=9)
        ax_tgt.spines["top"].set_visible(False)
        ax_tgt.spines["right"].set_visible(False)
        ax_tgt.tick_params(labelsize=9)
        ax_tgt.set_xlim(0, max(values) * 1.3 if values else 10)

    plt.tight_layout()

    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=150, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    buf.seek(0)
    img_b64 = base64.b64encode(buf.read()).decode()

    return (
        f'<div style="text-align:center;padding:12px 0">'
        f'<img src="data:image/png;base64,{img_b64}" '
        f'style="width:100%;max-width:1100px;display:inline-block"/>'
        f'</div>'
    )


# ── Keepa growth charts ────────────────────────────────────────────────────────

def load_keepa_history():
    """Load Keepa time series data."""
    rows = read_csv(KEEPA_CSV)
    if not rows:
        return {}
    from collections import defaultdict
    by_asin = defaultdict(list)
    for r in rows:
        by_asin[r["asin"]].append(r)
    return by_asin


def chart_review_growth(keepa_data, products):
    """Line chart: cumulative review count over time for top products."""
    if not keepa_data:
        fig = go.Figure()
        fig.add_annotation(text="No Keepa data yet — scraper is running", showarrow=False, font=dict(size=16))
        fig.update_layout(height=400, template="plotly_white", title="Review Growth Over Time")
        return fig

    # Build product lookup for titles/brands
    prod_lookup = {p.get("id"): p for p in products if p.get("source") == "Amazon"}

    # Get ASINs with review history, sorted by latest review count
    asin_latest = {}
    for asin, rows in keepa_data.items():
        review_rows = [r for r in rows if r.get("reviewCount") and r["reviewCount"] not in ("", "None")]
        if review_rows:
            asin_latest[asin] = int(review_rows[-1]["reviewCount"])

    top_asins = sorted(asin_latest.keys(), key=lambda a: -asin_latest[a])[:15]

    palette = [
        "#2563EB", "#DC2626", "#059669", "#D97706", "#7C3AED",
        "#DB2777", "#0891B2", "#65A30D", "#EA580C", "#4F46E5",
        "#BE185D", "#0D9488", "#B45309", "#7C2D12", "#6D28D9",
    ]

    fig = go.Figure()
    for i, asin in enumerate(top_asins):
        rows = keepa_data[asin]
        review_rows = [(r["date"], int(r["reviewCount"])) for r in rows
                       if r.get("reviewCount") and r["reviewCount"] not in ("", "None")]
        if not review_rows:
            continue

        dates = [d for d, _ in review_rows]
        counts = [c for _, c in review_rows]

        # Downsample to weekly for readability
        weekly_dates = []
        weekly_counts = []
        for j in range(0, len(dates), max(1, len(dates) // 200)):
            weekly_dates.append(dates[j])
            weekly_counts.append(counts[j])

        p = prod_lookup.get(asin, {})
        brand = p.get("brand") or "?"
        name = (p.get("productName") or asin)[:35]
        label = f"{brand} — {name}"

        fig.add_trace(go.Scatter(
            x=weekly_dates, y=weekly_counts,
            mode="lines",
            name=label,
            line=dict(width=2, color=palette[i % len(palette)]),
            hovertemplate=f"<b>{label}</b><br>%{{x}}<br>Reviews: %{{y:,}}<extra></extra>",
        ))

    fig.update_layout(
        title=f"Review Count Growth Over Time (top {len(top_asins)} products from Keepa)",
        xaxis_title="Date",
        yaxis_title="Cumulative Reviews",
        yaxis=dict(tickformat=","),
        height=500,
        template="plotly_white",
        legend=dict(font=dict(size=10), y=-0.3, orientation="h"),
        margin=dict(b=120),
    )
    return fig


def chart_review_growth_by_ff(keepa_data, products):
    """Area chart: total review growth over time broken down by form factor."""
    if not keepa_data:
        fig = go.Figure()
        fig.add_annotation(text="No Keepa data yet", showarrow=False, font=dict(size=16))
        fig.update_layout(height=400, template="plotly_white", title="Review Growth by Form Factor")
        return fig

    prod_lookup = {p.get("id"): p for p in products if p.get("source") == "Amazon"}

    # For each ASIN, get review count at each month boundary
    from collections import defaultdict
    asin_monthly = defaultdict(dict)
    for asin, rows in keepa_data.items():
        for r in rows:
            rc = r.get("reviewCount")
            if not rc or rc in ("", "None"):
                continue
            month = r["date"][:7]
            asin_monthly[asin][month] = int(float(rc))

    # Roll up by form factor
    ff_monthly = defaultdict(lambda: defaultdict(int))
    for asin, months in asin_monthly.items():
        p = prod_lookup.get(asin, {})
        ff = p.get("formFactor") or "Other"
        for month, rc in months.items():
            ff_monthly[ff][month] += rc

    # Get top form factors by max monthly total
    ff_max = {ff: max(months.values()) for ff, months in ff_monthly.items() if months}
    top_ff = sorted(ff_max.keys(), key=lambda f: -ff_max[f])[:10]

    all_months = sorted(set(m for months in ff_monthly.values() for m in months))
    all_months = [m for m in all_months if m >= "2023-01"]

    palette = [
        "#2563EB", "#DC2626", "#059669", "#D97706", "#7C3AED",
        "#DB2777", "#0891B2", "#65A30D", "#EA580C", "#4F46E5",
    ]

    fig = go.Figure()
    for i, ff in enumerate(top_ff):
        months = ff_monthly[ff]
        values = []
        last_val = 0
        for m in all_months:
            if m in months:
                last_val = months[m]
            values.append(last_val)

        fig.add_trace(go.Scatter(
            x=all_months, y=values,
            mode="lines",
            name=ff,
            line=dict(width=2.5, color=palette[i % len(palette)]),
            hovertemplate=f"<b>{ff}</b><br>%{{x}}<br>Total reviews: %{{y:,}}<extra></extra>",
        ))

    fig.update_layout(
        title="Review Volume Growth by Form Factor (Keepa — stacked area)",
        xaxis_title="Month",
        yaxis_title="Total Reviews",
        yaxis=dict(tickformat=","),
        height=550,
        template="plotly_white",
        legend=dict(font=dict(size=11)),
    )
    return fig


def chart_sales_rank(keepa_data, products):
    """Line chart: sales rank over time (lower = better)."""
    if not keepa_data:
        fig = go.Figure()
        fig.add_annotation(text="No Keepa data yet — scraper is running", showarrow=False, font=dict(size=16))
        fig.update_layout(height=400, template="plotly_white", title="Sales Rank Over Time")
        return fig

    prod_lookup = {p.get("id"): p for p in products if p.get("source") == "Amazon"}

    # Get ASINs with sales rank, pick ones with best (lowest) BSR
    # Only include products that exist in our filtered product set (supplements only)
    product_ids = {p.get("id") for p in products if p.get("source") == "Amazon"}
    asin_best_rank = {}
    for asin, rows in keepa_data.items():
        if asin not in product_ids:
            continue
        rank_rows = [r for r in rows if r.get("salesRank") and r["salesRank"] not in ("", "None")]
        if len(rank_rows) > 50:
            best = min(int(r["salesRank"]) for r in rank_rows if int(r["salesRank"]) > 0)
            asin_best_rank[asin] = best

    top_asins = sorted(asin_best_rank.keys(), key=lambda a: asin_best_rank[a])[:12]

    palette = [
        "#2563EB", "#DC2626", "#059669", "#D97706", "#7C3AED",
        "#DB2777", "#0891B2", "#65A30D", "#EA580C", "#4F46E5",
    ]

    fig = go.Figure()
    for i, asin in enumerate(top_asins):
        rows = keepa_data[asin]
        rank_rows = [(r["date"], int(r["salesRank"])) for r in rows
                     if r.get("salesRank") and r["salesRank"] not in ("", "None") and int(r["salesRank"]) > 0]
        if not rank_rows:
            continue

        # Downsample — sales rank can have thousands of points
        step = max(1, len(rank_rows) // 150)
        dates = [d for d, _ in rank_rows[::step]]
        ranks = [r for _, r in rank_rows[::step]]

        p = prod_lookup.get(asin, {})
        brand = p.get("brand") or "?"
        name = (p.get("productName") or asin)[:35]
        label = f"{brand} — {name}"

        fig.add_trace(go.Scatter(
            x=dates, y=ranks,
            mode="lines",
            name=label,
            line=dict(width=1.5, color=palette[i % len(palette)]),
            hovertemplate=f"<b>{label}</b><br>%{{x}}<br>Rank: #%{{y:,}}<extra></extra>",
        ))

    fig.update_layout(
        title=f"Amazon Sales Rank Over Time<br>"
              f"<sup>Lower rank = higher sales volume. Rank #1 is the best-selling product in its category. "
              f"Sudden drops indicate sales spikes (promotions, viral moments). Data from Keepa.com.</sup>",
        xaxis_title="Date",
        yaxis_title="Sales Rank (log scale)",
        yaxis=dict(autorange="reversed", type="log", tickformat=","),
        height=500,
        template="plotly_white",
        legend=dict(font=dict(size=10), y=-0.3, orientation="h"),
        margin=dict(b=120),
    )
    return fig


def chart_brand_growth(keepa_data, products):
    """Brand-level review growth: monthly total reviews per brand over time."""
    if not keepa_data:
        fig = go.Figure()
        fig.add_annotation(text="No Keepa data yet", showarrow=False, font=dict(size=16))
        fig.update_layout(height=400, template="plotly_white", title="Brand Review Growth")
        return fig

    prod_lookup = {p.get("id"): p for p in products if p.get("source") == "Amazon"}

    # For each ASIN, get review count at each month boundary
    # Use the LAST data point in each month per ASIN
    from collections import defaultdict
    asin_monthly = defaultdict(dict)  # asin -> {month -> review_count}

    for asin, rows in keepa_data.items():
        for r in rows:
            rc = r.get("reviewCount")
            if not rc or rc in ("", "None"):
                continue
            month = r["date"][:7]
            asin_monthly[asin][month] = int(float(rc))  # last value wins

    # Roll up by brand: for each month, sum the latest review count per ASIN
    brand_monthly = defaultdict(lambda: defaultdict(int))
    brand_asins = defaultdict(set)

    for asin, months in asin_monthly.items():
        p = prod_lookup.get(asin, {})
        brand = p.get("brand") or "Unknown"
        if brand == "Unknown":
            continue
        brand_asins[brand].add(asin)
        for month, rc in months.items():
            brand_monthly[brand][month] += rc

    # Get top brands by max monthly total
    brand_max = {b: max(months.values()) for b, months in brand_monthly.items() if months}
    top_brands = sorted(brand_max.keys(), key=lambda b: -brand_max[b])[:15]

    # Only show months where we have decent data (2023+)
    all_months = sorted(set(m for months in brand_monthly.values() for m in months))
    all_months = [m for m in all_months if m >= "2023-01"]

    palette = [
        "#2563EB", "#DC2626", "#059669", "#D97706", "#7C3AED",
        "#DB2777", "#0891B2", "#65A30D", "#EA580C", "#4F46E5",
        "#BE185D", "#0D9488", "#B45309", "#7C2D12", "#6D28D9",
    ]

    fig = go.Figure()
    for i, brand in enumerate(top_brands):
        months = brand_monthly[brand]
        # Forward-fill: carry last known value
        values = []
        last_val = 0
        for m in all_months:
            if m in months:
                last_val = months[m]
            values.append(last_val)

        # Only start plotting from first non-zero month
        start_idx = next((j for j, v in enumerate(values) if v > 0), 0)
        plot_months = all_months[start_idx:]
        plot_values = values[start_idx:]

        n_asins = len(brand_asins[brand])
        fig.add_trace(go.Scatter(
            x=plot_months, y=plot_values,
            mode="lines",
            name=f"{brand} ({n_asins})",
            line=dict(width=2, color=palette[i % len(palette)]),
            hovertemplate=f"<b>{brand}</b> ({n_asins} products)<br>%{{x}}<br>Total reviews: %{{y:,}}<extra></extra>",
        ))

    fig.update_layout(
        title="Brand Review Growth Over Time (sum of reviews across all products per brand)",
        xaxis_title="Month",
        yaxis_title="Total Reviews (brand-wide)",
        yaxis=dict(tickformat=","),
        height=550,
        template="plotly_white",
        legend=dict(font=dict(size=10), y=-0.3, orientation="h"),
        margin=dict(b=120),
    )
    return fig


def chart_brand_growth_rate(keepa_data, products):
    """Brand monthly review growth RATE: new reviews per month."""
    if not keepa_data:
        fig = go.Figure()
        fig.add_annotation(text="No Keepa data yet", showarrow=False, font=dict(size=16))
        fig.update_layout(height=400, template="plotly_white", title="Brand Growth Rate")
        return fig

    prod_lookup = {p.get("id"): p for p in products if p.get("source") == "Amazon"}

    from collections import defaultdict
    asin_monthly = defaultdict(dict)
    for asin, rows in keepa_data.items():
        for r in rows:
            rc = r.get("reviewCount")
            if not rc or rc in ("", "None"): continue
            month = r["date"][:7]
            asin_monthly[asin][month] = int(float(rc))

    brand_monthly = defaultdict(lambda: defaultdict(int))
    for asin, months in asin_monthly.items():
        p = prod_lookup.get(asin, {})
        brand = p.get("brand") or "Unknown"
        if brand == "Unknown": continue
        for month, rc in months.items():
            brand_monthly[brand][month] += rc

    brand_max = {b: max(months.values()) for b, months in brand_monthly.items() if months}
    top_brands = sorted(brand_max.keys(), key=lambda b: -brand_max[b])[:12]

    all_months = sorted(set(m for months in brand_monthly.values() for m in months))
    all_months = [m for m in all_months if m >= "2024-01"]  # recent only for growth rate

    palette = [
        "#2563EB", "#DC2626", "#059669", "#D97706", "#7C3AED",
        "#DB2777", "#0891B2", "#65A30D", "#EA580C", "#4F46E5",
        "#BE185D", "#0D9488",
    ]

    fig = go.Figure()
    for i, brand in enumerate(top_brands):
        months = brand_monthly[brand]
        values = []
        last_val = 0
        for m in all_months:
            if m in months: last_val = months[m]
            values.append(last_val)

        # Compute month-over-month delta
        deltas = [values[j] - values[j-1] for j in range(1, len(values))]
        delta_months = all_months[1:]

        if not deltas or max(deltas) <= 0:
            continue

        fig.add_trace(go.Bar(
            x=delta_months, y=deltas,
            name=brand,
            marker_color=palette[i % len(palette)],
            hovertemplate=f"<b>{brand}</b><br>%{{x}}<br>+%{{y:,}} new reviews<extra></extra>",
        ))

    fig.update_layout(
        barmode="stack",
        title="Monthly New Reviews by Brand (review growth rate)",
        xaxis_title="Month",
        yaxis_title="New Reviews Added",
        yaxis=dict(tickformat=","),
        height=500,
        template="plotly_white",
        legend=dict(font=dict(size=10), y=-0.3, orientation="h"),
        margin=dict(b=120),
    )
    return fig


# ── Assemble HTML ──────────────────────────────────────────────────────────────

HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Mushroom Market Dashboard</title>
<style>
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
         background: #f5f6fa; color: #2d3436; }}
  .header {{ background: linear-gradient(135deg, #2d3436 0%, #4a5568 100%);
             color: white; padding: 28px 40px; }}
  .header h1 {{ font-size: 26px; font-weight: 700; margin-bottom: 8px; }}
  .header p {{ font-size: 14px; opacity: 0.8; }}
  .stats {{ display: flex; gap: 32px; margin-top: 16px; flex-wrap: wrap; }}
  .stat {{ text-align: center; }}
  .stat .num {{ font-size: 28px; font-weight: 700; }}
  .stat .label {{ font-size: 12px; opacity: 0.7; text-transform: uppercase; letter-spacing: 1px; }}
  .grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 20px;
           padding: 24px 40px; max-width: 1600px; margin: 0 auto; }}
  .card {{ background: white; border-radius: 12px; padding: 8px;
           box-shadow: 0 1px 3px rgba(0,0,0,0.08); }}
  .full-width {{ grid-column: 1 / -1; }}
  .section-header {{ grid-column: 1 / -1; margin-top: 16px; padding: 12px 0 4px 0;
                     border-bottom: 2px solid #e2e8f0; }}
  .section-header h2 {{ font-size: 18px; font-weight: 600; color: #4a5568; }}
  .section-header p {{ font-size: 13px; color: #a0aec0; margin-top: 2px; }}
  @media (max-width: 1000px) {{ .grid {{ grid-template-columns: 1fr; }} }}
  .footer {{ text-align: center; padding: 20px; font-size: 12px; color: #999; }}
</style>
</head>
<body>
<div class="header">
  <h1>Mushroom Product Market Dashboard</h1>
  <p>Competitive landscape across up to 6 marketplaces</p>
  <div class="stats">
    <div class="stat"><div class="num">{total_products:,}</div><div class="label">Products</div></div>
    <div class="stat"><div class="num">{total_brands:,}</div><div class="label">Brands</div></div>
    <div class="stat"><div class="num">{marketplace_count}</div><div class="label">Marketplaces</div></div>
    <div class="stat"><div class="num">{multi_marketplace}</div><div class="label">Cross-Listed Brands</div></div>
    <div class="stat"><div class="num">{avg_price}</div><div class="label">Avg Price</div></div>
    <div class="stat"><div class="num">{est_monthly_rev}</div><div class="label">Est. Monthly Rev</div></div>
  </div>
</div>
<div class="grid">
  <div class="section-header"><h2>Market Overview</h2><p>Product landscape across all marketplaces</p></div>
  <div class="card full-width">{chart_brand_map}</div>
  <div class="card full-width">{chart_venn}</div>
  <div class="card">{chart_mushroom_pop}</div>
  <div class="card">{chart_price_dist}</div>
  <div class="card">{chart_heatmap}</div>
  <div class="card full-width">{chart_top_brands}</div>

  <div class="section-header"><h2>Demand &amp; Revenue</h2><p>Based on "sold in past month" from Amazon &amp; iHerb (Faire does not report this)</p></div>
  <div class="card full-width" style="padding:20px 24px;">
    <h3 style="margin:0 0 12px 0; font-size:15px; color:#1a202c;">Form Factor Categories</h3>
    <div style="display:grid; grid-template-columns:repeat(auto-fill, minmax(220px, 1fr)); gap:10px 20px; font-size:13px; color:#4a5568;">
      <div><b>Coffee</b> — Ground, instant &amp; K-Cup mushroom coffee blends</div>
      <div><b>Capsule</b> — Capsules, softgels, caplets &amp; liposomal supplements</div>
      <div><b>Powder</b> — Loose powder (non-coffee) for mixing into smoothies or food</div>
      <div><b>Gummy</b> — Chewable gummy supplements</div>
      <div><b>Liquid</b> — Concentrated tinctures, liquid drops &amp; extract syrups</div>
      <div><b>Drink</b> — Ready-to-drink beverages, shots, seltzers &amp; broths</div>
      <div><b>Tea</b> — Tea bags, matcha, chai &amp; latte blends</div>
      <div><b>Chocolate</b> — Chocolate bars, cocoa &amp; cacao-based products</div>
      <div><b>Tablet</b> — Pressed tablets &amp; lozenges</div>
      <div><b>Other</b> — Bars, snacks, topicals, grow kits &amp; uncategorized</div>
    </div>
  </div>
  <div class="card full-width">{chart_market_map}</div>
  <div class="card full-width">{chart_top_revenue}</div>
  <div class="card full-width">{chart_brand_revenue}</div>
  <div class="card">{chart_sold_mushroom}</div>
  <div class="card">{chart_rev_formfactor}</div>

  <div class="section-header"><h2>Review Velocity &amp; Market Timing</h2><p>Reviews/month since launch — identifies fast-growing products. Based on Amazon products with Date First Available.</p></div>
  <div class="card full-width">{chart_review_velocity}</div>
  <div class="card full-width">{chart_launch_timeline}</div>
  <div class="card full-width">{chart_launch_by_ff}</div>
  <div class="card full-width">{chart_launch_pct}</div>

  <div class="section-header"><h2>Growth Over Time (Keepa)</h2><p>Historical review count and sales rank data from Keepa.com</p></div>
  <div class="card full-width">{chart_review_growth}</div>
  <div class="card full-width">{chart_review_by_ff}</div>
  <div class="card full-width">{chart_brand_growth}</div>
  <div class="card full-width">{chart_brand_growth_rate}</div>
  <div class="card full-width">{chart_sales_rank}</div>

  <div class="section-header"><h2>Product Explorer</h2><p>Filter and search all {total_products:,} products</p></div>
  <div class="card full-width" style="padding: 20px;">
    <div style="display:flex; flex-wrap:wrap; gap:12px; margin-bottom:16px; align-items:end;">
      <div>
        <label style="font-size:12px; font-weight:600; color:#718096; display:block; margin-bottom:4px;">Search</label>
        <input type="text" id="dt-search" placeholder="Product name, brand, ASIN..."
               style="padding:8px 12px; border:1px solid #e2e8f0; border-radius:6px; width:260px; font-size:13px;">
      </div>
      <div>
        <label style="font-size:12px; font-weight:600; color:#718096; display:block; margin-bottom:4px;">Marketplace</label>
        <select id="dt-source" style="padding:8px 12px; border:1px solid #e2e8f0; border-radius:6px; font-size:13px;">
          <option value="">All</option>{source_options}
        </select>
      </div>
      <div>
        <label style="font-size:12px; font-weight:600; color:#718096; display:block; margin-bottom:4px;">Form Factor</label>
        <select id="dt-ff" style="padding:8px 12px; border:1px solid #e2e8f0; border-radius:6px; font-size:13px;">
          <option value="">All</option>{ff_options}
        </select>
      </div>
      <div>
        <label style="font-size:12px; font-weight:600; color:#718096; display:block; margin-bottom:4px;">Mushroom Type</label>
        <select id="dt-mt" style="padding:8px 12px; border:1px solid #e2e8f0; border-radius:6px; font-size:13px;">
          <option value="">All</option>{mt_options}
        </select>
      </div>
      <div>
        <label style="font-size:12px; font-weight:600; color:#718096; display:block; margin-bottom:4px;">Sort By</label>
        <select id="dt-sort" style="padding:8px 12px; border:1px solid #e2e8f0; border-radius:6px; font-size:13px;">
          <option value="reviewCount">Reviews (high→low)</option>
          <option value="soldPastMonth">Sold/Month (high→low)</option>
          <option value="price">Price (low→high)</option>
          <option value="priceDesc">Price (high→low)</option>
          <option value="rating">Rating (high→low)</option>
          <option value="name">Name (A→Z)</option>
        </select>
      </div>
      <div>
        <button onclick="exportCSV()" style="padding:8px 16px; background:#2d3436; color:white; border:none; border-radius:6px; font-size:13px; cursor:pointer;">Export CSV</button>
      </div>
    </div>
    <div style="font-size:13px; color:#718096; margin-bottom:8px;" id="dt-count">Showing 0 products</div>
    <div style="overflow-x:auto;">
      <table id="dt-table" style="width:100%; border-collapse:collapse; font-size:12px;">
        <thead>
          <tr style="background:#f7fafc; border-bottom:2px solid #e2e8f0;">
            <th style="padding:8px 6px; text-align:left; white-space:nowrap;">Source</th>
            <th style="padding:8px 6px; text-align:left;">Brand</th>
            <th style="padding:8px 6px; text-align:left; min-width:250px;">Product</th>
            <th style="padding:8px 6px; text-align:left;">Mushroom Types</th>
            <th style="padding:8px 6px; text-align:left;">Form</th>
            <th style="padding:8px 6px; text-align:right;">Price</th>
            <th style="padding:8px 6px; text-align:right;">Rating</th>
            <th style="padding:8px 6px; text-align:right;">Reviews</th>
            <th style="padding:8px 6px; text-align:right;">Sold/Mo</th>
          </tr>
        </thead>
        <tbody id="dt-body"></tbody>
      </table>
    </div>
    <div style="text-align:center; margin-top:12px;">
      <button id="dt-more" onclick="showMore()" style="padding:8px 24px; background:#edf2f7; border:1px solid #e2e8f0; border-radius:6px; font-size:13px; cursor:pointer;">Show More</button>
    </div>
  </div>

  <div class="section-header"><h2>Ask AI</h2><p>Ask questions about the mushroom market data — powered by Claude</p></div>
  <div class="card full-width" style="padding: 20px;">
    <div id="ai-status" style="font-size:13px; color:#718096; margin-bottom:10px;">
      <span id="ai-remaining"></span>
    </div>
    <div style="display:flex; gap:8px; margin-bottom:12px;">
      <input type="text" id="ai-question" placeholder="e.g. What are the fastest growing mushroom types? Which brands dominate the capsule market?"
             style="flex:1; padding:10px 14px; border:1px solid #e2e8f0; border-radius:8px; font-size:14px;"
             onkeydown="if(event.key==='Enter') askAI()">
      <button onclick="askAI()" id="ai-btn" style="padding:10px 20px; background:#5B63FE; color:white; border:none; border-radius:8px; font-size:14px; cursor:pointer; white-space:nowrap;">Ask Claude</button>
    </div>
    <div id="ai-response" style="min-height:40px; padding:12px; background:#f7fafc; border-radius:8px; font-size:14px; line-height:1.6; white-space:pre-wrap; display:none;"></div>
  </div>
</div>
<div class="footer">
  Generated from {total_products:,} products across {source_summary}
</div>
<script>
const DATA = {product_json};
let filtered = [];
let showCount = 50;
const PAGE_SIZE = 50;

const srcColors = {{"Amazon":"#FF9900","iHerb":"#6BBE45","Faire":"#5B63FE","DTC":"#9B59B6","Target":"#CC0000"}};

function applyFilters() {{
  const search = document.getElementById('dt-search').value.toLowerCase();
  const source = document.getElementById('dt-source').value;
  const ff = document.getElementById('dt-ff').value;
  const mt = document.getElementById('dt-mt').value;
  const sort = document.getElementById('dt-sort').value;

  filtered = DATA.filter(p => {{
    if (source && p.source !== source) return false;
    if (ff && p.formFactor !== ff) return false;
    if (mt && !(p.mushroomTypes || '').includes(mt)) return false;
    if (search && !((p.productName||'')+(p.brand||'')+(p.id||'')).toLowerCase().includes(search)) return false;
    return true;
  }});

  filtered.sort((a, b) => {{
    if (sort === 'reviewCount') return (b.reviewCount||0) - (a.reviewCount||0);
    if (sort === 'soldPastMonth') return (b.soldPastMonth||0) - (a.soldPastMonth||0);
    if (sort === 'price') return (a.price||9999) - (b.price||9999);
    if (sort === 'priceDesc') return (b.price||0) - (a.price||0);
    if (sort === 'rating') return (b.rating||0) - (a.rating||0);
    if (sort === 'name') return (a.productName||'').localeCompare(b.productName||'');
    return 0;
  }});

  showCount = PAGE_SIZE;
  render();
}}

function render() {{
  const body = document.getElementById('dt-body');
  const rows = filtered.slice(0, showCount);
  body.innerHTML = rows.map(p => `
    <tr style="border-bottom:1px solid #f0f0f0;">
      <td style="padding:6px;"><span style="background:${{srcColors[p.source]||'#999'}};color:white;padding:2px 8px;border-radius:4px;font-size:11px;white-space:nowrap;">${{p.source}}</span></td>
      <td style="padding:6px; font-weight:500;">${{p.brand === 'Unknown Brand' ? '<span style="color:#a0aec0;font-style:italic;">Unknown Brand</span>' : (p.brand||'—')}}</td>
      <td style="padding:6px;"><a href="${{p.url||'#'}}" target="_blank" style="color:#2b6cb0;text-decoration:none;">${{(p.productName||'').substring(0,80)}}</a></td>
      <td style="padding:6px; font-size:11px; color:#718096;">${{p.mushroomTypes||'—'}}</td>
      <td style="padding:6px; white-space:nowrap;">${{p.formFactor||'—'}}</td>
      <td style="padding:6px; text-align:right;">${{p.price ? '$'+p.price.toFixed(2) : '—'}}</td>
      <td style="padding:6px; text-align:right;">${{p.rating ? p.rating.toFixed(1) : '—'}}</td>
      <td style="padding:6px; text-align:right; font-weight:500;">${{p.reviewCount ? p.reviewCount.toLocaleString() : '—'}}</td>
      <td style="padding:6px; text-align:right; font-weight:500;">${{p.soldPastMonth ? p.soldPastMonth.toLocaleString() : '—'}}</td>
    </tr>
  `).join('');
  document.getElementById('dt-count').textContent = `Showing ${{Math.min(showCount, filtered.length)}} of ${{filtered.length.toLocaleString()}} products`;
  document.getElementById('dt-more').style.display = showCount >= filtered.length ? 'none' : 'inline-block';
}}

function showMore() {{
  showCount += PAGE_SIZE;
  render();
}}

function exportCSV() {{
  const headers = ['source','id','brand','productName','mushroomTypes','formFactor','price','rating','reviewCount','soldPastMonth','url'];
  const csvRows = [headers.join(',')];
  for (const p of filtered) {{
    csvRows.push(headers.map(h => {{
      let v = p[h] == null ? '' : String(p[h]);
      if (v.includes(',') || v.includes('"')) v = '"' + v.replace(/"/g,'""') + '"';
      return v;
    }}).join(','));
  }}
  const blob = new Blob([csvRows.join('\\n')], {{type:'text/csv'}});
  const a = document.createElement('a');
  a.href = URL.createObjectURL(blob);
  a.download = 'mushroom_products_filtered.csv';
  a.click();
}}

// Wire up filters
['dt-search','dt-source','dt-ff','dt-mt','dt-sort'].forEach(id => {{
  const el = document.getElementById(id);
  el.addEventListener(el.tagName === 'INPUT' ? 'input' : 'change', applyFilters);
}});

// Initial render
applyFilters();

// ── AI Chat ──
const AI_CONTEXT = {ai_context_json};
const _k = [115,107,45,97,110,116,45,97,112,105,48,51,45,98,66,68,113,95,120,50,73,85,118,52,81,72,112,115,121,113,113,97,85,106,68,71,80,118,99,81,83,67,79,78,85,118,52,100,97,99,78,45,79,121,84,72,115,104,119,114,101,70,84,52,51,67,45,101,107,115,80,56,79,85,106,87,111,75,100,52,106,105,90,101,74,89,110,78,75,75,88,113,78,65,105,49,78,98,103,45,116,73,114,106,102,119,65,65];
const MAX_PROMPTS = 5;
const LIMIT_KEY = 'mush_ai_used';

function getUsed() {{ return parseInt(localStorage.getItem(LIMIT_KEY) || '0'); }}
function setUsed(n) {{ localStorage.setItem(LIMIT_KEY, String(n)); }}
function updateCounter() {{
  const left = Math.max(MAX_PROMPTS - getUsed(), 0);
  document.getElementById('ai-remaining').textContent = left > 0
    ? left + ' question' + (left !== 1 ? 's' : '') + ' remaining'
    : 'You have used all 5 free questions.';
  document.getElementById('ai-btn').disabled = left <= 0;
  if (left <= 0) document.getElementById('ai-btn').textContent = 'Limit reached';
}}
updateCounter();

async function askAI() {{
  const used = getUsed();
  if (used >= MAX_PROMPTS) {{ updateCounter(); return; }}
  const question = document.getElementById('ai-question').value.trim();
  if (!question) return;

  const btn = document.getElementById('ai-btn');
  const respDiv = document.getElementById('ai-response');
  btn.disabled = true;
  btn.textContent = 'Thinking...';
  respDiv.style.display = 'block';
  respDiv.textContent = 'Analyzing data...';

  try {{
    const apiKey = String.fromCharCode(..._k);
    const resp = await fetch('https://api.anthropic.com/v1/messages', {{
      method: 'POST',
      headers: {{
        'Content-Type': 'application/json',
        'x-api-key': apiKey,
        'anthropic-version': '2023-06-01',
        'anthropic-dangerous-direct-browser-access': 'true',
      }},
      body: JSON.stringify({{
        model: 'claude-sonnet-4-20250514',
        max_tokens: 1500,
        messages: [{{
          role: 'user',
          content: `You are a market analyst examining mushroom/functional mushroom product data from Amazon, iHerb, and Faire. Here is a summary of the dataset:\\n\\n${{AI_CONTEXT}}\\n\\nAnswer this question concisely with specific numbers and insights:\\n\\n${{question}}`
        }}],
      }})
    }});

    if (!resp.ok) {{
      const err = await resp.json();
      throw new Error(err.error?.message || `HTTP ${{resp.status}}`);
    }}

    const data = await resp.json();
    respDiv.textContent = data.content[0].text;
    setUsed(getUsed() + 1);
    updateCounter();
  }} catch (e) {{
    respDiv.textContent = 'Error: ' + e.message;
  }} finally {{
    btn.disabled = getUsed() >= MAX_PROMPTS;
    btn.textContent = getUsed() >= MAX_PROMPTS ? 'Limit reached' : 'Ask Claude';
  }}
}}
</script>
</body>
</html>"""


def fig_to_html(fig):
    """Convert a plotly figure (or raw HTML string) to an embeddable HTML div."""
    if isinstance(fig, str):
        return fig
    return fig.to_html(full_html=False, include_plotlyjs=False, config={
        "displaylogo": False,
        "modeBarButtonsToRemove": ["select2d", "lasso2d"],
    })


def main():
    print("Loading data…")
    products = load_all()
    print(f"  {len(products):,} products loaded")

    print("Building brand aggregates…")
    brands = build_brands(products)
    print(f"  {len(brands):,} brands")

    print("Loading Keepa history…")
    keepa_data = load_keepa_history()
    print(f"  {len(keepa_data)} ASINs with Keepa data")

    print("Generating charts…")
    figs = {
        "chart_brand_map":      chart_brand_map(brands),
        "chart_venn":           chart_venn(products),
        "chart_form_factor":    chart_form_factor_by_marketplace(products),
        "chart_mushroom_pop":   chart_mushroom_popularity(products),
        "chart_price_dist":     chart_price_distribution(products),
        "chart_top_brands":     chart_top_brands(brands),
        "chart_heatmap":        chart_price_mushroom_heatmap(products),
        "chart_top_revenue":    chart_top_revenue_products(products),
        "chart_brand_revenue":  chart_brand_revenue(products),
        "chart_sold_mushroom":  chart_sold_by_mushroom_type(products),
        "chart_rev_formfactor": chart_revenue_by_form_factor(products),
        "chart_market_map":     chart_market_map(products),
        "chart_review_velocity": chart_review_velocity(products),
        "chart_launch_timeline": chart_launch_timeline(products),
        "chart_launch_by_ff":   chart_launch_by_form_factor(products),
        "chart_launch_pct":     chart_launch_pct(products),
        "chart_review_growth":  chart_review_growth(keepa_data, products),
        "chart_review_by_ff":   chart_review_growth_by_ff(keepa_data, products),
        "chart_brand_growth":   chart_brand_growth(keepa_data, products),
        "chart_brand_growth_rate": chart_brand_growth_rate(keepa_data, products),
        "chart_sales_rank":     chart_sales_rank(keepa_data, products),
    }

    # Convert to HTML
    chart_html = {k: fig_to_html(v) for k, v in figs.items()}

    # Stats
    prices = [p["price"] for p in products if p.get("price")]
    multi_mp = sum(1 for b in brands if b["marketplaceCount"] >= 2)
    source_counts = Counter(p["source"] for p in products)
    total_rev = sum(p["price"] * p["soldPastMonth"] for p in products
                    if p.get("price") and p.get("soldPastMonth") and p["soldPastMonth"] > 0)

    # Build product JSON for data table (compact — only fields needed)
    print("Building data table JSON…")
    table_data = []
    for p in products:
        table_data.append({
            "source": p["source"],
            "id": p.get("id"),
            "brand": p.get("brand") or "Unknown Brand",
            "productName": p.get("productName"),
            "mushroomTypes": ", ".join(p["mushroomTypes"]) if isinstance(p.get("mushroomTypes"), list) else p.get("mushroomTypes"),
            "formFactor": p.get("formFactor"),
            "price": p.get("price"),
            "rating": p.get("rating"),
            "reviewCount": p.get("reviewCount"),
            "soldPastMonth": p.get("soldPastMonth"),
            "url": p.get("url") if p.get("url") and not p["url"].startswith("data:") else None,
        })
    product_json = json.dumps(table_data, default=str)

    # Build filter options
    ff_counts = Counter(p.get("formFactor") for p in products if p.get("formFactor"))
    ff_options = "".join(f"<option>{ff}</option>" for ff, _ in ff_counts.most_common())

    mt_set = Counter()
    for p in products:
        types = p.get("mushroomTypes", [])
        if isinstance(types, list):
            for mt in types:
                mt_set[mt] += 1
        elif isinstance(types, str) and types:
            for mt in types.split(", "):
                mt_set[mt] += 1
    mt_options = "".join(f"<option>{mt}</option>" for mt, _ in mt_set.most_common())

    # Build source filter options and footer summary dynamically
    active_sources = sorted(source_counts.keys(), key=lambda s: list(COLORS.keys()).index(s) if s in COLORS else 99)
    source_options = "".join(f"<option>{s}</option>" for s in active_sources)
    source_summary = ", ".join(f"{s} ({source_counts[s]:,})" for s in active_sources)
    marketplace_count = len(active_sources)

    # Build compact AI context summary
    print("Building AI context summary…")
    ai_lines = []
    ai_lines.append(f"DATASET: {len(products):,} products across {source_summary}")
    ai_lines.append(f"BRANDS: {len(brands):,} unique brands, {multi_mp} on 2+ marketplaces")
    ai_lines.append(f"EST MONTHLY REVENUE: ${total_rev:,.0f} (sources with sales volume data only)")
    ai_lines.append(f"AVG PRICE: ${statistics.mean(prices):.2f}")
    ai_lines.append("")
    ai_lines.append("FORM FACTOR BREAKDOWN (product count):")
    for ff, count in ff_counts.most_common(15):
        ai_lines.append(f"  {ff}: {count}")
    ai_lines.append("")
    ai_lines.append("MUSHROOM TYPE BREAKDOWN (product count):")
    for mt, count in mt_set.most_common(15):
        ai_lines.append(f"  {mt}: {count}")
    ai_lines.append("")
    ai_lines.append("TOP 30 BRANDS BY REVIEWS:")
    top_brands = sorted([b for b in brands if b.get("maxReviews", 0) > 0], key=lambda b: -b["maxReviews"])[:30]
    for b in top_brands:
        ai_lines.append(f"  {b['brand']}: {b['maxReviews']:,} reviews, {b['skus']} SKUs, "
                        f"avg ${b['avgPrice']:.2f}" + (f", on {', '.join(b['sources'])}" if b.get('sources') else ""))
    ai_lines.append("")
    ai_lines.append("TOP 20 PRODUCTS BY EST MONTHLY REVENUE:")
    rev_products = sorted(
        [p for p in products if p.get("price") and p.get("soldPastMonth") and p["soldPastMonth"] > 0],
        key=lambda p: -(p["price"] * p["soldPastMonth"])
    )[:20]
    for p in rev_products:
        rev = p["price"] * p["soldPastMonth"]
        ai_lines.append(f"  ${rev:,.0f}/mo | {p.get('brand','?')} | {p['productName'][:50]} | {p['source']}")

    ai_context = "\n".join(ai_lines)
    ai_context_json = json.dumps(ai_context)

    html = HTML_TEMPLATE.format(
        total_products=len(products),
        total_brands=len(brands),
        multi_marketplace=multi_mp,
        marketplace_count=marketplace_count,
        avg_price=f"${statistics.mean(prices):.2f}" if prices else "N/A",
        est_monthly_rev=f"${total_rev/1e6:.1f}M",
        source_options=source_options,
        source_summary=source_summary,
        product_json=product_json,
        ff_options=ff_options,
        mt_options=mt_options,
        ai_context_json=ai_context_json,
        **chart_html,
    )

    # Add plotly.js CDN at the top
    html = html.replace("</head>",
        '<script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>\n</head>')

    Path(OUTPUT_HTML).write_text(html, encoding="utf-8")
    print(f"\n✅ Dashboard saved → {OUTPUT_HTML}")
    print(f"   Open in browser: open {OUTPUT_HTML}")


if __name__ == "__main__":
    main()
