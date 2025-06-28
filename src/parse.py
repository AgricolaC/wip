import re, json, os, pathlib
from bs4 import BeautifulSoup
from tqdm import tqdm

RAW_DIR   = pathlib.Path(os.getenv("FILINGS_RAW_DIR", "data/raw"))
OUT_DIR   = pathlib.Path("data/processed")
OUT_DIR.mkdir(parents=True, exist_ok=True)
OUT_FILE  = OUT_DIR / "item1a_item7.jsonl"

def extract_sections(html_text: str) -> dict:
    """
    Return {'item_1a': str, 'item_7': str}.
    Uses plain-text regex so it works on ASCII filings too.
    """
    soup  = BeautifulSoup(html_text, "lxml")
    text  = soup.get_text("\n")        # one giant string
    text  = re.sub(r'\xa0', ' ', text) # replace non-breaking spaces

    def grab(start_pat, end_pat):
        # DOTALL so newlines match '.', IGNORECASE for 'Item 1A', 'ITEM 1A.'
        pat = rf"{start_pat}(.*?){end_pat}"
        m   = re.search(pat, text, flags=re.I | re.S)
        return m.group(1).strip() if m else ""

    return {
        "item_1a": grab(r"item\s+1a[^a-z]", r"item\s+1b"),
        "item_7" : grab(r"item\s+7[^0-9a-z]", r"item\s+7a"),
    }

# ──────────────────────────────────────────────────────────────
with OUT_FILE.open("w", encoding="utf-8") as fout:
    files = list(RAW_DIR.glob("*.html")) + list(RAW_DIR.glob("*.txt"))
    for fp in tqdm(files, desc="Parsing filings"):        sections = extract_sections(fp.read_text(errors="ignore"))
        if not any(sections.values()):
            print("⚠️  No matches in", fp.name)
            continue                       # skip empty hits
        for sec_name, content in sections.items():
            if content:
                fout.write(json.dumps({
                    "doc_id" : fp.stem,
                    "section": sec_name,   # 'item_1a' or 'item_7'
                    "text"   : content
                }) + "\n")

print("✅  Written", OUT_FILE, "→", sum(1 for _ in open(OUT_FILE)), "records")
