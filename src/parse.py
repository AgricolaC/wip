import re, json, pathlib, os
from bs4 import BeautifulSoup
from tqdm import tqdm

RAW = pathlib.Path(os.getenv("FILINGS_RAW_DIR", "data/raw"))
OUT = pathlib.Path("data/processed"); OUT.mkdir(parents=True, exist_ok=True)
OUT_FILE = OUT / "toy_item1A_item7.jsonl"

def pull_sections(html_text: str) -> dict:
    """Return a dict of {'item_1a': text, 'item_7': text} (plain)."""
    soup = BeautifulSoup(html_text, "lxml")
    text = soup.get_text("\n")                      # raw plain text

    # crude but effective regex to slice sections
    def grab(start_pat, end_pat):
        pattern = rf"{start_pat}(.*?){end_pat}"
        m = re.search(pattern, text, flags=re.I | re.S)
        return m.group(1).strip() if m else ""

    return {
        "item_1a": grab(r"item\s+1a[^a-z]", r"item\s+1b"),
        "item_7":  grab(r"item\s+7[^0-9a-z]", r"item\s+7a"),
    }

with OUT_FILE.open("w", encoding="utf-8") as fout:
    for fp in tqdm(list(RAW.glob("*.html"))):
        doc_id = fp.stem                    # filename without .html
        sections = pull_sections(fp.read_text(errors="ignore"))
        for sec_name, content in sections.items():
            if content:
                fout.write(json.dumps({
                    "doc_id": doc_id,
                    "section": sec_name,
                    "text": content
                }) + "\n")

print("✅ extracted →", OUT_FILE, "lines:", sum(1 for _ in open(OUT_FILE)))
