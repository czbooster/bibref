import json
import re
import hashlib
from elasticsearch import Elasticsearch, helpers

# ======== KONFIGURACE ========
JSON_PATH = "emails_export (2).json"
INDEX_NAME = "bible_comments"
# =============================

# Připojení k Elasticsearch
es = Elasticsearch("http://raspberrypi:9200")

documents = []
skipped = []

def parse_reference(ref_text):
    match = re.search(r"([A-Za-z]+)\s+(\d+),(\d+)-(\d+)", ref_text)
    if not match:
        raise ValueError("neparsovatelná reference")
    book, chapter, vfrom, vto = match.groups()
    return book, int(chapter), int(vfrom), int(vto)

def clean_text(s):
    return " ".join(s.split())

def compute_hash(subject, body):
    m = hashlib.md5()
    m.update((subject + body).encode("utf-8"))
    return m.hexdigest()

def exists_in_elastic(md5_hash):
    query = {
        "query": {
            "term": {"hash.keyword": md5_hash}
        }
    }
    resp = es.search(index=INDEX_NAME, body=query)
    return resp["hits"]["total"]["value"] > 0

# Načtení JSON dat
with open(JSON_PATH, "r", encoding="utf-8") as f:
    emails = json.load(f)

print(f"📦 Celkový počet zpráv v JSON: {len(emails)}")

for i, email_obj in enumerate(emails, 1):
    subject = email_obj.get("subject", "")
    from_ = email_obj.get("from", "")
    body = email_obj.get("body", "")
    date = email_obj.get("date", None)

    if "Kopie:" in body:
        body = body.split("Kopie:")[-1]
    elif "---------- Přeposlaná zpráva ----------" in body:
        body = body.split("---------- Přeposlaná zpráva ----------")[-1]

    lines = [l.strip() for l in body.splitlines() if l.strip()]
    if len(lines) < 2:
        skipped.append((i, subject, "příliš krátké tělo zprávy"))
        continue

    title = clean_text(lines[0])  # zachovat čistý titulek

    # 🆕 Parsování reference ze subjectu
    ref_match = re.search(r"([A-Za-z]+ \d+,\d+-\d+)", subject)
    if not ref_match:
        skipped.append((i, subject, "chybí reference v subjectu"))
        continue

    ref_text = ref_match.group(1)
    try:
        book, chapter, verse_from, verse_to = parse_reference(ref_text)
    except ValueError as e:
        skipped.append((i, subject, str(e)))
        continue

    comment = "\n".join(lines[2:])  # zachovat odřádkování
    url = f"https://www.obohu.cz/bible/index.php?styl=KLP&v={verse_from}-{verse_to}&kv={verse_from}-{verse_to}&k={book}&kap={chapter}#v{verse_from}-{verse_to}"
    md5_hash = compute_hash(subject, body)

    if exists_in_elastic(md5_hash):
        skipped.append((i, subject, "duplicitní hash"))
        continue

    doc = {
        "hash": md5_hash,
        "book": book,
        "chapter": chapter,
        "verse_from": verse_from,
        "verse_to": verse_to,
        "title": title,
        "comment": comment,
        "author": from_,
        "source": subject,
        "url": url,
        "language": "sk",
        "date": date
    }

    documents.append(doc)

# Import do Elasticsearch
if documents:
    helpers.bulk(es, [{"_index": INDEX_NAME, "_source": d} for d in documents])
    print(f"✅ Naimportováno {len(documents)} komentářů do Elasticsearch.")
else:
    print("⚠️ Nebyl nalezen žádný nový komentář k importu.")

# Výpis přeskočených zpráv
if skipped:
    print(f"\n🛑 Přeskočeno {len(skipped)} zpráv:")
    for i, subj, reason in skipped:
        print(f"  #{i}: '{subj}' → {reason}")
