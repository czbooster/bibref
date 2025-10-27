import imaplib
import email
from email.header import decode_header
import re
from elasticsearch import Elasticsearch, helpers
import hashlib

# ======== KONFIGURACE ========
IMAP_SERVER = "imap.centrum.cz"
EMAIL_ACCOUNT = "booster@atlas.cz"
EMAIL_PASSWORD = "K2438dih"
MAILBOX = "dvorovy"  # přímo složka se štítkem
INDEX_NAME = "bible_comments"
# =============================

# Připojení k Elasticsearch
es = Elasticsearch("http://raspberrypi:9200")

# Připojení k IMAP
mail = imaplib.IMAP4_SSL(IMAP_SERVER)
mail.login(EMAIL_ACCOUNT, EMAIL_PASSWORD)
mail.select(MAILBOX)

# Vyhledání všech emailů ve složce
status, messages = mail.search(None, 'ALL')
email_ids = messages[0].split()

documents = []

def parse_reference(ref_text):
    """
    Parsuje referenci typu: Lk 12,39-48
    Vrací: book, chapter, verse_from, verse_to
    """
    match = re.search(r"([A-Za-z]+)\s+(\d+),(\d+)-(\d+)", ref_text)
    if not match:
        raise ValueError(f"Nepodařilo se parsovat referenci: {ref_text}")
    book, chapter, vfrom, vto = match.groups()
    return book, int(chapter), int(vfrom), int(vto)

def clean_text(s):
    """Odstraní přebytečné bílé znaky a nové řádky"""
    return " ".join(s.split())

def compute_hash(subject, body):
    """Spočítá MD5 hash ze subjectu + těla zprávy"""
    m = hashlib.md5()
    m.update((subject + body).encode("utf-8"))
    return m.hexdigest()

def exists_in_elastic(md5_hash):
    """Zjistí, zda už dokument s daným hashem existuje"""
    query = {
        "query": {
            "term": {"hash.keyword": md5_hash}
        }
    }
    resp = es.search(index=INDEX_NAME, body=query)
    return resp["hits"]["total"]["value"] > 0

for eid in email_ids:
    status, msg_data = mail.fetch(eid, "(RFC822)")
    raw_email = msg_data[0][1]
    msg = email.message_from_bytes(raw_email)
    
    # Dekódování předmětu
    subject, encoding = decode_header(msg["Subject"])[0]
    if isinstance(subject, bytes):
        subject = subject.decode(encoding or "utf-8")

    # Odesílatel
    from_ = msg.get("From")

    # Extrahování těla (text)
    body = ""
    if msg.is_multipart():
        for part in msg.walk():
            ctype = part.get_content_type()
            cdispo = str(part.get("Content-Disposition"))
            if ctype == "text/plain" and "attachment" not in cdispo:
                body = part.get_payload(decode=True).decode("utf-8", errors="ignore")
                break
    else:
        body = msg.get_payload(decode=True).decode("utf-8", errors="ignore")

    # Odstranit hlavičky přeposlané zprávy
    if "Kopie:" in body:
        body = body.split("Kopie:")[-1]
    elif "---------- Přeposlaná zpráva ----------" in body:
        body = body.split("---------- Přeposlaná zpráva ----------")[-1]

    lines = [l.strip() for l in body.splitlines() if l.strip()]
    if len(lines) < 2:
        continue  # příliš krátký email

    title = clean_text(lines[0])
    
    ref_line = lines[1]
    ref_match = re.search(r"\(([A-Za-z]+ \d+,\d+-\d+)\)", ref_line)
    if not ref_match:
        continue
    ref_text = ref_match.group(1)
    try:
        book, chapter, verse_from, verse_to = parse_reference(ref_text)
    except ValueError:
        continue

    # Komentář: vše od třetího řádku dál
    comment = clean_text("\n".join(lines[2:]))

    # URL do Obohu.cz
    url = f"https://www.obohu.cz/bible/index.php?styl=KLP&v={verse_from}-{verse_to}&kv={verse_from}-{verse_to}&k={book}&kap={chapter}#v{verse_from}-{verse_to}"

    # Spočítej hash
    md5_hash = compute_hash(subject, body)

    # Přeskoč, pokud už existuje
    if exists_in_elastic(md5_hash):
        print(f"✋ Přeskočeno (duplicitní): {subject}")
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
        "language": "sk"  # ← doplněno
    }

    documents.append(doc)

# Hromadný import do Elasticsearch
if documents:
    helpers.bulk(es, [{"_index": INDEX_NAME, "_source": d} for d in documents])
    print(f"Naimportováno {len(documents)} nových komentářů do Elasticsearch.")
else:
    print("Nebyl nalezen žádný nový komentář k importu.")

mail.logout()
