import json
import re
import hashlib
import boto3
import os

# DynamoDB klient
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('BibleCommentsV2')

# ======== KONFIGURACE ========
S3_BUCKET = os.environ.get("S3_BUCKET")
S3_KEY = os.environ.get("S3_KEY")
# =============================

s3 = boto3.client('s3')

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

def lambda_handler(event, context):
    skipped = []
    imported = 0

    # Načtení JSON ze S3
    obj = s3.get_object(Bucket=S3_BUCKET, Key=S3_KEY)
    emails = json.loads(obj['Body'].read().decode('utf-8'))

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

        title = clean_text(lines[0])

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

        comment = "\n".join(lines[2:])
        url = f"https://www.obohu.cz/bible/index.php?styl=KLP&v={verse_from}-{verse_to}&kv={verse_from}-{verse_to}&k={book}&kap={chapter}#v{verse_from}-{verse_to}"
        md5_hash = compute_hash(subject, body)

        # Kontrola duplicitního hash
        response = table.query(
            IndexName="HashIndex",
            KeyConditionExpression=boto3.dynamodb.conditions.Key("hash").eq(md5_hash)
        )

        if response.get("Items"):
            skipped.append((i, subject, "duplicitní hash"))
            continue

        item = {
            'Book': book,
            'Chapter': chapter,
            'VerseFrom': verse_from,
            'VerseTo': verse_to,
            'CreatedAt': date or f"import-{i}",
            'Author': from_,
            'Source': subject,
            'Title': title,
            'Comment': comment,
            'URL': url,
            'Language': 'sk',
            'hash': md5_hash
        }

        table.put_item(Item=item)
        imported += 1

    print(f"✅ Naimportováno {imported} komentářů do DynamoDB.")

    if skipped:
        print(f"\n🛑 Přeskočeno {len(skipped)} zpráv:")
        for i, subj, reason in skipped:
            print(f"  #{i}: '{subj}' → {reason}")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "imported": imported,
            "skipped": len(skipped)
        })
    }
