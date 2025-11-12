#!/usr/bin/env python3
"""
Small utility: extract Bible-comments-style items from an HTML file and
print them as JSON (or optionally write to DynamoDB).

Usage:
  python extract_html_to_dynamo.py resources/test.html
  python extract_html_to_dynamo.py resources/test.html --write   # write to DynamoDB (requires AWS creds)

This is a minimal, self-contained script using BeautifulSoup.
Install deps: pip install beautifulsoup4 lxml boto3
"""
import argparse
import hashlib
import json
import os
from datetime import datetime

from bs4 import BeautifulSoup

try:
    import boto3
except Exception:
    boto3 = None


def clean_text(s: str) -> str:
    return " ".join(s.split())


def compute_hash(subject: str, body: str) -> str:
    m = hashlib.md5()
    m.update((subject + body).encode("utf-8"))
    return m.hexdigest()


def normalize_subject(subject: str) -> str:
    # remove space after comma before digits: "Jn 1, 10-18" -> "Jn 1,10-18"
    import re
    return re.sub(r",\s+(?=\d)", ",", subject)


def parse_reference_text(text: str):
    """Return (book, chapter, verse_from, verse_to) or raise ValueError.

    Accepts variants like:
      'Lk 3, 10 – 18' or 'Lk 3,10-18'
    """
    import re
    # normalize some dash variants and spaces
    t = text.replace("\u2013", "-").replace("\u2014", "-")
    t = normalize_subject(t)
    # try strict pattern first: Book 1,2-3
    m = re.search(r"([A-Za-zČčĎďĹĺŇňŠšŽžÝýÁáÉéÍíÓóÚúŮů]+)\s+(\d+)\s*,\s*(\d+)\s*-\s*(\d+)", t)
    if m:
        book, chapter, vfrom, vto = m.groups()
        return book, int(chapter), int(vfrom), int(vto)

    # fallback: capture book, chapter and the whole verse-section, then extract numbers
    m2 = re.search(r"([A-Za-zČčĎďĹĺŇňŠšŽžÝýÁáÉéÍíÓóÚúŮů]+)\s+(\d+)\s*,\s*([0-9A-Za-z\.,\s\-–—]+)", t)
    if not m2:
        raise ValueError("neparsovatelná reference")

    book, chapter, verse_section = m2.groups()
    # find all integer sequences in the verse section
    nums = re.findall(r"(\d+)", verse_section)
    if not nums:
        raise ValueError("neparsovatelná reference")

    # per rule: verse_from = first number up to first non-numeric char, verse_to = last number after last non-numeric char
    vfrom = int(nums[0])
    vto = int(nums[-1])
    return book, int(chapter), vfrom, vto


def extract_items_from_html(filepath: str):
    with open(filepath, 'r', encoding='utf-8') as f:
        soup = BeautifulSoup(f, 'lxml')

    items = []

    # Find every <h3 class="block_7"> and work within its parent DIV
    for h3 in soup.find_all('h3', class_='block_7'):
        parent_div = h3.find_parent('div')
        if not parent_div:
            continue

        # First <p class="block_"> after the h3 inside the same div is the reference
        ref_p = None
        for tag in h3.find_next_siblings():
            if tag == parent_div:
                break
            if tag.name == 'p' and 'block_' in (tag.get('class') or []):
                ref_p = tag
                break

        # Fallback: search within parent_div for first such p after h3
        if ref_p is None:
            for tag in parent_div.find_all('p', class_='block_'):
                # if possible, compare source lines; otherwise take the first occurrence
                ref_p = tag
                break

        if ref_p is None:
            continue

        # Build subject string from ref_p: book from <i> and the rest text
        i_tag = ref_p.find('i')
        if i_tag:
            book_code = clean_text(i_tag.get_text())
            # subject_text is the whole p text minus the i text
            full_text = clean_text(ref_p.get_text())
            # remove the book code from start (first occurrence)
            subject_text = full_text[len(book_code):].strip()
            subject = f"{book_code} {subject_text}"
        else:
            subject = clean_text(ref_p.get_text())

        subject_normalized = normalize_subject(subject)

        # Collect following paragraphs up to end of parent_div
        body_parts = []
        started = False
        for tag in parent_div.find_all(['p', 'h3', 'h4', 'div']):
            # We only want p elements after ref_p
            if tag is ref_p:
                started = True
                continue
            if not started:
                continue
            if tag.name == 'p':
                body_parts.append(clean_text(tag.get_text()))

        if not body_parts:
            # nothing to extract
            continue

        # Title: use H3 and H4 content (H3 is the main heading, H4 optional)
        h3_text = clean_text(h3.get_text())
        h4_tag = h3.find_next('h4')
        h4_text = ''
        if h4_tag and parent_div in h4_tag.parents:
            h4_text = clean_text(h4_tag.get_text())
        title = ' - '.join([t for t in (h3_text, h4_text) if t])

        # Comment: everything after the reference (all collected paragraphs)
        comment = "\n\n".join(body_parts)

        # parse reference
        try:
            book, chapter, verse_from, verse_to = parse_reference_text(subject_normalized)
        except ValueError:
            # skip or record unparsable
            items.append({
                'error': 'neparsovatelná reference',
                'subject': subject,
                'title': title,
                'comment': comment,
            })
            continue

        url = f"https://www.obohu.cz/bible/index.php?styl=KLP&v={verse_from}-{verse_to}&kv={verse_from}-{verse_to}&k={book}&kap={chapter}#v{verse_from}-{verse_to}"

        md5_hash = compute_hash(subject, comment)

        item = {
            'Book': book,
            'Chapter': chapter,
            'VerseFrom': verse_from,
            'VerseTo': verse_to,
            'CreatedAt': datetime.utcnow().isoformat() + 'Z',
            'Author': None,
            'Source': subject_normalized,
            'Title': title,
            'Comment': comment,
            'URL': url,
            'Language': 'sk',
            'hash': md5_hash,
        }

        items.append(item)

    return items


def write_to_dynamo(items, table_name='BibleCommentsV2'):
    if boto3 is None:
        raise RuntimeError('boto3 not available')
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)
    for it in items:
        # skip error rows
        if it.get('error'):
            print('Skipping unparsable:', it['subject'])
            continue
        table.put_item(Item=it)
        print('Wrote item:', it['Source'])


def main():
    p = argparse.ArgumentParser()
    p.add_argument('html', help='path to html file')
    p.add_argument('--write', action='store_true', help='write items to DynamoDB')
    p.add_argument('--table', default='BibleCommentsV2', help='DynamoDB table name')
    args = p.parse_args()

    items = extract_items_from_html(args.html)
    print(json.dumps({'count': len(items), 'items_preview': items[:5]}, ensure_ascii=False, indent=2))

    if args.write:
        write_to_dynamo(items, table_name=args.table)


if __name__ == '__main__':
    main()
