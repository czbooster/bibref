from flask import Flask, request, jsonify, render_template_string
from elasticsearch import Elasticsearch
import re
from markupsafe import escape, Markup


app = Flask(__name__)

# Připojení k Elasticsearch běžícímu na serveru raspberrypi
es = Elasticsearch("http://raspberrypi:9200")

INDEX_NAME = "bible_comments"

# Pokud index neexistuje, vytvoříme ho
if not es.indices.exists(index=INDEX_NAME):
    es.indices.create(
        index=INDEX_NAME,
        body={
            "mappings": {
                "properties": {
                    "book": {"type": "keyword"},
                    "chapter": {"type": "integer"},
                    "verse_from": {"type": "integer"},
                    "verse_to": {"type": "integer"},
                    "author": {"type": "text"},
                    "source": {"type": "text"},
                    "comment": {"type": "text"},
                }
            }
        },
    )


def parse_reference(ref):
    """Parsuje biblický odkaz jako 'J 1 1-10' → (book='J', chapter=1, verse_from=1, verse_to=10)"""
    m = re.match(r"(\w+)\s+(\d+)\s+(\d+)(?:-(\d+))?", ref.strip())
    if not m:
        raise ValueError("Invalid reference format. Use e.g. 'J 1 1-10'")
    book, chapter, vfrom, vto = m.group(1), int(m.group(2)), int(m.group(3)), m.group(4)
    return book, chapter, vfrom, int(vto) if vto else int(vfrom)


@app.route("/")
def index():
    """Základní HTML rozhraní s filtrem autora"""
    html = """
    <html>
    <head>
        <title>Bible Comments</title>
        <style>
            body { font-family: sans-serif; max-width: 700px; margin: 40px auto; }
            h1 { color: #333; }
            input, textarea { width: 100%; margin-bottom: 8px; padding: 6px; }
            button { padding: 6px 10px; }
            .comment { background: #f6f6f6; padding: 10px; border-radius: 6px; margin-bottom: 10px; }
        </style>
    </head>
    <body>
        <h1>Bible Comments</h1>

        <h2>Přidat komentář</h2>
        <form action="/add" method="post">
            <input name="reference" placeholder="Odkaz např. J 1 1-10" required><br>
            <input name="author" placeholder="Autor"><br>
            <input name="source" placeholder="Zdroj"><br>
            <textarea name="comment" placeholder="Text komentáře" rows="4" required></textarea><br>
            <label for="language">Jazyk:</label>
            <select name="language" id="language">
                <option value="cs" selected>Čeština</option>
                <option value="sk">Slovenština</option>
                <option value="en">Angličtina</option>
            </select>
            <button type="submit">Uložit</button>
        </form>

        <hr>

        <h2>Vyhledávání</h2>
        <form action="/search" method="get">
            <input name="q" placeholder="Hledat text..." required><br>
            <input name="author" placeholder="Autor (volitelné)"><br>
            <button type="submit">Hledat</button>
        </form>

        <form action="/range" method="get" style="margin-top:10px;">
            <input name="ref" placeholder="Rozsah např. J 1 1-10" required>
            <button type="submit">Najít podle rozsahu</button>
        </form>
    </body>
    </html>
    """
    return render_template_string(html)


@app.route("/add", methods=["POST"])
def add_comment():
    """Přidá nový komentář přes formulář nebo JSON API."""
    data = request.form if request.form else request.json
    try:
        book, chapter, vfrom, vto = parse_reference(data["reference"])
    except Exception as e:
        return jsonify({"error": str(e)}), 400

    doc = {
        "book": book,
        "chapter": chapter,
        "verse_from": vfrom,
        "verse_to": vto,
        "author": data.get("author"),
        "source": data.get("source"),
        "comment": data.get("comment"),
        "language": data.get("language", "cs")  # výchozí čeština
    }
    es.index(index=INDEX_NAME, document=doc)
    return (
        "<p>✅ Komentář uložen.</p><a href='/'>Zpět</a>"
        if request.form
        else jsonify({"status": "ok", "saved": doc})
    )


@app.route("/search", methods=["GET"])
def search_comments():
    query = request.args.get("q", "")
    author = request.args.get("author", "")
    lang = request.args.get("lang", "")

    must_clauses = []
    if query:
        must_clauses.append({
            "multi_match": {
                "query": query,
                "fields": ["comment"],
                "analyzer": "cs_sk_analyzer"
            }
        })

    if author:
        must_clauses.append({"match": {"author": author}})

    if lang:
        langs = [l.strip() for l in lang.split(",") if l.strip()]
        must_clauses.append({"terms": {"language": langs}})

    body = {
        "query": {
            "bool": {
                "must": must_clauses if must_clauses else {"match_all": {}}
            }
        },
        "_source": ["comment", "author", "book", "chapter", "verse_from", "verse_to", "date"]
    }

    res = es.search(index=INDEX_NAME, body=body)

    if "text/html" in request.headers.get("Accept", ""):
        html = f"<h2>Výsledky hledání pro <em>{escape(query)}</em></h2>"
        for hit in res["hits"]["hits"]:
            c = hit["_source"]
            ref_display = f"{c['book']} {c['chapter']}:{c['verse_from']}-{c['verse_to']}"
            created = c['date']
            obohu_url = f"https://www.obohu.cz/bible/index.php?styl=KLP&v={c['verse_from']}-{c['verse_to']}&kv={c['verse_from']}-{c['verse_to']}&k={c['book']}&kap={c['chapter']}#v{c['verse_from']}-{c['verse_to']}"

            comment = (c['comment']).replace("\n", "<br>")

            # zvýraznění hledaného výrazu
            if query:
                pattern = re.compile(re.escape(query), re.IGNORECASE)
                comment = pattern.sub(lambda m: f"<mark>{m.group(0)}</mark>", comment)

            html += f"""
            <div class='comment'>
                <b>{ref_display}</b><br>
                <small>({created})</small> -
                <a href="{obohu_url}" target="_blank">Zobrazit v Bibli</a><br>
                {comment}<br>
                <i>{escape(c.get('author',''))}</i>
            </div>
            """
        html += "<a href='/'>Zpět</a>"
        return html
    else:
        return jsonify([hit["_source"] for hit in res["hits"]["hits"]])

@app.route("/range", methods=["GET"])
def get_by_range():
    ref = request.args.get("ref")
    if not ref:
        return jsonify({"error": "Missing ?ref parameter"}), 400

    try:
        book, chapter, vfrom, vto = parse_reference(ref)
    except Exception as e:
        return jsonify({"error": str(e)}), 400

    query = {
        "bool": {
            "must": [
                {"term": {"book": book}},
                {"term": {"chapter": chapter}},
                {"range": {"verse_from": {"lte": vto}}},
                {"range": {"verse_to": {"gte": vfrom}}},
            ]
        }
    }

    res = es.search(index=INDEX_NAME, query=query)

    if "text/html" in request.headers.get("Accept", ""):
        html = f"<h2>Komentáře k <em>{escape(ref)}</em></h2>"
        for hit in res["hits"]["hits"]:
            c = hit["_source"]
            verse_from = c['verse_from']
            verse_to = c['verse_to']
            book = c['book']
            chapter = c['chapter']
            created = c.get("date", "")[:10]  # zobrazit jen datum

            obohu_url = f"https://www.obohu.cz/bible/index.php?styl=KLP&v={verse_from}-{verse_to}&kv={verse_from}-{verse_to}&k={book}&kap={chapter}#v{verse_from}-{verse_to}"
            ref_display = f"{book} {chapter}:{verse_from}-{verse_to}"

            comment = (c['comment']).replace("\n", "<br>")

            html += f"""
            <div class='comment'>
                <b>{ref_display}</b> <small>({created})</small> -
                <a href="{obohu_url}" target="_blank">Zobrazit v Bibli</a><br>
                {comment}<br>
                <i>{escape(c.get('author',''))}</i>
            </div>
            """
        html += "<a href='/'>Zpět</a>"
        return html
    else:
        return jsonify([hit["_source"] for hit in res["hits"]["hits"]])

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
