from elasticsearch import Elasticsearch

es = Elasticsearch("http://raspberrypi:9200")

index_name = "bible_comments"

if es.indices.exists(index=index_name):
    es.indices.delete(index=index_name)

settings = {
    "settings": {
        "analysis": {
            "filter": {
                "cs_sk_stemmer": {
                    "type": "stemmer",
                    "name": "czech"   # používáme český stemmer i pro slovenštinu
                },
                "cs_sk_stop": {
                    "type": "stop",
                    "stopwords": "_czech_"  # česká stopslova (lze doplnit vlastní)
                }
            },
            "analyzer": {
                "cs_sk_analyzer": {
                    "tokenizer": "standard",
                    "filter": [
                        "lowercase",
                        "cs_sk_stop",
                        "cs_sk_stemmer"
                    ]
                }
            }
        }
    },
    "mappings": {
        "properties": {
            "date": {"type": "keyword"},
            "book": {"type": "keyword"},
            "chapter": {"type": "integer"},
            "verse_from": {"type": "integer"},
            "verse_to": {"type": "integer"},
            "title": {"type": "text", "analyzer": "cs_sk_analyzer"},
            "comment": {"type": "text", "analyzer": "cs_sk_analyzer"},
            "author": {"type": "keyword"},
            "url": {"type": "keyword"},
            "language": {"type": "keyword"},
            "md5": {"type": "keyword"}
        }
    }
}

es.indices.create(index=index_name, body=settings)
print("✅ Index vytvořen:", index_name)