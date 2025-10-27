import boto3
import json
from decimal import Decimal
from boto3.dynamodb.conditions import Key, Attr

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('BibleCommentsV2')

def clean_decimals(obj):
    if isinstance(obj, list):
        return [clean_decimals(i) for i in obj]
    elif isinstance(obj, dict):
        return {k: clean_decimals(v) for k, v in obj.items()}
    elif isinstance(obj, Decimal):
        return int(obj) if obj % 1 == 0 else float(obj)
    else:
        return obj

def lambda_handler(event, context):
    params = event.get("queryStringParameters") or {}
    book = params.get("book")
    chapter = params.get("chapter")
    vfrom = params.get("from")
    vto = params.get("to")

    if not book or not chapter:
        return {
            "statusCode": 400,
            "body": json.dumps({"error": "Zadej alespoň book a chapter"})
        }

    try:
        chapter = int(chapter)

        if vfrom and vto:
            vfrom = int(vfrom)
            vto = int(vto)
            response = table.query(
                IndexName="ChapterRangeIndex",
                KeyConditionExpression=Key("Chapter").eq(chapter) & Key("VerseFrom").lte(vto),
                FilterExpression=Attr("VerseTo").gte(vfrom) & Attr("Book").eq(book)
            )
        else:
            response = table.scan(
                FilterExpression=Attr("Book").eq(book) & Attr("Chapter").eq(chapter)
            )

        items = response.get("Items", [])
        cleaned = clean_decimals(items)

        return {
            "statusCode": 200,
            "headers": { "Access-Control-Allow-Origin": "*" },
            "body": json.dumps(cleaned)
        }

    except Exception as e:
        return {
            "statusCode": 500,
            "headers": { "Access-Control-Allow-Origin": "*" },
            "body": json.dumps({"error": str(e)})
        }
