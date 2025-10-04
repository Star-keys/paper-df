from collections import defaultdict
from pymongo import MongoClient
from elasticsearch import Elasticsearch, helpers
import psycopg2
import json

def get_mongo_col():
    mongo_client = MongoClient(
        "",
        socketTimeoutMS=3600 * 1000 * 10,
        connectTimeoutMS=3600 * 1000 * 5,
        serverSelectionTimeoutMS=3600 * 1000 * 5
    )
    mongo_db = mongo_client["starkeys"]
    return mongo_client, mongo_db["paper_json"]

def get_entity_types_from_pg(pmc_id):
    conn = psycopg2.connect(
        dbname="postgres", user="", password="",
        host="", port=5432
    )
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT entity_type FROM starkeys.category WHERE paper_id = %s", (pmc_id,))
    types = [row[0] for row in cur.fetchall()]
    cur.close()
    conn.close()
    return types

def parse_json_entry(entry, pmcId):
    result = {
        "paperId": pmcId,
        "authors": [],
        "title": "",
        "doi": "",
        "abstract": "",
        "introduction": "",
        "method": "",
        "result": "",
        "discussion": "",
        "conclusion": "",
        "fields": []
    }
    type_map = {
        "TITLE": "title",
        "ABSTRACT": "abstract",
        "INTRO": "introduction",
        "METHODS": "method",
        "RESULTS": "result",
        "DISCUSS": "discussion",
        "CONCL": "conclusion"
    }
    type_texts = defaultdict(list)
    section_counters = defaultdict(int)
    for passage in entry.get('passages', []):
        infons = passage.get("infons", {})
        section_type = infons.get("section_type", "").upper()
        text = passage.get('text', '').strip()
        type_key = type_map.get(section_type)
        # TITLE, INTRO 제외 first passage 무시
        if type_key and section_type not in {"TITLE", "ABSTRACT"}:
            section_counters[section_type] += 1
            if section_counters[section_type] == 1:
                continue
        if type_key and text:
            type_texts[type_key].append(text)
        if section_type == "TITLE":
            result["title"] = text
            result["doi"] = infons.get("article-id_doi", "")
            # author 추출
            author_fields = [k for k in infons if k.startswith('name')]
            for field in author_fields:
                value = infons[field]
                surname = ""
                given_names = ""
                for part in value.split(';'):
                    if part.startswith("surname"):
                        surname = part.replace("surname", "").strip(":").strip()
                    elif part.startswith("given-names"):
                        given_names = part.replace("given-names", "").strip(":").strip()
                full_name = f"{surname} {given_names}".strip()
                if full_name:
                    result["authors"].append(full_name)
    for k in type_map.values():
        result[k] = "\n".join(type_texts[k])
    result["fields"] = get_entity_types_from_pg(pmcId)
    return result

def bulk_insert_to_es(es_docs, es_host, es_api, es_index):
    es = Elasticsearch(
        es_host,
        api_key=es_api
    )
    actions = [
        {
            "_index": es_index,
            "_id": doc["paperId"] if "_id" not in doc else doc["_id"],
            "_source": doc
        }
        for doc in es_docs
    ]
    helpers.bulk(es, actions)
    print(f"ES Bulk insert completed: {len(es_docs)} docs")

def bulk_insert_authors_to_es(author_docs, es_host, es_api, es_index):
    es = Elasticsearch(
        es_host,
        api_key=es_api
    )
    actions = [
        {
            "_index": es_index,
            "_source": doc
        }
        for doc in author_docs
    ]
    helpers.bulk(es, actions)
    print(f"ES Author Bulk insert completed: {len(author_docs)} docs")

# 메인 실행 로직
BATCH = 50
ES_HOST = ""
ES_API = ""
PAPER_INDEX = ""
AUTHOR_INDEX = ""

mongo_client, papers_col = get_mongo_col()
total = papers_col.count_documents({})
mongo_client.close()

for start in range(0, total, BATCH):
    mongo_client, papers_col = get_mongo_col()
    cursor = papers_col.find({}, {"bioc_json": 1, "_id": 1}).sort("_id", 1).skip(start).limit(BATCH)
    paper_docs = []
    author_docs = []
    for doc in cursor:
        raw_json = doc.get("bioc_json")
        if isinstance(raw_json, str):
            try:
                json_entry = json.loads(raw_json)
            except Exception as e:
                print(f"JSON 변환 실패", e)
                continue
        else:
            json_entry = raw_json
        try:
            paper_id = doc.get("_id")
            main_entry = json_entry[0]["documents"][0]
        except Exception as e:
            print(f"잘못된 json 구조: {e}")
            continue
        paper_doc = parse_json_entry(main_entry, paper_id)
        # author는 paper와 분리, doc 내에서 추출
        if "authors" in paper_doc and paper_doc["authors"]:
            for author in paper_doc["authors"]:
                author_docs.append({
                    "name": author,
                    "paperId": paper_doc["paperId"]
                })
            del paper_doc["authors"]  # 논문 도큐먼트에서는 제외
        paper_docs.append(paper_doc)

    if paper_docs:
        bulk_insert_to_es(paper_docs, ES_HOST, ES_API, PAPER_INDEX)
    if author_docs:
        bulk_insert_authors_to_es(author_docs, ES_HOST, ES_API, AUTHOR_INDEX)

    print(f"batch start={start} ~ done")
    mongo_client.close()

print(f"전체 처리 완료")
