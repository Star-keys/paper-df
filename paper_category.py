import psycopg2
from pymongo import MongoClient
import spacy
import json

# Spacy 모델 적재
nlp_bc5 = spacy.load("./models/en_ner_bc5cdr_md-0.5.4/en_ner_bc5cdr_md/en_ner_bc5cdr_md-0.5.4/")
nlp_bio = spacy.load("./models/en_ner_bionlp13cg_md-0.5.4/en_ner_bionlp13cg_md/en_ner_bionlp13cg_md-0.5.4/")

# PostgreSQL 연결
conn = psycopg2.connect(
    dbname="postgres",
    user="",
    password="",
    host="",
    port=""
)
cur = conn.cursor()
cur.execute("""
CREATE TABLE IF NOT EXISTS starkeys.category (
    id SERIAL PRIMARY KEY,
    paper_id varchar(100),
    entity_text varchar(100),
    entity_type varchar(100),
    count INT DEFAULT 1
);
""")
conn.commit()

# MongoDB 연결 (timeout 최대치 조정)
mongo_client = MongoClient(
    "",
    socketTimeoutMS=3600*1000*10,    # 10시간
    connectTimeoutMS=3600*1000*5,    # 5시간
    serverSelectionTimeoutMS=3600*1000*5
)
mongo_db = mongo_client["starkeys"]
papers_col = mongo_db["paper_json"]

def merge_ents(doc_text, ents_bc5, ents_bio):
    spans = []
    for e in ents_bc5:
        spans.append((e.start_char, e.end_char, e.label_, "bc5cdr"))
    for e in ents_bio:
        spans.append((e.start_char, e.end_char, e.label_, "bionlp13cg"))
    spans.sort(key=lambda x: (x[0], -(x[1]-x[0])))
    merged = []
    for sp in spans:
        if not merged:
            merged.append(sp)
            continue
        last = merged[-1]
        if not (last[1] <= sp[0] or sp[1] <= last[0]):
            # overlap
            merged[-1] = last if (last[1]-last[0]) >= (sp[1]-sp[0]) else sp
        else:
            merged.append(sp)
    results = []
    for s, e, label, _src in merged:
        text = doc_text[s:e]
        results.append((text, label, s, e))
    return results

print("MongoDB에서 논문 JSON 전체 적재 후 NER 시작...")
cnt = 0
for doc in papers_col.find({}, {"_id": 1, "bioc_json": 1}).sort("_id", 1):
    pmc_id = doc["_id"]

    # bioc_json 파싱
    bioc_data = doc.get("bioc_json")
    if isinstance(bioc_data, str):
        try:
            bioc_data = json.loads(bioc_data)
        except Exception as e:
            print(f"{pmc_id} json 변환 실패: {e}")
            continue

    # PostgreSQL에 이미 존재하면 SKIP
    cur.execute("SELECT 1 FROM starkeys.category WHERE paper_id = %s LIMIT 1", (pmc_id,))
    exists = cur.fetchone()
    if exists:
        print(f"SKIP: {pmc_id} - already in DB.")
        continue

    entity_map = {}
    for passage in bioc_data[0]["documents"][0]["passages"]:
        text = passage.get("text", "")
        if not text:
            continue

        doc_bc5 = nlp_bc5(text)
        doc_bio = nlp_bio(text)
        merged = merge_ents(text, doc_bc5.ents, doc_bio.ents)

        for ent_text, ent_label, s, e in merged:
            key = (ent_text.lower(), ent_label)
            if key not in entity_map:
                entity_map[key] = {
                    "paper_id": pmc_id[:100],
                    "entity_text": ent_text.lower()[:100],
                    "entity_type": ent_label[:100],
                    "count": 1
                }
            else:
                entity_map[key]["count"] += 1

    batch = sorted(entity_map.values(), key=lambda x: x["count"], reverse=True)[:10]

    if batch:
        args_str = ",".join(
            cur.mogrify("(%s,%s,%s,%s)", (
                e["paper_id"], e["entity_text"], e["entity_type"], e["count"]
            )).decode("utf-8") for e in batch)
        cur.execute("INSERT INTO starkeys.category (paper_id, entity_text, entity_type, count) VALUES " + args_str)
        conn.commit()

    cnt += 1
    print(f"--------------{pmc_id} : {cnt} 건 처리완료--------------")

cur.close()
conn.close()
print("✅ MongoDB 기반 category 테이블 적재 완료.")
