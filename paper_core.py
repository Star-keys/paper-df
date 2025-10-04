from pymongo import MongoClient
import requests
import csv
import json
import re


def get_mongo_client():
    return MongoClient() # mogoDB url

mongo_client = get_mongo_client()
mongo_db = mongo_client["starkeys"]
papers_col = mongo_db["paper_json"]

def extract_pmc_id_from_link(link):
    m = re.search(r'PMC\d+', str(link))
    return m.group(0) if m else None

pmc_ids = set()
with open("./SB_publication_PMC.csv", newline='', encoding='utf-8') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        pmc_id = extract_pmc_id_from_link(row.get("Link", ""))
        if pmc_id:
            pmc_ids.add(pmc_id)

print("----------------- " + str(len(pmc_ids)) + " 건 추출 시작 -----------------")
cnt = 0
err_ids = set()

for pmc_id in pmc_ids:
    # 1000건마다 mongo 커넥션 새로 생성
    if cnt % 200 == 0 and cnt > 0:
        mongo_client.close()
        mongo_client = get_mongo_client()
        mongo_db = mongo_client["starkeys"]
        papers_col = mongo_db["paper_json"]

    url = f"https://www.ncbi.nlm.nih.gov/research/bionlp/RESTful/pmcoa.cgi/BioC_json/{pmc_id}/unicode"
    response = requests.get(url)

    if response.status_code != 200:
        print(f"API 요청 실패 {pmc_id} 상태 코드 {response.status_code}")
        err_ids.add([pmc_id, f"status_code_{response.status_code}"])
        cnt += 1
        continue

    try:
        bioc_data = response.json()
        paper_id = bioc_data[0]["documents"][0].get("id", "PMC_UNKNOWN")
        json.dumps(bioc_data)
    except Exception as e:
        print("JSON 디코딩 실패:", e)
        print("실제 응답:", response.text)
        err_ids.add(pmc_id)
        cnt += 1
        continue

    # MongoDB 적재 시 커넥션 에러 발생 시 재연결
    try:
        papers_col.update_one(
            {"_id": pmc_id},
            {"$set": {"bioc_json": json.dumps(bioc_data)}},
            upsert=True
        )
    except Exception as e:
        print("MongoDB 적재 실패:", e)
        mongo_client = get_mongo_client()
        mongo_db = mongo_client["starkeys"]
        papers_col = mongo_db["paper_json"]
        err_ids.add(pmc_id)
        cnt += 1
        continue

    cnt += 1
    print("--------------" + pmc_id + " : " + str(cnt) + " 건 처리완료 --------------")

print("실패 id 총 개수:", len(err_ids))

# 실패한 pmc_id를 CSV로 저장
with open("./err_ids.csv", "w", newline='', encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["pmc_id"])  # 헤더
    writer.writerows(err_ids)
print("실패 id list를 err_ids.csv에 저장 완료.")
