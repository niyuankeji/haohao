import motor.motor_asyncio
from loguru import logger
from pymongo import UpdateOne
import asyncio

def get__aws_mongo_link(db_name: str, coll_name: str):
    mongo_uri = "mongodb://root:nDq40tpoJ2kZ95Fo@ec2-13-214-244-12.ap-southeast-1.compute.amazonaws.com:30000/?authSource=admin"
    mongo_client = motor.motor_asyncio.AsyncIOMotorClient(mongo_uri, maxPoolSize=200, minPoolSize=10, maxIdleTimeMS=60000)
    collection = mongo_client[db_name][coll_name]
    logger.info(f"连接AWS-MongoDB成功「 {coll_name} 」")
    return collection

async def main():
    collection = get__aws_mongo_link("haohao_youtube", "id_keyword")
    BATCH_SIZE = 1000
    with open("keyword.txt", "r", encoding="utf-8") as f:
        keywords = [line.strip() for line in f if line.strip()]
    for i in range(0, len(keywords), BATCH_SIZE):
        batch = keywords[i:i + BATCH_SIZE]
        ops = [UpdateOne({"_id": kw}, {"$setOnInsert": {"status": 0}}, upsert=True) for kw in batch]
        await collection.bulk_write(ops, ordered=False)

if __name__ == "__main__":
    asyncio.run(main())