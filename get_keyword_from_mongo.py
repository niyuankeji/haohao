import motor.motor_asyncio
from loguru import logger

def get__aws_mongo_link(db_name: str, coll_name: str):
    mongo_uri = "mongodb://root:nDq40tpoJ2kZ95Fo@ec2-13-214-244-12.ap-southeast-1.compute.amazonaws.com:30000/?authSource=admin"
    mongo_client = motor.motor_asyncio.AsyncIOMotorClient(mongo_uri, maxPoolSize=200, minPoolSize=10, maxIdleTimeMS=60000)
    collection = mongo_client[db_name][coll_name]
    logger.info(f"连接AWS-MongoDB成功「 {coll_name} 」")
    return collection



async def get_pending_ids():
    collection = get__aws_mongo_link("haohao_youtube", "id_keyword")
    cursor = collection.find({"status": {"$ne": 1}}, {"_id": 1})
    return [doc["_id"] async for doc in cursor]