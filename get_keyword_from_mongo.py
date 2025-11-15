from pymongo import MongoClient
from loguru import logger


def get_aws_mongo_link_sync(db_name: str, coll_name: str):
    """
    同步连接 AWS MongoDB，返回 pymongo Collection 实例
    """
    mongo_uri = "mongodb://root:nDq40tpoJ2kZ95Fo@ec2-13-214-244-12.ap-southeast-1.compute.amazonaws.com:30000/?authSource=admin"

    # 同步客户端 + 连接池配置
    client = MongoClient(
        mongo_uri,
        maxPoolSize=200,
        minPoolSize=10,
        maxIdleTimeMS=60000,
        serverSelectionTimeoutMS=5000,  # 连接超时
        connectTimeoutMS=10000,
        socketTimeoutMS=10000
    )

    collection = client[db_name][coll_name]
    logger.info(f"同步连接 AWS-MongoDB 成功「{db_name}.{coll_name}」")

    return collection


def get_pending_ids_sync(n):
    """
    同步获取 status != 1 的 _id 列表
    """
    collection = get_aws_mongo_link_sync("haohao_youtube", "id_keyword_bark")
    cursor = collection.find({"list": int(n), "status": {"$ne": 1}}, {"_id": 1})

    # 直接同步迭代
    return [doc["_id"] for doc in cursor]
