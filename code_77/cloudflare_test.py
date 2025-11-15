import asyncio
import itertools
from loguru import logger
import motor.motor_asyncio


async def get_async_ny_mongo_link(db_name: str, coll_name: str):
    mongo_uri = "mongodb://developer:QYZ3mxps4POMFb76@mongo-wy-hk.mereith.top:30017/?authSource=admin"
    mongo_client = motor.motor_asyncio.AsyncIOMotorClient(
        mongo_uri,
        maxPoolSize=200,
        minPoolSize=10,
        maxIdleTimeMS=60000,
    )
    await mongo_client.admin.command("ping")
    collection = mongo_client[db_name][coll_name]
    return collection


async def main():
    keyword_with_page_total_77_coll = await get_async_ny_mongo_link(
        "youtube_task_favortrain", "keyword_with_page_total_77"
    )
    filmont_url_77_coll = await get_async_ny_mongo_link(
        "youtube_task_favortrain", "filmont_url_77"
    )
    await filmont_url_77_coll.create_index("keyword")
    await filmont_url_77_coll.create_index("crawler_status")
    total_info_list = []
    _id_list = []
    async for mongo_info in keyword_with_page_total_77_coll.find(
        {"page_total": {"$ne": None}}, no_cursor_timeout=True
    ):
        if int(mongo_info["page_total"]) == 0:
            continue
        if mongo_info.get("status") == "success":
            continue
        page_total = 83
        if int(mongo_info["page_total"]) < 83:
            page_total = int(mongo_info["page_total"])
        _id_list.append(mongo_info["_id"])
        info_list = [
            {
                "keyword": mongo_info["keyword"],
                "page_index": page_index,
                "crawler_status": None,
            }
            for page_index in range(1, page_total + 1)
        ]
        total_info_list.extend(info_list)
    for batched_tuple in itertools.batched(total_info_list, 10000):
        await filmont_url_77_coll.insert_many(list(batched_tuple), ordered=False)
        logger.info(f"写入一批数据: {len(batched_tuple)}")

    for _id_batched_tupls in itertools.batched(_id_list, 1000):
        await keyword_with_page_total_77_coll.update_many(
            {"_id": {"$in": list(_id_batched_tupls)}}, {"$set": {"status": "success"}}
        )
        logger.info(f"完成一批数据的更新: {len(_id_batched_tupls)}")


if __name__ == "__main__":
    asyncio.run(main())
