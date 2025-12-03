import itertools
import asyncio
import motor.motor_asyncio

sports_terms = [
    # 一、运动项目（综合类）
    "田径",
    "游泳",
    "篮球",
    "足球",
    "排球",
    "乒乓球",
    "羽毛球",
    "网球",
    "棒球",
    "橄榄球",
    "高尔夫球",
    "台球",
    "保龄球",
    "拳击",
    "摔跤",
    "柔道",
    "跆拳道",
    "空手道",
    "击剑",
    "射箭",
    "射击",
    "体操",
    "艺术体操",
    "蹦床",
    "举重",
    "自行车",
    "马术",
    "赛艇",
    "皮划艇",
    "帆船",
    "冲浪",
    "滑板",
    "滑雪",
    "滑冰",
    "冰球",
    "冰壶",
    "手球",
    "曲棍球",
    "水球",
    "橄榄球",
    "棒球",
    "垒球",
    "羽毛球",
    "乒乓球",
    "网球",
    "壁球",
    " squash",
    "高尔夫",
    "马术",
    "柔道",
    "跆拳道",
    "空手道",
    "拳击",
    "摔跤",
    "举重",
    "田径",
    "游泳",
    "跳水",
    "花样游泳",
    "水球",
    "赛艇",
    "皮划艇",
    "帆船",
    "冲浪",
    "滑板",
    "滑雪",
    "滑冰",
    "冰球",
    "冰壶",
    "手球",
    "曲棍球",
    "排球",
    "沙滩排球",
    "篮球",
    "3x3篮球",
    "足球",
    "五人制足球",
    "棒球",
    "垒球",
    "羽毛球",
    "乒乓球",
    "网球",
    "壁球",
    "高尔夫",
    "马术",
    "射箭",
    "射击",
    "体操",
    "艺术体操",
    "蹦床",
    "自行车",
    "公路自行车",
    "山地自行车",
    "场地自行车",
    "BMX小轮车",
]


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
    for batched_tuple in itertools.batched(sports_terms, 10000):
        keyword_with_page_total_77_coll.insert_many(
            [{"keyword": keyword} for keyword in batched_tuple]
        )


if __name__ == "__main__":
    asyncio.run(main())
