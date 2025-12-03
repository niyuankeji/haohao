import itertools
import asyncio
import motor.motor_asyncio

sports_terms = [
    "Football Boot", "Basketball Shoe", "Running Shoe", "Badminton Racket",
    "Table Tennis Racket", "Tennis Racket", "Golf Club", "Baseball Bat",
    "Softball Bat", "Fencing Sword", "Archery Bow", "Arrow", "Shooting Rifle",
    "Gymnastics Mat", "Weightlifting Barbell", "Dumbbell", "Bicycle", "Rowing Oar",
    "Canoe Paddle", "Sail", "Ski", "Snowboard", "Ski Pole", "Ice Skate",
    "Ice Hockey Stick", "Curling Stone", "Volleyball Net", "Basketball Hoop",
    "Football Goal", "Tennis Net", "Ping Pong Table", "Billards Table",
    "Bowling Pin", "Boxing Glove", "Wrestling Singlet", "Judo Gi", "Taekwondo Dobok",
    "Swim Cap", "Swim Goggles", "Swimsuit", "Lifebuoy", "Diving Board", "Water Polo Ball",
    "Field Hockey Stick", "Handball", "Squash Racket", "Golf Ball", "Baseball",
    "Softball", "Shuttlecock", "Table Tennis Ball", "Tennis Ball", "Hockey Ball",
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
    keyword_with_page_total_77_coll.create_index("keyword", unique=True)
    for batched_tuple in itertools.batched(sports_terms, 10000):
        keyword_with_page_total_77_coll.insert_many(
            [{"keyword": keyword} for keyword in batched_tuple]
        )


async def main2():
    keyword_with_page_total_77_coll = await get_async_ny_mongo_link(
        "youtube_task_favortrain", "keyword_with_page_total_77"
    )
    filmont_url_77_coll = await get_async_ny_mongo_link(
        "youtube_task_favortrain", "filmont_url_77"
    )
    task_list = []
    async for mongo_info in keyword_with_page_total_77_coll.find({}):
        for i in range(1, 84):
            task_list.append(
                {
                    "keyword": mongo_info["keyword"],
                    "page_index": i,
                    "crawler_status": None,
                }
            )
    for batched_tuple in itertools.batched(task_list, 10000):
        await filmont_url_77_coll.insert_many(list(batched_tuple), ordered=False)
        print("完成一批")


if __name__ == "__main__":
    asyncio.run(main())
