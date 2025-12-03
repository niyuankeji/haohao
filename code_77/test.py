import itertools
import asyncio
import motor.motor_asyncio

sports_terms = [
    "Athletics",
    "Swimming",
    "Basketball",
    "Football",
    "Volleyball",
    "Table Tennis",
    "Badminton",
    "Tennis",
    "Baseball",
    "Rugby",
    "Golf",
    "Boxing",
    "Wrestling",
    "Judo",
    "Taekwondo",
    "Karate",
    "Fencing",
    "Archery",
    "Shooting",
    "Gymnastics",
    "Artistic Gymnastics",
    "Trampoline",
    "Weightlifting",
    "Cycling",
    "Equestrian",
    "Rowing",
    "Canoeing",
    "Kayaking",
    "Sailing",
    "Surfing",
    "Skateboarding",
    "Skiing",
    "Snowboarding",
    "Ice Hockey",
    "Figure Skating",
    "Speed Skating",
    "Curling",
    "Handball",
    "Field Hockey",
    "Water Polo",
    "3x3 Basketball",
    "Futsal",
    "Softball",
    "Squash",
    "Racquetball",
    "BMX",
    "Triathlon",
    "Marathon",
    "Sprint",
    "Hurdles",
    "Long Jump",
    "High Jump",
    "Pole Vault",
    "Shot Put",
    "Discus Throw",
    "Javelin Throw",
    "Hammer Throw",
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
