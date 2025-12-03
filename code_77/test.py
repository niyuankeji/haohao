import itertools
import asyncio
import motor.motor_asyncio

sports_terms = [
    # 三、赛事相关术语
    "Olympics", "Paralympics", "Asian Games", "World Cup", "European Championship",
    "Asian Cup", "Champions League", "Premier League", "La Liga", "Bundesliga",
    "Serie A", "Ligue 1", "NBA", "CBA", "AFC Champions League", "FIFA World Cup",
    "Tournament", "Championship", "Invitation Tournament", "Friendly Match",
    "Qualifiers", "Playoffs", "Knockout Stage", "Group Stage", "Semi-Final",
    "Final", "Quarter-Final", "Round of 16", "Preliminary Round", "Relegation",
    "Promotion", "League", "Cup", "Derby", "Home Game", "Away Game", "Neutral Venue",
    "Extra Time", "Penalty Shootout", "Sudden Death", "Overtime", "Halftime",
    " Injury Time", "Stoppage Time", "Fixture", "Schedule", "Ranking", "Standings",
    
    # 四、规则与判定术语
    "Foul", "Yellow Card", "Red Card", "Offside", "Handball", "VAR", "Referee",
    "Linesman", "Umpire", "Decision", "Review", "Overturn", "Appeal", "Suspension",
    "Ban", "Disqualification", "Forfeiture", "Draw", "Win", "Loss", "Tie",
    "Draw", "Victory", "Defeat", "Goal", "Assist", "Own Goal", "Penalty",
    "Free Kick", "Corner", "Throw-In", "Goal Kick", "Kick-Off", "Substitution",
    "Bench", "Starting XI", "Squad", "Captain", "Injury", "Comeback", "Upset",
    "Hat-Trick", "Brace", "Clean Sheet", "Shutout", "Double", "Treble",
    
    # 五、器材装备术语
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
    
    # 六、职位与角色术语
    "Athlete", "Player", "Coach", "Assistant Coach", "Head Coach", "Referee",
    "Umpire", "Linesman", "Line Judge", "Scorekeeper", "Timer", "Recorder",
    "Team Manager", "Physiotherapist", "Sports Doctor", "Fitness Coach",
    "Technical Coach", "Goalkeeper Coach", "Offensive Coach", "Defensive Coach",
    "Agent", "Tournament Director", "Referee Supervisor", "Arbitrator",
    "Volunteer", "Commentator", "Pundit", "Photographer", "Journalist",
    "Captain", "Vice-Captain", "Striker", "Midfielder", "Defender", "Goalkeeper",
    "Winger", "Center Back", "Full Back", "Center Forward", "Playmaker",
    "Defensive Midfielder", "Attacking Midfielder",
    
    # 七、战术与策略术语
    "Defense", "Offense", "Counterattack", "Pressing", "Zone Defense", "Man-to-Man Defense",
    "Offside Trap", "Set Piece", "Free Kick Routine", "Penalty Strategy",
    "Substitution Tactics", "Formation", "4-4-2", "4-3-3", "3-5-2", "5-3-2",
    "Tiki-Taka", "Long Ball", "Possession Play", "Counter-Press", "Wing Play",
    "Central Play", "Defensive Block", "Attacking Wave", "Fast Break", "Half-Court Offense",
    "Full-Court Press", "Zone Press", "Screen", "Pick and Roll", "Give and Go",
    "Overlap", "Underlap", "Switch Play"
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
    await keyword_with_page_total_77_coll.create_index("keyword", unique=True)
    for batched_tuple in itertools.batched(sports_terms, 10000):
        try:
            await keyword_with_page_total_77_coll.insert_many(
                [{"keyword": keyword} for keyword in batched_tuple],
                ordered=False
            )
        except Exception as e:
            print(e.__class__.__name__)


async def main2():
    keyword_with_page_total_77_coll = await get_async_ny_mongo_link(
        "youtube_task_favortrain", "keyword_with_page_total_77"
    )
    filmont_url_77_coll = await get_async_ny_mongo_link(
        "youtube_task_favortrain", "filmont_url_77"
    )
    task_list = []
    async for mongo_info in keyword_with_page_total_77_coll.find(
        {"crawler_status": None}
    ):
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
