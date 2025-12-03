import os
import sys
import random
import datetime
import uuid
import asyncio
import string
import secrets
import json
from dataclasses import dataclass, field
import aiohttp
import motor.motor_asyncio
from curl_cffi import requests as curl_requests
from loguru import logger
from lxml import etree
from aiostream import pipe, stream

sys.path.append(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)

from pools import ConnectionStrategy, ConnectionPool

cloudflare_sem = asyncio.Semaphore(20)


@dataclass
class CloudflareConn:
    conn_id: str
    success_count: int
    proxy: str
    cookies: dict
    user_agent: str
    use_datetime: datetime.datetime
    is_close: bool = field(default=False)

    def close(self):
        self.is_close = True


def generate_secure_random_string(min_length=12, max_length=16):
    length = random.randint(min_length, max_length)
    characters = string.ascii_letters + string.digits
    return "".join(secrets.choice(characters) for i in range(length))


def get_user_agent():
    return (
        random.choice(
            [
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
                "Mozilla/5.0 (X11; Linux x86_64)",
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
            ]
        )
        + " "
        + random.choice(
            [
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36",
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36",
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36 Edg/130.0.0.0",
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0",
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36 Edg/132.0.0.0",
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36 Edg/133.0.0.0",
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36 Edg/134.0.0.0",
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36 Edg/135.0.0.0",
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36 Edg/136.0.0.0",
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36 Edg/137.0.0.0",
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36 Edg/138.0.0.0",
            ]
        )
    )


m_session = "eyJpdiI6ImgreEZobGxTSkcvMDNxbXpkNnRCQnc9PSIsInZhbHVlIjoiMkVIZGF3M3lmYkZzUTl2MUEzUGtqZzMzZ1Y1NzBiOVBIbEl4b0Vacmc5N2N1RnFRQ2FoZStremhBRWlZVkZlSi9ieTFJNVJYZkNlNkl5R1NVVTdpNEVuemJNV1NvTlVGOC8wbDZvY2FUdEF5Y1RJUEtmb09MWGZhODZpT0ZzS0ciLCJtYWMiOiIxZTRkMjMxZTJjMjNkMmMzYWFlZmI3MTk4Y2M2MjQyOTdlZmRjYzE3ZGRlN2ZlZThiY2Y5Y2FiNDJlZTgyYjUzIiwidGFnIjoiIn0%3D"


async def get_cloudflare_cookie_from_ezcaptcha(
    target_url: str,
    proxy: str,
    key="0c77fa69aaab42a0a298b3accbff9f7a193959",
):
    """从ez_captcha平台中获取cookie"""
    headers = {"Content-Type": "application/json"}
    task_data = {
        "clientKey": key,
        "task": {
            "websiteURL": target_url,
            "type": "CloudFlare5STask",
            "proxy": proxy,
            "rqData": {"cookie": {"m_session": m_session}},
        },
    }
    async with aiohttp.ClientSession() as session:
        async with session.post(
            url="https://api.ez-captcha.com/createTask",
            json=task_data,
            headers=headers,
            allow_redirects=False,
            timeout=aiohttp.ClientTimeout(
                total=10 * 60, connect=15, sock_connect=15, sock_read=15
            ),
        ) as task_response:
            task_response.raise_for_status()
            task_resp_json = json.loads(await task_response.text())
            task_id = task_resp_json["taskId"]
            for _ in range(15):
                result_data = {"clientKey": key, "taskId": task_id}
                async with session.post(
                    url="https://api.ez-captcha.com/getTaskResult",
                    json=result_data,
                    headers=headers,
                    allow_redirects=False,
                    timeout=aiohttp.ClientTimeout(
                        total=10 * 60, connect=15, sock_connect=15, sock_read=15
                    ),
                ) as result_response:
                    result_resp_text = await result_response.text()
                    if "header" in result_resp_text:
                        result_resp_json = json.loads(result_resp_text)
                        return result_resp_json
                await asyncio.sleep(3)


async def create_conn_from_hcaptcha():
    conn_id = str(uuid.uuid4())[:8]
    url = "https://filmot.com/search/aaa/1/2?gridView=1&"
    retry_count = 0
    while True:
        try:
            user_agent = get_user_agent()
            headers = {
                "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
                "accept-language": "zh-CN,zh;q=0.9",
                "referer": "https://filmot.com/search/aaa/1/9?gridView=1&",
                "user-agent": user_agent,
            }
            proxy = f"http://welib_77-zone-adam507066-session-{generate_secure_random_string(6, 16)}-sesstime-10:welib_77@2ax1q1v2c6n7-as.ipidea.online:2333"
            async with curl_requests.AsyncSession() as session:
                response = await session.get(
                    url=url,
                    headers=headers,
                    proxies={"http": proxy, "https": proxy},
                    impersonate="chrome",
                    allow_redirects=False,
                )
                if response.status_code == 403:
                    cloudflare_cookie_info = await get_cloudflare_cookie_from_ezcaptcha(
                        target_url=url,
                        proxy=proxy,
                    )
                    if (not cloudflare_cookie_info) or (
                        cloudflare_cookie_info["status"] != "ready"
                    ):
                        logger.warning(
                            "[create_conn_from_hcaptcha] cloudflare_cookie_info出现了错误的回显"
                        )
                        continue
                    cookies = cloudflare_cookie_info["solution"]["cookies"]
                    user_agent = cloudflare_cookie_info["solution"]["header"][
                        "user-agent"
                    ]
                    return CloudflareConn(
                        conn_id=conn_id,
                        success_count=0,
                        proxy=proxy,
                        cookies=cookies,
                        user_agent=user_agent,
                        use_datetime=datetime.datetime.now(),
                    )
                else:
                    logger.warning(
                        f"[conn_id={conn_id} retry_count={retry_count}] 请求出来的结果不是403"
                    )
        except Exception as e:
            logger.error(f"create_conn error: {e.__class__.__name__}")
        finally:
            retry_count += 1


class FilmontConnectionStrategy(ConnectionStrategy[CloudflareConn]):
    async def make_connection(self) -> CloudflareConn:
        async with cloudflare_sem:
            return await create_conn_from_hcaptcha()

    def connection_is_closed(self, conn: CloudflareConn) -> bool:
        return conn.is_close

    async def close_connection(self, conn: CloudflareConn) -> None:
        conn.close()


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


###########################################################################################

stub: ConnectionPool = ConnectionPool(strategy=FilmontConnectionStrategy(), max_size=1)


def convert_number(s):
    s = s.strip().upper()
    if s.endswith("K"):
        return float(s[:-1]) * 1000
    elif s.endswith("M"):
        return float(s[:-1]) * 1_000_000
    elif s.endswith("B"):
        return float(s[:-1]) * 1_000_000_000
    else:
        return float(s)


async def get_page_num(mongo_info, page_index=1, lang="en"):
    retry_count = 3
    url = f"https://filmot.com/search/{mongo_info['keyword']}/1/{page_index}?gridView=1&"  # 这里page_index（1-83）
    while retry_count:
        conn: CloudflareConn
        async with stub.get_connection() as conn:
            try:
                headers = {
                    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
                    "accept-language": "zh-CN,zh;q=0.9",
                    "referer": f"https://filmot.com/search/{mongo_info['keyword']}/1/1?gridView=1&",
                    "user-agent": conn.user_agent,
                }
                async with curl_requests.AsyncSession() as session:
                    response = await session.get(
                        url=url,
                        headers=headers,
                        cookies=conn.cookies,
                        impersonate="chrome",
                        timeout=30,
                        proxies={"http": conn.proxy, "https": conn.proxy},
                        allow_redirects=False,
                    )
                    response.raise_for_status()
                    e = etree.HTML(response.text)
                    try:
                        sticky = "".join(e.xpath('//*[@id="resultTop"]/div/text()'))
                    except:
                        sticky = ""
                    try:
                        if "No" in sticky:
                            return mongo_info, 0
                        elif "M" in sticky or "K" in sticky or "B" in sticky:
                            page_num = (
                                int(
                                    convert_number(sticky.split("clips", maxsplit=1)[0])
                                    / 60
                                )
                                + 1
                            )
                        else:
                            page_num = (
                                int(int(sticky.split("clips", maxsplit=1)[0]) / 60) + 1
                            )
                        return mongo_info, page_num
                    except Exception as e:
                        logger.error(f"失败,原因是{e}")
                        return mongo_info, 0
            except Exception as e:
                logger.error(f"fetch_html出现错误: {e.__class__.__name__} {url}")
                conn.close()
            finally:
                retry_count -= 1
    return mongo_info, None


async def producer(mongo_coll):
    async for mongo_info in mongo_coll.find(
        {"page_total": None}, no_cursor_timeout=True
    ):
        yield mongo_info


async def main():
    keyword_with_page_total_77_coll = await get_async_ny_mongo_link(
        "youtube_task_favortrain", "keyword_with_page_total_77"
    )

    async def deal_fetch_html_result(mongo_info, page_num):
        await keyword_with_page_total_77_coll.update_one(
            {"keyword": mongo_info["keyword"]}, {"$set": {"page_total": page_num}}
        )

    await (
        stream.iterate(producer(keyword_with_page_total_77_coll))
        | pipe.map(get_page_num, task_limit=100, ordered=False)
        | pipe.starmap(deal_fetch_html_result, task_limit=100, ordered=False)
    )


if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(main())
