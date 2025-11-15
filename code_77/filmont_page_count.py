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

    def close(self, is_print: bool = True, close_reason: str = ""):
        if is_print:
            logger.warning(close_reason)
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


async def get_cloudflare_cookie_from_cunzhang(
    target_url: str,
    resp_text: str,
    proxy: str,
    user_agent: str,
    key="64798cf2e0464b0192cf5d467b8d76d8",
):
    """从村长的接口中去获取cloudflare的cookie"""
    data = {
        "url": target_url,
        "html": resp_text,
        "proxy": proxy,
        "ua": user_agent,
        "type": "cookie",
        "key": key,
    }
    async with aiohttp.ClientSession() as session:
        async with session.post(
            url="http://ec2-13-212-87-87.ap-southeast-1.compute.amazonaws.com:13000/compute",
            json=data,
            allow_redirects=False,
            timeout=aiohttp.ClientTimeout(
                total=10 * 60, connect=15 * 4, sock_connect=15 * 4, sock_read=15 * 4
            ),
        ) as response:
            response.raise_for_status()
            resp_json = json.loads(await response.text())
            return resp_json


async def create_conn():
    conn_id = str(uuid.uuid4())[:8]
    url = "https://filmot.com/search/aaa/1/2?lang=en&gridView=1"
    retry_count = 0
    while True:
        try:
            user_agent = get_user_agent()
            headers = {
                "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
                "accept-language": "zh-CN,zh;q=0.9",
                "referer": "https://filmot.com/search/aaa/1/9?lang=en&gridView=1",
                "user-agent": user_agent,
            }
            proxy = f"http://td-customer-SOluI6kkrdk2-sessid-{generate_secure_random_string()}-sesstime-15:rEpTA530j0i6@43.153.55.54:9999"
            async with curl_requests.AsyncSession() as session:
                response = await session.get(
                    url=url,
                    headers=headers,
                    proxies={"http": proxy, "https": proxy},
                    impersonate="chrome",
                    allow_redirects=False,
                )
                if response.status_code == 403:
                    cf_resp_json = await get_cloudflare_cookie_from_cunzhang(
                        target_url=url,
                        resp_text=response.text,
                        proxy=proxy,
                        user_agent=user_agent,
                    )
                    print(f"cf_resp_json: {cf_resp_json}")
                    if cf_resp_json["success"]:
                        cookies = session.cookies.get_dict() or {}
                        cookies["cf_clearance"] = cf_resp_json["cf"]
                        user_agent = cf_resp_json["ua"]
                        logger.info(
                            f"[conn_id={conn_id} retry_count={retry_count}] pass cloudflare success"
                        )
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
                            f"[conn_id={conn_id} retry_count={retry_count}] 过5s盾失败"
                        )
                else:
                    logger.warning(
                        f"[conn_id={conn_id} retry_count={retry_count}] 请求出来的结果不是403"
                    )
        except Exception as e:
            logger.error(f"create_conn error: {e.__class__.__name__}")
        finally:
            retry_count += 1


class ResearchgateConnectionStrategy(ConnectionStrategy[CloudflareConn]):
    async def make_connection(self) -> CloudflareConn:
        async with cloudflare_sem:
            return await create_conn()

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

stub: ConnectionPool = ConnectionPool(
    strategy=ResearchgateConnectionStrategy(), max_size=100
)


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
    url = f"https://filmot.com/search/{mongo_info['keyword']}/1/{page_index}?lang={lang}&gridView=1"  # 这里page_index（1-83）
    while retry_count:
        conn: CloudflareConn
        async with stub.get_connection() as conn:
            try:
                headers = {
                    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
                    "accept-language": "zh-CN,zh;q=0.9",
                    "referer": f"https://filmot.com/search/{mongo_info['keyword']}/1/1?lang={lang}&gridView=1",
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
                                int(convert_number(sticky.split("clips")[0]) / 60) + 1
                            )
                        else:
                            page_num = int(int(sticky.split("clips")[0]) / 60) + 1
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
