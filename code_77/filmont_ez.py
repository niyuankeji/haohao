import os
import sys
import random
import datetime
import uuid
import asyncio
import string
import secrets
import json
import time
import re
from dataclasses import dataclass, field
import aiohttp
from redis.asyncio import Redis as aioRedis
import pymongo
import pytz
import motor.motor_asyncio
from curl_cffi import requests as curl_requests
from loguru import logger
from lxml import etree
from aiostream import pipe, stream
from motor.motor_asyncio import AsyncIOMotorCollection as TypeAioCollection

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
            "rqData": {
                "cookie": {
                    "m_session": "eyJpdiI6InozUlIvZDFSOUNydHozZ1M1aldkM0E9PSIsInZhbHVlIjoiNUd5NFl5TDF5UlNJR2poRDJ2Tmp4WmZjeEQ0Q2NnczhRbzgvdXpGRlZXbGNFQXNuVWdaZFBuY29yMW5sbXJSbTZRUnc3RExxcHROWGc3ZWRaT3liTGVkaW81L0xpK3E3MkdzcG5raHRyMWoxWDBucitjWE5FaDB6VVpwc2RjSXgiLCJtYWMiOiI0MTkyMWFhMDI5N2QwMWI4NDhhOWVkMTY5MGZmYWFiNzBhNjVkMDBjYTBmNGI0Y2Q2MmZkOWIzOTRiOThhZjVmIiwidGFnIjoiIn0%3D",
                }
            },
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


async def create_conn_from_ez():
    conn_id = str(uuid.uuid4())[:8]
    url = "https://filmot.com/search/aaa/1/2?gridView=1&lang=en"
    retry_count = 0
    while True:
        try:
            user_agent = get_user_agent()
            headers = {
                "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
                "accept-language": "zh-CN,zh;q=0.9",
                "referer": "https://filmot.com/search/aaa/1/9?gridView=1&lang=en",
                "user-agent": user_agent,
            }
            proxy = f"http://td-customer-SOluI6kkrdk2-sessid-{generate_secure_random_string()}-sesstime-15:rEpTA530j0i6@43.153.55.54:9999"
            cookies = {
                "m_session": "eyJpdiI6InozUlIvZDFSOUNydHozZ1M1aldkM0E9PSIsInZhbHVlIjoiNUd5NFl5TDF5UlNJR2poRDJ2Tmp4WmZjeEQ0Q2NnczhRbzgvdXpGRlZXbGNFQXNuVWdaZFBuY29yMW5sbXJSbTZRUnc3RExxcHROWGc3ZWRaT3liTGVkaW81L0xpK3E3MkdzcG5raHRyMWoxWDBucitjWE5FaDB6VVpwc2RjSXgiLCJtYWMiOiI0MTkyMWFhMDI5N2QwMWI4NDhhOWVkMTY5MGZmYWFiNzBhNjVkMDBjYTBmNGI0Y2Q2MmZkOWIzOTRiOThhZjVmIiwidGFnIjoiIn0%3D",
            }
            async with curl_requests.AsyncSession() as session:
                response = await session.get(
                    url=url,
                    headers=headers,
                    cookies=cookies,
                    proxies={"http": proxy, "https": proxy},
                    impersonate="chrome",
                    allow_redirects=False,
                )
                if response.status_code == 403:
                    cf_resp_json = await get_cloudflare_cookie_from_ezcaptcha(
                        target_url=url,
                        proxy=proxy,
                    )
                    if cf_resp_json and cf_resp_json["errorId"] == 0:
                        cookies.update(session.cookies.get_dict() or {})
                        cookies.update(cf_resp_json["solution"]["cookies"])
                        user_agent = cf_resp_json["solution"]["header"]["user-agent"]
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
                elif response.status_code == 200:
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
            logger.error(f"create_conn_from_ez error: {e.__class__.__name__}")
        finally:
            retry_count += 1


sem = asyncio.Semaphore(1)


async def get_hcaptcha_cookie():
    retry_count = 1
    while True:
        try:
            user_agent = get_user_agent()
            proxy = f"http://td-customer-SOluI6kkrdk2-sessid-{generate_secure_random_string()}-sesstime-5:rEpTA530j0i6@43.153.55.54:9999"
            async with curl_requests.AsyncSession() as session:
                response = await session.get(
                    url="https://filmot.com/search/aaa/1/2",
                    params={"gridView": "1", "lang": "en"},
                    headers={
                        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
                        "accept-language": "zh-CN,zh;q=0.9",
                        "cache-control": "no-cache",
                        "pragma": "no-cache",
                        "user-agent": user_agent,
                    },
                    proxies={"http": proxy, "https": proxy},
                )
                _token = re.findall(
                    r'<input type="hidden" name="_token" value="(.*?)">',
                    response.text,
                )[0]
                data = {
                    "sitekey": "58b0f6cd-815d-4d93-aad6-d80c7d56a8aa",
                    "referer": "https://filmot.com/captcha-verify",
                    "rqdata": "",
                    "user_agent": user_agent,
                }
                data = json.dumps(data, separators=(",", ":"))
                response = await session.post(
                    url="http://api.nocaptcha.io/api/wanda/hcaptcha/universal",
                    data=data,
                    headers={
                        "Accept": "*/*",
                        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
                        "Content-Type": "application/json",
                        "User-Agent": user_agent,
                        "User-Token": "f77c1828-d256-49f8-adfd-e634c82a71c8",
                    },
                )
                resp_json = json.loads(response.text)
                if resp_json["msg"] != "验证成功":
                    continue
                headers = {
                    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
                    "accept-language": "zh-CN,zh;q=0.9,en;q=0.8",
                    "cache-control": "no-cache",
                    "content-type": "application/x-www-form-urlencoded",
                    "origin": "https://filmot.com",
                    "referer": "https://filmot.com/captcha-verify",
                    "user-agent": user_agent,
                }
                response = await session.post(
                    url="https://filmot.com/captcha-validate",
                    data={
                        "_token": _token,
                        "g-recaptcha-response": resp_json["data"][
                            "generated_pass_UUID"
                        ],
                        "h-captcha-response": resp_json["data"]["generated_pass_UUID"],
                    },
                    headers=headers,
                    proxies={"http": proxy, "https": proxy},
                )
                if response.status_code != 200:
                    continue
                return session.cookies.get_dict()
        except Exception as e:
            logger.error(f"get_hcaptcha_cookie error: {e.__class__.__name__}")
        finally:
            logger.info(f"get_hcaptcha_cookie尝试第{retry_count}结束")
            retry_count += 1


async def create_conn_from_hcaptcha():
    async with sem:
        hcaptcha_cookie_coll = await hcaptcha_db.get_db()
        mongo_info = await (await hcaptcha_db.get_db()).find_one(
            {"cookie": {"$ne": None}}
        )
        if not mongo_info:
            cookie_info = await get_hcaptcha_cookie()
            await hcaptcha_cookie_coll.insert_one({"cookie": cookie_info})
        else:
            cookie_info = mongo_info["cookie"]
    cookies = cookie_info
    user_agent = get_user_agent()
    conn_id = str(uuid.uuid4())[:8]
    proxy = f"http://td-customer-SOluI6kkrdk2-sessid-{generate_secure_random_string()}-sesstime-5:rEpTA530j0i6@43.153.55.54:9999"
    return CloudflareConn(
        conn_id=conn_id,
        success_count=0,
        proxy=proxy,
        cookies=cookies,
        user_agent=user_agent,
        use_datetime=datetime.datetime.now(),
    )


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


async def get_aio_redis_client():
    try:
        redis_client = aioRedis(
            host="redis-wy-hk.mereith.top",
            port=30079,
            username="",
            password="QYZ3mxps4POMFb76",
            db=5,
            decode_responses=True,
        )
        await redis_client.ping()
        logger.info("[ConfigDB] Redis连接成功")
        return redis_client
    except Exception as err:
        logger.error(f"[ConfigDB] Redis连接失败: {err}")
    return None


class HcaptchaDB:
    def __init__(self):
        self.db_name = "hcaptcha_cookie"
        self.db: TypeAioCollection = None

    async def get_db(self):
        if not self.db:
            self.db = await get_async_ny_mongo_link("google-scholar", self.db_name)
        return self.db


hcaptcha_db = HcaptchaDB()


###########################################################################################

stub: ConnectionPool = ConnectionPool(
    strategy=FilmontConnectionStrategy(), max_size=200
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


async def get_youtube_key_list(mongo_info, lang="en"):
    retry_count = 3
    url = f"https://filmot.com/search/{mongo_info['keyword']}/1/{mongo_info['page_index']}?gridView=1&lang={lang}"  # 这里page_index（1-83）
    while retry_count:
        conn: CloudflareConn
        async with stub.get_connection() as conn:
            try:
                headers = {
                    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
                    "accept-language": "zh-CN,zh;q=0.9",
                    "referer": f"https://filmot.com/search/{mongo_info['keyword']}/1/1?gridView=1&lang={lang}",
                    "user-agent": conn.user_agent,
                }
                start_time = time.time()
                async with curl_requests.AsyncSession() as session:
                    response = await session.get(
                        url=url,
                        headers=headers,
                        cookies=conn.cookies,
                        impersonate="chrome",
                        timeout=60,
                        proxies={"http": conn.proxy, "https": conn.proxy},
                        allow_redirects=False,
                    )
                    if response.status_code == 200:
                        e = etree.HTML(response.text)
                        trs = e.xpath("//div[contains(@id, 'vcard')]/a[2]/@href")
                        info_list = []
                        for tr in trs:
                            youtube_key = re.findall("=(.*?)&", "".join(tr))[0]
                            info_list.append(
                                {
                                    "_id": youtube_key,
                                    "dest_path": "week1/English/",
                                    "download_status": 4,
                                    "upload_status": 4,
                                }
                            )
                        logger.info(
                            f"[get_youtube_key_list keyword={mongo_info['keyword']} page_index={mongo_info['page_index']}] 成功拿到结果花费时间: {(time.time() - start_time):.2f}"
                        )
                        return mongo_info, info_list
                    elif response.status_code == 403:
                        conn.close()
                        logger.info(f"出现403关闭当前conn【conn_id={conn.conn_id}】")
                    else:
                        response.raise_for_status()
            except Exception as e:
                logger.error(f"fetch_html出现错误: {e.__class__.__name__} {url}")
                conn.close()
            finally:
                retry_count -= 1
    return mongo_info, None


async def producer(mongo_coll: TypeAioCollection):
    while True:
        try:
            mongo_info_list = []
            async for mongo_info in mongo_coll.find(
                {"crawler_status": None}, no_cursor_timeout=True
            ).limit(500):
                mongo_info_list.append(mongo_info)
            if mongo_info_list:
                await mongo_coll.update_many(
                    {
                        "_id": {
                            "$in": [mongo_info["_id"] for mongo_info in mongo_info_list]
                        }
                    },
                    {"$set": {"crawler_status": "processing"}},
                )
                for mongo_info in mongo_info_list:
                    yield mongo_info
            else:
                logger.info("当前没有数据...")
                await asyncio.sleep(30)
        except Exception as e:
            logger.error(f"producer函数出现了错误: {e}")
            await asyncio.sleep(2)


async def main():
    filmont_url_77_coll = await get_async_ny_mongo_link(
        "youtube_task_favortrain", "filmont_url_77"
    )
    xhs_run_1114_coll = await get_async_ny_mongo_link(
        "youtube_task_favortrain", "xhs_run_1114"
    )
    redis_client = await get_aio_redis_client()

    async def deal_get_youtube_key_list_result(mongo_info, youtube_key_list):
        if youtube_key_list is None:
            await filmont_url_77_coll.update_one(
                {"_id": mongo_info["_id"]}, {"$set": {"crawler_status": None}}
            )
            return
        insert_count = 0
        is_success = False
        try:
            await xhs_run_1114_coll.insert_many(youtube_key_list, ordered=False)
            insert_count = len(youtube_key_list)
            is_success = True
        except pymongo.errors.BulkWriteError as e:
            error_details = e.details
            failed_indices_len = len(error_details.get("writeErrors", []))
            insert_count = len(youtube_key_list) - failed_indices_len
            is_success = True
        except Exception as e:
            logger.error(
                f"deal_get_youtube_key_list_result出现错误: {e.__class__.__name__}"
            )
        finally:
            if insert_count:
                formatted_date = datetime.datetime.now(
                    tz=pytz.timezone("Asia/Shanghai")
                ).strftime("%Y-%m-%d")
                await redis_client.hincrby(
                    "xhs_run_1114" + ":" + formatted_date, "total", insert_count
                )
                logger.success(f"写入数据量: {insert_count}")
        if is_success:
            await filmont_url_77_coll.update_one(
                {"_id": mongo_info["_id"]}, {"$set": {"crawler_status": "success"}}
            )

    await (
        stream.iterate(producer(filmont_url_77_coll))
        | pipe.map(get_youtube_key_list, task_limit=100, ordered=False)
        | pipe.starmap(deal_get_youtube_key_list_result, task_limit=100, ordered=False)
    )


if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(create_conn_from_hcaptcha())
