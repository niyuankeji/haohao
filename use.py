import asyncio
import re
import time
import motor.motor_asyncio
import socket
import requests as requests_no
from pymongo import UpdateOne
import datetime
from kink import di, inject
from curl_cffi import requests
from lxml import etree
from threading import Thread
from loguru import logger
from pymongo import MongoClient
import get_keyword_from_mongo


# ---------------- MongoDB -------------------
mongo_url: str = "mongodb://developer:QYZ3mxps4POMFb76@mongo-wy-hk.mereith.top:30017/?authSource=admin"

mongodb_cli = MongoClient(mongo_url)
youtube_task_favortrain = mongodb_cli.youtube_task_favortrain
mongodb_client = youtube_task_favortrain.xhs_run_1114   # 插入 filmot 结果的集合


# ---------------- 语言映射 -------------------
filmot_language = {
    "Indonesian": "id",
    "English": "en",
    "Portuguese": "pt",
    "Turkish": "tr",
    "Japanese": "ja",
    "Spanish": "es",
    "French": "fr",
    "Korean": "ko",
    "Vietnamese": "vi",
    "Romanian": "ro",
    "Italian": "it",
    "German": "de",
    "Dutch": "nl",
    "Filipino": "fil",
    "Russian": "ru",
    "Polish": "pl",
    "Hindi": "hi",
    "Arabic": "ar",
    "Finnish": "fi",
    "Thai": "th",
    "Hungarian": "hu",
    "Czech": "cs",
    "Slovak": "sk",
    "Lithuanian": "lt",
    "Ukrainian": "uk",
    "Greek": "el",
    "Swedish": "sv",
    "Latvian": "lv",
    "Hebrew": "iw",
    "Persian": "fa",
    "Danish": "da",
    "Norwegian": "no",
    "Bulgarian": "bg",
    "Bangla": "bn",
    "Telugu": "te",
    "Serbian": "sr",
    "Tamil": "ta",
    "Kannada": "kn",
    "Punjabi": "pa",
    "Gujarati": "gu",
    "Slovenian": "sl",
    "Marathi": "mr",
    "Georgian": "ka",
    "Catalan": "ca",
    "Chinese": "yue"
}


# ---------------- Cookie / Header / Proxy -------------------

def generate_secure_random_string(min_length=3, max_length=16):
    import secrets, random, string
    length = random.randint(min_length, max_length)
    characters = string.ascii_letters + string.digits
    return ''.join(secrets.choice(characters) for i in range(length))


def get_proxy():
    return f"http://td-customer-SOluI6kkrdk2-sessid-{generate_secure_random_string()}-sesstime-10:rEpTA530j0i6@43.153.55.54:9999"


# di 污染危险，初始化用，但 proxy 将按线程覆盖
di["cookie"] = {'cf_clearance': ""}
di["headers"] = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'zh-CN,zh;q=0.9',
    'cache-control': 'no-cache',
    'pragma': 'no-cache',
    'priority': 'u=0, i',
    'referer': 'https://filmot.com/search/q/1/2?lang=en&gridView=1&',
    'sec-ch-ua': '"Chromium";v="142", "Google Chrome";v="142", "Not_A Brand";v="99"',
    'sec-ch-ua-arch': '"x86"',
    'sec-ch-ua-bitness': '"64"',
    'sec-ch-ua-full-version': '"142.0.7444.60"',
    'sec-ch-ua-full-version-list': '"Chromium";v="142.0.7444.60", "Google Chrome";v="142.0.7444.60", "Not_A Brand";v="99.0.0.0"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36',
}

session = requests.Session()



@inject
def requests_session(url, n):
    for i in range(5):
        try:
            logger.debug(f'Requesting {url}')
            resp = requests.get(
                url,
                cookies=di["cookie"],
                impersonate="chrome",
                headers=di["headers"],
                proxy=di[f"proxy{n}"],
                timeout=600
            )

            if resp.status_code == 403:
                logger.debug("cookie失效,准备获取cookie")
                di[f"proxy{n}"] = get_proxy()
                resp = requests.get(
                    url,
                    cookies=di["cookie"],
                    impersonate="chrome",
                    headers=di["headers"],
                    proxy=di[f"proxy{n}"]
                )
                di["cookie"]["cf_clearance"] = get_cookie(
                    target_url=url, resp_text=resp.text,
                    proxy=di[f"proxy{n}"],
                    user_agent=di["headers"]["user-agent"]
                )
                continue

            if resp.status_code in [200, 404]:
                return resp

            time.sleep(1)

        except Exception as e:
            logger.error(f"请求出现异常: {e}")
            time.sleep(3)

    return None  # 修复点


def get_cookie(target_url, resp_text, proxy, user_agent, type="cookie", key="64798cf2e0464b0192cf5d467b8d76d8"):
    data = {
        "url": target_url,
        "html": resp_text,
        "proxy": proxy,
        "ua": user_agent,
        "type": "cookie",
        "key": key,
    }
    for _ in range(3):
        try:
            response = requests_no.post(
                url="http://ec2-13-212-87-87.ap-southeast-1.compute.amazonaws.com:13000/compute",
                json=data
            )
            resp_json = response.json()
            if resp_json["success"]:
                return resp_json["cf"]
            else:
                return ""
        except Exception as e:
            logger.error(f"获取cookie失败: {e}")
    return ""


def convert_number(s):
    s = s.strip().upper()
    if s.endswith('K'):
        return float(s[:-1]) * 1000
    elif s.endswith('M'):
        return float(s[:-1]) * 1_000_000
    elif s.endswith('B'):
        return float(s[:-1]) * 1_000_000_000
    else:
        return float(s)


class Filmot:
    def __init__(self, languages):
        self.languages = languages

    def get_html(self, titleQuery: str, n, buffer):
        page_num = 84
        for i in range(1, 84):
            if i > page_num:
                break
            url = f"https://filmot.com/search/{titleQuery}/1/{i}?lang={filmot_language[self.languages]}&gridView=1&"
            response = requests_session(url, n)
            if not response:
                logger.error(f"请求失败: {url}")
                continue
            e = etree.HTML(response.text)
            sticky = "".join(e.xpath('//*[@id="resultTop"]/div/text()')) or ""
            logger.info(f"Sticky top: {sticky}")
            if "No" in sticky:
                break
            try:
                if any(x in sticky for x in ["M", "K", "B"]):
                    page_num = int(convert_number(sticky.split("clips")[0]) / 40) + 1
                else:
                    page_num = int(int(sticky.split("clips")[0]) / 40) + 1
            except:
                page_num = 1
            trs = e.xpath("//div[contains(@id, 'vcard')]/a[2]/@href")
            for tr in trs:
                youtube_key = re.findall("=(.*?)&", "".join(tr))[0]
                buffer.append({"_id": youtube_key, "language": self.languages})
                if len(buffer) >= 1000:
                    try:
                        mongodb_client.insert_many(buffer, ordered=False)
                        logger.info(f"写入 {len(buffer)} 条")
                    except Exception as e:
                        logger.error(f"写入异常：{e}")
                    buffer.clear()


def thread_work(keyword_list, n):
    language_list = ["English", "Chinese"]
    # 每线程独立 proxy
    di[f"proxy{n}"] = get_proxy()
    buffer = []
    for lang in language_list:
        for keyword in keyword_list:
            logger.info(f'当前key为：{keyword} 语言：{lang}')
            Filmot(lang).get_html(keyword, n, buffer)
            # 修复：线程中不能 asyncio.run → 改同步 Mongo
            try:
                youtube_task_favortrain.id_keyword.update_one(
                    {"_id": keyword},
                    {"$set": {"status": 1}}
                )
                logger.info(f"更新关键词状态：{keyword} → 1")
            except Exception as e:
                logger.error(f"更新关键词失败 {keyword}: {e}")
    # 🚀 最关键补丁：最后不足 1000 条也写入
    if buffer:
        try:
            mongodb_client.insert_many(buffer, ordered=False)
            logger.info(f"最后写入残留 {len(buffer)} 条")
        except Exception as e:
            logger.error(f"最后插入残留数据失败: {e}")
        finally:
            buffer.clear()


async def run():
    di["coll"] = get_keyword_from_mongo.get__aws_mongo_link("haohao_youtube", "id_keyword")
    keyword_list = await get_keyword_from_mongo.get_pending_ids()
    chunk_size = (len(keyword_list) + 9) // 10
    chunks = [keyword_list[i:i + chunk_size] for i in range(0, len(keyword_list), chunk_size)]
    threads = []
    idx = 0

    for chunk in chunks:
        idx += 1
        t = Thread(target=thread_work, args=(chunk, idx))
        t.start()
        threads.append(t)

    for t in threads:
        t.join()


if __name__ == '__main__':
    asyncio.run(run())
