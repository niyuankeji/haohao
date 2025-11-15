import asyncio
import re
import time
import motor.motor_asyncio
import socket
import requests as requests_no
from pymongo import UpdateOne
import datetime
from kink import di, inject
from  curl_cffi import requests
from lxml import etree
from threading import Thread
from loguru import logger
from pymongo import MongoClient
import get_keyword_from_mongo

# youtube_task_favortrain


mongo_url: str = "mongodb://developer:QYZ3mxps4POMFb76@mongo-wy-hk.mereith.top:30017/?authSource=admin"

mongodb_cli = MongoClient(mongo_url)
youtube_task_favortrain = mongodb_cli.youtube_task_favortrain
mongodb_client = youtube_task_favortrain.xhs_run_1114

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
    "Chinese":"yue"
}

#5s盾必备3要素,方便后续ua池维护
di["cookie"]=  {'cf_clearance': ""}
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
    'sec-ch-ua-model': '""',
    'sec-ch-ua-platform': '"Windows"',
    'sec-ch-ua-platform-version': '"19.0.0"',
    'sec-fetch-dest': 'document',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'same-origin',
    'sec-fetch-user': '?1',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36',
    # 'cookie': 'ShowFilter=1; cf_clearance=kQS3IGvDVBg5wrDZ5FfhbTY.ey7pUno6CsoDRZA.Xe4-1763129633-1.2.1.1-UTJC43QDA5XnDTDDGTmm.nyymMWx5PoQZkZ5AXEP8EQDJFjOdTTdKaqFE79bKBjZ71FWdPgOt2fF4OlVs.zgnWUR.U80z5HtWabfEpOwR60yRsKJg7oY49VpLm6agwucdPXkUkjMKegOiU9wE.2vQ8zMC93OnF2XgBIybt_vJC84LQl07ihEn.WKMR3O.Bm.q9DTnYy6FHl.fxgWnhZfyb.Y3SuGGHOQ7qLw4GLVVqw; XSRF-TOKEN=eyJpdiI6IkdjUFBiL2tLTkRycHBwMUYyKzhlaEE9PSIsInZhbHVlIjoiNVBYNjVaNWdXZWY1QThRTk9kYmdsSENOZXNZbzlrSEZBUTNuU1UwaW5nTUtxWXppT3E1ZVpKSlRBOXdxa240a2lBeHhvcFlndUxTcDN6cG9hcmUzcVFsTkRJMllhUmozUkd5RFpkNTY3b0syQnRBejdkMUtZeHBtZE9PcjBhaWsiLCJtYWMiOiI2ZjlkZGFkNzQ1NjlhMDMxYzNlZmVkZjQ0OGY3ODEzNjgyNDY0MzVhOTliNTM5ZTE1YWYxMWM4MGNiYTdkNjJmIiwidGFnIjoiIn0%3D; m_session=eyJpdiI6IjJxV3cwWXdhK0pLQnYrQjJ5TTc4WFE9PSIsInZhbHVlIjoiSWF1UW9HdXdheW0vVVFlNjI1b0kyK09EMnBwZndlV092TXVhWUxVVzl5TXRJSG9KK0NjbjNYaEwzaUR5VlppYjVOZlhpNFhkYnR0Yis1b3VJc0s2d3d0eW5sbXlIUmRHcjFXd0ROWVJVWDNEeStWMGw4cFlKbk4yeFU1K2s2R0YiLCJtYWMiOiI0OWE0YWZmYjkyM2MwOTIwZWZhNzBmZjU5MWU4MGFmOTdiNDE3NTQ2ODg2NDZjMDk0MWE5ZjdiZjkyZjQ5NmExIiwidGFnIjoiIn0%3D',
}
def generate_secure_random_string(min_length=3, max_length=16):
    import secrets, random, string
    length = random.randint(min_length, max_length)
    characters = string.ascii_letters + string.digits
    return ''.join(secrets.choice(characters) for i in range(length))
def get_proxy():
    return f"http://td-customer-SOluI6kkrdk2-sessid-{generate_secure_random_string()}-sesstime-10:rEpTA530j0i6@43.153.55.54:9999"


di["proxy"]=get_proxy()
#proxy = f"http://google_scholar-zone-custom-session-{generate_secure_random_string()}-sessTime-15:google_scholar@503e3d3d62471c33.arq.na.ipidea.online:2333"
session = requests.Session()
# 获取主机名
hostname = socket.gethostname()
buffer = []
batch_size = 10000


@inject
def requests_session(url,n):
    for i in range(5):
        try:
            logger.debug(f'Requesting {url}')
            resp = requests.get(url, cookies=di["cookie"], impersonate="chrome", headers=di["headers"],proxy=di[f"proxy{n}"],timeout=600)
            if resp.status_code==403:
                logger.debug("cookie失效,准备获取cookie")
                di[f"proxy{n}"] = get_proxy()
                resp = requests.get(url, cookies=di["cookie"], impersonate="chrome", headers=di["headers"],
                                    proxy=di[f"proxy{n}"])
                di["cookie"]["cf_clearance"] = get_cookie(target_url=url,resp_text=resp.text,proxy=di[f"proxy{n}"],user_agent=di["headers"]["user-agent"])
                #di["cookie"], di["headers"], di["proxy"]
                continue
            elif resp.status_code != 200 and resp.status_code != 404:
                 time.sleep(1)
                 continue
            elif resp.status_code==200:
                return resp  # 返回响应内容
        except Exception as e:
                logger.error(f"请求出现异常: {e}")
                time.sleep(3)

def get_cookie(
    target_url: str,
    resp_text: str,
    proxy: str,
    user_agent: str,
    type="cookie",
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
    times=3
    while times:
        times=times-1
        try:
            response=requests_no.post(
                    url="http://ec2-13-212-87-87.ap-southeast-1.compute.amazonaws.com:13000/compute",
                    json=data)
            resp_json =response.json()
            logger.debug(resp_json)
            if resp_json["success"]==True:
                return resp_json["cf"]
            else:
               return ""
        except Exception as e:
            logger.error(f"获取cookie失败,正在重试{e}")
            continue

def convert_number(s):
    """
    将带后缀（如K, M, B）的数字字符串转换为具体数值。

    示例：
      "1.1K" -> 1100.0
      "1.2M" -> 1200000.0
      "500"  -> 500.0
    """
    # 去除左右空格，并将字符串转为大写，便于统一处理
    s = s.strip().upper()
    # 判断是否包含后缀
    if s.endswith('K'):
        return float(s[:-1]) * 1_000
    elif s.endswith('M'):
        return float(s[:-1]) * 1_000_000
    elif s.endswith('B'):
        return float(s[:-1]) * 1_000_000_000
    else:
        # 如果没有后缀，直接转换为浮点数
        return float(s)


def get_today() -> str:
    today = datetime.datetime.now().strftime('%Y%m%d')
    return today


class Filmot:
    def __init__(self, languages):
        self.languages = languages


    def get_html(self, titleQuery: str,n):
        num_age = 0
        page_num = 84
        for i in range(1, 84):
            if num_age > page_num:
                break
            url = f"https://filmot.com/search/{titleQuery}/1/{i}?lang={filmot_language[self.languages]}&gridView=1&"
            response = requests_session(url,n)
            num_age += 1
            if response:
                e = etree.HTML(response.text)
                try:
                    sticky = "".join(e.xpath('//*[@id="resultTop"]/div/text()'))
                    logger.info(f"Sticky top: {sticky}")
                except Exception as e:
                    sticky = ""
                if "No" in sticky:
                    break
                elif "M" in sticky or "K" in sticky or "B" in sticky:
                    page_num = int(convert_number(sticky.split("clips")[0]) / 40) + 1
                    logger.info(f"M-->K-->B: {page_num}")
                else:
                    page_num = int(int(sticky.split("clips")[0]) / 40) + 1
                    logger.info(f"page_num: {page_num}")
                trs = e.xpath("//div[contains(@id, 'vcard')]/a[2]/@href")
                for tr in trs:
                    youtube_key = re.findall("=(.*?)&", "".join(tr))[0]
                    buffer.append({"_id": youtube_key, "language": self.languages})
                    logger.debug(buffer)
                    if len(buffer) >= batch_size:
                        try:
                            mongodb_client.insert_many(buffer, ordered=False)
                            # todo 插入monogo
                            print(f"写入 {len(buffer)} 条")
                        except Exception as e:
                            print("写入异常：", e)
                        buffer.clear()  # 清空继续跑

def thread_work(keyword_list,n):
    language_list = ["English", "Chinese"]
    # "Chinese":"yue"
    di[f"proxy{n}"] = get_proxy()
    for i in language_list:
        for j in keyword_list:
            json_data = {
                'keyword':j,
                'language': i,
            }
            keyword = json_data["keyword"]
            languages = json_data["language"]
            logger.info(f'当前key为：{keyword}')
            Filmot(languages).get_html(keyword,n)
            # 将这个keywork_list的字段到mongo设置为1
            op = UpdateOne({"_id": keyword}, {"$set": {"status": 1}})
            asyncio.run(di["coll"].bulk_write([op]))
            logger.info(f"已将 {keyword_list} 个关键词状态更新为 1")




async def run():
    di["coll"]=get_keyword_from_mongo.get__aws_mongo_link("haohao_youtube", "id_keyword")
    keyword_list=await get_keyword_from_mongo.get_pending_ids()
    chunk_size = (len(keyword_list) + 9) // 10
    chunks = [keyword_list[i:i + chunk_size] for i in range(0, len(keyword_list), chunk_size)]
    threads = []
    i=0
    for chunk in chunks:
        i=i+1
        t = Thread(target=thread_work, args=(chunk,i))
        t.start()
        threads.append(t)
    for t in threads:
        t.join()


if __name__ == '__main__':
    asyncio.run(run())

