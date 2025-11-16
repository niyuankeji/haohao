import random
import string
import secrets
import requests
from curl_cffi import requests as curl_requests


def generate_secure_random_string(min_length=12, max_length=16):
    length = random.randint(min_length, max_length)
    characters = string.ascii_letters + string.digits
    return "".join(secrets.choice(characters) for i in range(length))


headers = {
    # "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    # "accept-language": "zh-CN,zh;q=0.9",
    # "cache-control": "no-cache",
    # "pragma": "no-cache",
    # "priority": "u=0, i",
    # "referer": "https://filmot.com/search/aaa/1?gridView=1",
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "accept-language": "zh-CN,zh;q=0.9",
    "referer": "https://filmot.com/search/aaa/1/9?lang=en&gridView=1",
    "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36",
}
cookies = {
    # "XSRF-TOKEN": "eyJpdiI6IlhNNzF6VjBlaWxKdUJRbGd4ZmtwMUE9PSIsInZhbHVlIjoiN2tldzN2S0F0djhaV0ZMWW1vd0xjWGFTRnlic1lCYzY2UitkbUwwek96TFdzd0VZV3ZvbW00aXBiRldCVW5ManlDYTNzQTM3Q1BWRkUrdS95YVk3V3BRMVRQanoyR0l2TEtYaG1rWWY2MjdkUWJxUlpPbW5DNmpUVHRsclZtQ3YiLCJtYWMiOiI0MzdhYjg3NDIyNDEyNmFlMmE3NTQ3MmFkMWY2NzU4MDNjMzY0MzM4YzExMTAxMmI2NGZlZjk0N2M1OTdhNmQ1IiwidGFnIjoiIn0%3D",
    "m_session": "eyJpdiI6InozUlIvZDFSOUNydHozZ1M1aldkM0E9PSIsInZhbHVlIjoiNUd5NFl5TDF5UlNJR2poRDJ2Tmp4WmZjeEQ0Q2NnczhRbzgvdXpGRlZXbGNFQXNuVWdaZFBuY29yMW5sbXJSbTZRUnc3RExxcHROWGc3ZWRaT3liTGVkaW81L0xpK3E3MkdzcG5raHRyMWoxWDBucitjWE5FaDB6VVpwc2RjSXgiLCJtYWMiOiI0MTkyMWFhMDI5N2QwMWI4NDhhOWVkMTY5MGZmYWFiNzBhNjVkMDBjYTBmNGI0Y2Q2MmZkOWIzOTRiOThhZjVmIiwidGFnIjoiIn0%3D",
    # "ShowFilter": "0",
}
# url = "https://filmot.com/search/zqq/1/2"
# params = {"gridView": "1", "lang": "en"}
url = "https://filmot.com/search/aaa/1/2?gridView=1&lang=en"
proxies = {
    "http": f"http://td-customer-SOluI6kkrdk2-sessid-{generate_secure_random_string()}-sesstime-15:rEpTA530j0i6@43.153.55.54:9999",
    "https": f"http://td-customer-SOluI6kkrdk2-sessid-{generate_secure_random_string()}-sesstime-15:rEpTA530j0i6@43.153.55.54:9999",
}
response = curl_requests.get(
    url,
    headers=headers,
    cookies=cookies,
    # params=params,
    proxies=proxies,
    impersonate="chrome"
)

print(response.text)
print(response)
