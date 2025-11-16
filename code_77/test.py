# import random
# import string
# import secrets
# import requests
# from curl_cffi import requests as curl_requests
# def generate_secure_random_string(min_length=12, max_length=16):
#     length = random.randint(min_length, max_length)
#     characters = string.ascii_letters + string.digits
#     return "".join(secrets.choice(characters) for i in range(length))
# headers = {
#     # "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
#     # "accept-language": "zh-CN,zh;q=0.9",
#     # "cache-control": "no-cache",
#     # "pragma": "no-cache",
#     # "priority": "u=0, i",
#     # "referer": "https://filmot.com/search/aaa/1?gridView=1",
#     "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
#     "accept-language": "zh-CN,zh;q=0.9",
#     "referer": "https://filmot.com/search/aaa/1/9?lang=en&gridView=1",
#     "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
# }
# cookies = {
#     "m_session": "eyJpdiI6Imp0am1mZnJrWldpbUV3TEpFK1d1ZkE9PSIsInZhbHVlIjoiSHd5dXg2a1c2RW96SHR6ZlNpVGUzVFpJNFNCR2ZjN3ZNSHV6NWZCTzBjWVcxYXcxNkl1U0RwaG1mYS9zc3F4dmM0TWQzOS9kMSsrUWw3OHJBb2VFV2tFSUIxRmJMemhYMndXSDYxcVV6aEJneHJRekJWQkRGWXY5bkNYeisrQ1IiLCJtYWMiOiIxN2I3YTM2YzAyMGFmYTM0ZTcxMWJmODdiMDg1YzI1MWJjZjgwYTcwZmFkYmQ3OTVjYmIyMjhjYTcyYWVjNzk5IiwidGFnIjoiIn0%3D",
# }
# # url = "https://filmot.com/search/zqq/1/2"
# # params = {"gridView": "1", "lang": "en"}
# url = "https://filmot.com/search/aaa/1/2?gridView=1&lang=en"
# proxies = {
#     "http": f"http://td-customer-SOluI6kkrdk2-sessid-{generate_secure_random_string()}-sesstime-15:rEpTA530j0i6@43.153.55.54:9999",
#     "https": f"http://td-customer-SOluI6kkrdk2-sessid-{generate_secure_random_string()}-sesstime-15:rEpTA530j0i6@43.153.55.54:9999",
# }
# response = curl_requests.get(
#     url,
#     headers=headers,
#     cookies=cookies,
#     # params=params,
#     proxies=proxies,
#     impersonate="chrome",
# )
# if response.status_code != 200:
#     print(f"错误的response.status_code: {response.status_code}")
# else:
#     for i in range(3, 15):
#         url = f"https://filmot.com/search/aaa/1/{i}?gridView=1&lang=en"
#         response = curl_requests.get(
#             url,
#             headers=headers,
#             cookies=cookies,
#             # params=params,
#             proxies=proxies,
#             impersonate="chrome",
#         )
#         print(f"「{i}」成功的response.status_code: {response.status_code}")

import re
import json
from curl_cffi import requests

session = requests.Session()

headers = {
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "accept-language": "zh-CN,zh;q=0.9",
    "cache-control": "no-cache",
    "pragma": "no-cache",
    "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36",
}
url = "https://filmot.com/search/aaa/1/2"
params = {"gridView": "1", "lang": "en"}
response = session.get(url, headers=headers, params=params)
_token = re.findall(
    r'<input type="hidden" name="_token" value="(.*?)">',
    response.text,
)[0]

headers = {
    "Accept": "*/*",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    "Content-Type": "application/json",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36",
    "User-Token": "f77c1828-d256-49f8-adfd-e634c82a71c8",
}
url = "http://api.nocaptcha.io/api/wanda/hcaptcha/universal"
data = {
    "sitekey": "58b0f6cd-815d-4d93-aad6-d80c7d56a8aa",
    "referer": "https://filmot.com/captcha-verify",
    "rqdata": "",
    "user_agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36",
    "extra": {"origin": "chrome-plugin"},
}
data = json.dumps(data, separators=(",", ":"))
response = requests.post(url, headers=headers, data=data, verify=False, timeout=20)

resp_json = json.loads(response.text)
print(resp_json)

headers = {
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "accept-language": "zh-CN,zh;q=0.9,en;q=0.8",
    "cache-control": "no-cache",
    "content-type": "application/x-www-form-urlencoded",
    "origin": "https://filmot.com",
    "referer": "https://filmot.com/captcha-verify",
    "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36",
}
url = "https://filmot.com/captcha-validate"
data = {
    "_token": _token,
    "g-recaptcha-response": resp_json["data"]["generated_pass_UUID"],
    "h-captcha-response": resp_json["data"]["generated_pass_UUID"],
}
response = session.post(url, headers=headers, data=data)
print(response.text)
