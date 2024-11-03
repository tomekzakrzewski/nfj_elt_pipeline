import requests
import urllib.parse
import time

# HOW SHOULD PROJECT STRUCTURE LOOK, WHERE SHOULD BE INITIAL SCRAPE
# HOW CAN I USE THIS IN ANOTHER FILE
BASE_URL = "https://nofluffjobs.com/api/search/posting"

CATEGORIES = [
    "sys-administrator",
    "business-analyst",
    "architecture",
    "backend",
    "data",
    "ux",
    "devops",
    "erp",
    "embedded",
    "frontend",
    "fullstack",
    "game-dev",
    "mobile",
    "project-manager",
    "security",
    "support",
    "testing",
    "other",
]

HEADERS = {
    "accept": "application/json, text/plain, */*",
    "content-type": "application/infiniteSearch+json",
}


def scrape(pageSize: int):
    payload = get_payload(pageSize)
    query_params = get_query_params(pageSize)
    url = get_url(query_params)

    print("started scraping")
    start = time.time()

    response = requests.post(url, headers=HEADERS, json=payload)
    response.raise_for_status()

    data = response.json()

    unix_timestamp = int(time.time())
    end = time.time()
    took = end - start
    print(f"scaping went successfull")
    print(f"took {took:.2f}")
    print(data)


def get_url(query_params: dict):
    url = f"{BASE_URL}?{urllib.parse.urlencode(query_params)}"

    return url


def get_payload(pageSize: int) -> dict:
    payload = {
        "criteria": f"category={','.join(CATEGORIES)}",
        "url": {"searchParam": "artificial-intelligence"},
        "rawSearch": f"artificial-intelligence category={','.join(CATEGORIES)} ",
        "pageSize": pageSize,
        "withSalaryMatch": False,
    }

    return payload


def get_query_params(pageSize: int) -> dict:
    query_params = {
        "sort": "newest",
        "pageTo": 1,
        "pageSize": pageSize,
        "salaryCurrency": "PLN",
        "salaryPeriod": "month",
        "region": "pl",
        "language": "en-GB",
    }

    return query_params
