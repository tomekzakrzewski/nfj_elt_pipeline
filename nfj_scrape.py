import requests
import urllib.parse
import time

SIZE = 15000
BASE_URL = "https://nofluffjobs.com/api/search/posting"

categories = [
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


headers = {
    "accept": "application/json, text/plain, */*",
    "content-type": "application/infiniteSearch+json",
}

query_params = {
    "sort": "newest",
    "pageTo": 1,
    "pageSize": SIZE,
    "salaryCurrency": "PLN",
    "salaryPeriod": "month",
    "region": "pl",
    "language": "en-GB",
}

url = f"{BASE_URL}?{urllib.parse.urlencode(query_params)}"

payload = {
    "criteria": f"category={','.join(categories)}",
    "url": {"searchParam": "artificial-intelligence"},
    "rawSearch": f"artificial-intelligence category={','.join(categories)} ",
    "pageSize": SIZE,
    "withSalaryMatch": False,
}


def scrape():
    print("started scraping")
    start = time.time()
    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()


# HOW SHOULD PROJECT STRUCTURE LOOK, WHERE SHOULD BE INITIAL SCRAPE
