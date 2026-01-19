import json, requests
from config import SEC_HEADERS_BASE
cik10="0001594805"  # example. use your mapped one
url=f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik10}.json"
j=requests.get(url, headers=SEC_HEADERS_BASE).json()
print(j.keys())
print(j.get("facts", {}).keys())