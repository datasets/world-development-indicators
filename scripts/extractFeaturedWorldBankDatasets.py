from bs4 import BeautifulSoup
from urllib.request import urlopen, Request
import re
import os
import time
import shutil


def removeQuestMarkAndAfter(link):
    link = re.sub(r'\?.+', '', link)
    return link


url = "https://data.worldbank.org/indicator?tab=featured"
headers = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}
req = Request(url, headers=headers)
html = urlopen(req).read()
soup = BeautifulSoup(html, 'html.parser')
sections = soup.find("div", {"class": "overviewArea body"}).findAll("section", {"class": "nav-item"})
ulList = list(map(lambda x: x.findAll("ul")[-1], sections))
liList = []
for ul in ulList:
    lis = ul.findAll("li")
    liList += lis

hrefList = list(map(lambda x: x.find("a").get("href"), liList))
links = list(map(lambda x: 'https://data.worldbank.org' + removeQuestMarkAndAfter(x), hrefList))
links = list(set(links))
for index, link in enumerate(links):
    os.system('python3 scripts/get.py ' + link + ' --force')
    time.sleep(3)
    if index % 20 == 0:
        status = str(round(index*100/len(links))) + ' %  completed'
        print(status)

# worldBankIndicatorsFolder = '500ds-Branko/world-bank-indicator-datasets/indicators'
# shutil.move('indicators', worldBankIndicatorsFolder)
shutil.rmtree('cache')
