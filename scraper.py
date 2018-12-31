import json
from bs4 import BeautifulSoup
import requests
import re
import csv
import pandas as pd


def createStateURL():
    # url = 'https://www.greatschools.org/schools/districts/{state}/{abbr}/'
    url = []
    with open('states.csv') as csvFile:
        readCSV = csv.reader(csvFile, delimiter=',')
        for state in readCSV:
            url.append(f'https://www.greatschools.org/schools/districts/{state[0]}/{state[1]}/')
    return url


def scrapeDistrictLinks(statePage):
    req = requests.get(statePage)
    soup = BeautifulSoup(req.content, 'html.parser')
    # for link in soup.find_all('td', {'class': 'city-district-link'}).find_all('a', recursive=False):
    # districtLinks = [a["href"] for td in soup.find_all('td', {'class': 'city - district - link'}) for a in td.find_all('a')]
    return [f"https://www.greatschools.org{a['href']}" for td in soup.find_all('td', {'class': 'city-district-link'}) for a in td.find_all('a')]
    # districtLinks = []
    # for td in soup.find_all('td', {'class': 'city-district-link'}):
    #     # print(td)
    #     for a in td.find_all('a'):
    #         districtLinks.append(f'https://www.greatschools.org{a["href"]}')
    # print(districtLinks)


def scrapeDistrict(url):
    # url = 'https://www.greatschools.org/california/lafayette/acalanes-union-high-school-district/'
    req = requests.get(url)
    soup = BeautifulSoup(req.content, 'html.parser')

    js = soup.find_all('script', type='application/ld+json')[2].text
    data = json.loads(js, strict=False)
    # print(data)
    # print(data["name"])
    # print(data["address"]["addressLocality"])
    # print(data["telephone"])

    js2 = soup.find_all('script', type="application/json")[1].text
    data2 = json.loads(js2, strict=False)
    districtURL = ''
    if "districtUrl" in data2["locality"]:
        districtURL = data2["locality"]["districtUrl"]

    # print(data2["locality"]["districtUrl"])
    # print(data2["heroData"]["schoolCount"])
    # print(data2["heroData"]["grades"])
    return [data["telephone"], districtURL, data2["heroData"]["schoolCount"], data2["heroData"]["grades"]]


if __name__ == '__main__':
    stateURL = createStateURL()
    # for statePage in stateURL:
    #     scrapeDistrictList(statePage)
    print(stateURL[4])
    districtLinks = scrapeDistrictLinks(stateURL[4])
    # print(districtLinks)
    print(len(districtLinks))
    df = pd.read_html(stateURL[4], header=0)
    df[0][['Phone Number', 'Website', '# of Schools', '# of Schools', 'Grades']] = [scrapeDistrict(districtLinks[0]) for i in districtLinks]
    # for i in districtLinks:
    #     print(scrapeDistrict(i))
    # print(listcomp.head(5))
    # df['Phone Number'] =
    # df['Website'] =
    # df['# of Schools'] =
    # df['Grades'] =
    with pd.option_context('display.max_rows', None, 'display.max_columns', None):
        print(df[0].head())
    # scrapeDistrict(districtLinks[0])
