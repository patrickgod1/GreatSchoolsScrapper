import json
from bs4 import BeautifulSoup
import requests
import csv
import pandas as pd
import asyncio
import aiohttp
import html5lib
from collections import defaultdict


def createStateURL(dfDict):
    # url = 'https://www.greatschools.org/schools/districts/{state}/{abbr}/'
    # url = []
    with open('states.csv') as csvFile:
        # readCSV = csv.reader(csvFile, delimiter=',')
        for state in csv.reader(csvFile, delimiter=','):
            dfDict[state[1]]['stateURL'] = f'https://www.greatschools.org/schools/districts/{state[0]}/{state[1]}/'
        # for state in readCSV:
        #     url.append(f'https://www.greatschools.org/schools/districts/{state[0]}/{state[1]}/')
    # return url


# async def fetchDistrictLinks(session, statePage, dfDict):
#     async with session.get(statePage) as res:
#         text = await res.text()

#         districtLinks = [f"https://www.greatschools.org{a['href']}" for td in BeautifulSoup(text, 'html.parser').find_all('td', {'class': 'city-district-link'}) for a in td.find_all('a')]
#         # df = pd.read_html(text, header=0)
#         # with pd.option_context('display.max_rows', None, 'display.max_columns', None):
#         #     print(df[0].head())
#         # new_df = pd.DataFrame(columns=['Phone Number', 'Website', '# of Schools', 'Grades'], data=[scrapeDistrict(session, i) for i in districtLinks])
#     # resultDF = pd.concat((df[0], new_df), axis=1)

#     # soup = await BeautifulSoup(req.content, 'html.parser')

#     # for link in soup.find_all('td', {'class': 'city-district-link'}).find_all('a', recursive=False):
#     # districtLinks = [a["href"] for td in soup.find_all('td', {'class': 'city - district - link'}) for a in td.find_all('a')]
#     dfDict[statePage[-3:-1]]['districtLinks'] = districtLinks
#     # return districtLinks
#     # districtLinks = []
#     # for td in soup.find_all('td', {'class': 'city-district-link'}):
#     #     # print(td)
#     #     for a in td.find_all('a'):
#     #         districtLinks.append(f'https://www.greatschools.org{a["href"]}')
#     # print(districtLinks)


async def scrapeDistrictLinks(session, dfDict):
    # async with aiohttp.ClientSession() as session:
    for key in dfDict.keys():
        async with session.get(dfDict[key]['stateURL']) as res:
            text = await res.text()
            dfDict[key]['districtLinks'] = [f"https://www.greatschools.org{a['href']}" for td in BeautifulSoup(text, 'html.parser').find_all('td', {'class': 'city-district-link'}) for a in td.find_all('a')]
            result = [await scrapeDistrict(session, link) for link in dfDict[key]['districtLinks']]
        dfDict[key]['df'] = pd.read_html(text, header=0)[0]
        new_df = pd.DataFrame(columns=['Phone Number', 'Website', '# of Schools', 'Grades'], data=result)
        dfDict[key]['df'] = pd.concat((dfDict[key]['df'], new_df), axis=1)
        dfDict[key]['df'].to_csv(f"{key}.csv")
    # result = await [fetchDistrictLinks(session, statePage, dfDict) for statePage in stateURL]
    # return result


async def scrapeDistrict(session, link):
    # url = 'https://www.greatschools.org/california/lafayette/acalanes-union-high-school-district/'
    async with session.get(link) as res:
        text = await res.text()
    # req = requests.get(url)
    soup = BeautifulSoup(text, 'html.parser')

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

    # print(districtURL)
    # print(data2["heroData"]["schoolCount"])
    # print(data2["heroData"]["grades"])
    print([data["name"], data["address"]["addressLocality"], data["telephone"], districtURL, data2["heroData"]["schoolCount"], data2["heroData"]["grades"]])

    result = [data["telephone"], districtURL, data2["heroData"]["schoolCount"], data2["heroData"]["grades"]]

    return result


async def main():
    dfDict = defaultdict(dict)
    createStateURL(dfDict)
    # print(stateURL)
    # for statePage in stateURL:
    #     scrapeDistrictList(statePage)
    # print(stateURL[4])
    # districtLinks = loop.run_until_complete(scrapeDistrictLinks(stateURL))

    async with aiohttp.ClientSession() as session:
        districtLinks = await scrapeDistrictLinks(session, dfDict)

        # for key in dfDict.keys():
        #     data = asyncio.ensure_future(scrapeDistrict(session, dfDict, key))
        #     print(data)
        # new_df = pd.DataFrame(columns=['Phone Number', 'Website', '# of Schools', 'Grades'], data=data)
    # print(dfDict['WY']['districtLinks'])

    # districtLinks = scrapeDistrictLinks(stateURL[4])
    # print(districtLinks)
    # print(len(districtLinks))
    # async with pd.read_html(stateURL[4], header=0) as res:
    #     df = await res[0]
    # for state in stateURL:
    #     dfDict[state[-3:-1]]['df'] = pd.read_html(state, header=0)[0]
    #     new_df = pd.DataFrame(columns=['Phone Number', 'Website', '# of Schools', 'Grades'], data=[await scrapeDistrict(session, i) for i in districtLinks])

    with pd.option_context('display.max_rows', None, 'display.max_columns', None):
        # print(df[0].head())
        print(dfDict['CA']['df'].head())
    # new_df = pd.DataFrame(columns=['Phone Number', 'Website', '# of Schools', 'Grades'], data=[await scrapeDistrict(session, i) for i in districtLinks])

    # print(f'{stateURL[4][-3:-1]}')
    # result = pd.concat((df[0], new_df), axis=1)
    # result.to_csv(f"{stateURL[4][-3:-1]}.csv")
    # with pd.option_context('display.max_rows', None, 'display.max_columns', None):
    #     # print(df[0].head())
    #     print(result)


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    districtLinks = loop.run_until_complete(main())
    loop.close()
