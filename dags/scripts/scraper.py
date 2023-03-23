
import requests
from bs4 import BeautifulSoup
from airflow.models import Variable

def getMakeYrPrice():
    i = 1
    url = 'https://www.streetsideclassics.com/vehicles?page=' + str(i)
    arr = []
    while True:
        page =requests.get(url)
        src2 = page.content
        new_soup = BeautifulSoup(src2, 'html.parser')
        classic_search = new_soup.find("div", {'class': 'inventory-grid'})
        i += 1
        url = 'https://www.streetsideclassics.com/vehicles?page=' + str(i)
        try:
            for x in classic_search.find_all('div', {'class': 'top'}):
               spans = x.find_all('span')
               year = spans[1].text.strip()
               make = spans[2].text.strip()
               price = spans[3].text.strip()
               arr.append([year, make, price])
        except:
            break
    csv_path = Variable.get('yr_mk_price')
    df = pd.DataFrame(arr,columns=['Year','Make','Price'])
    return df.to_csv(csv_path,
              index=None, header=True,encoding='utf-8')


def getModel():
    i = 1
    url = 'https://www.streetsideclassics.com/vehicles?page=' + str(i)
    arr = []
    while True:
        page =requests.get(url)
        src2 = page.content
        new_soup = BeautifulSoup(src2, 'html.parser')
        classic_search = new_soup.find("div", {'class': 'inventory-grid'})
        i += 1
        url = 'https://www.streetsideclassics.com/vehicles?page=' + str(i)
        try:
            for x in classic_search.find_all('div', {'class': 'middle'}):
               spans = x.find_all('span')
               model = spans[0].text.strip()
               arr.append([model])
        except:
            break
    csv_path = Variable.get('model_csv')
    df = pd.DataFrame(arr, columns=['Model'])
    return df.to_csv(csv_path,
                     index=None, header=True, encoding='utf-8')

def getDescription():
    i = 1
    url = 'https://www.streetsideclassics.com/vehicles?page=' + str(i)
    arr = []
    while True:
        page =requests.get(url)
        src2 = page.content
        new_soup = BeautifulSoup(src2, 'html.parser')
        classic_search = new_soup.find("div", {'class': 'inventory-grid'})
        i += 1
        url = 'https://www.streetsideclassics.com/vehicles?page=' + str(i)
        try:
            for x in classic_search.find_all('div', {'class': 'bottom'}):
               spans = x.find_all('span')
               description = spans[0].text.strip()
               arr.append([description])
        except:
            break
    csv_path = Variable.get('description_csv')
    df = pd.DataFrame(arr, columns=['Description'])
    return df.to_csv(csv_path,
                     index=None, header=True, encoding='utf-8')

def getLink():
    i = 1
    url = 'https://www.streetsideclassics.com/vehicles?page=' + str(i)
    arr = []
    while True:
        page =requests.get(url)
        src2 = page.content
        new_soup = BeautifulSoup(src2, 'html.parser')
        classic_search = new_soup.find("div", {'class': 'inventory-grid'})
        i += 1
        url = 'https://www.streetsideclassics.com/vehicles?page=' + str(i)
        try:
            for x in classic_search.find_all('a', href=True):
                arr.append([x['href']])
        except:
            break
    csv_path = Variable.get('link_csv')
    df = pd.DataFrame(arr, columns=['Url'])
    df['Url'] = 'streetsideclassics.com' + df['Url']
    return df.to_csv(csv_path,
                     index=None, header=True, encoding='utf-8')

if __name__=="__main__":
    getLink()
