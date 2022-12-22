import os
import sys
import requests
import re
import json
from bs4 import BeautifulSoup

def get_item(item_url: str) -> dict:
    item_dct = {}

    item_page = requests.get(item_url)
    soup = BeautifulSoup(item_page.content, 'html.parser')

    id = re.findall(r'https://realt.by/sale-flats/object/(\d+)/', item_url)[0]
    try:
        price = soup.find('span', class_='sm:ml-1.5 text-subhead sm:text-body text-basic')
        price = re.findall('≈?([\d\s]+)', price.text)[0].strip()
    except:
        price = 'None'

    item_dct['id'] = id
    item_dct['last_price'] = price
    item_dct['fingerprint'] = id + '-' + price

    data = soup.find('ul', class_="w-full -my-1")
    for line in data.find_all('li', class_='relative py-1'):
        item_dct[line.find('span').text] = line.find('p').text

    geo = soup.find('ul', class_="w-full mb-0.5 -my-1")
    for line in geo.find_all('li', class_='relative py-1'):
        try:
            item_dct[line.find('span').text] = line.find('a').text
        except:
            item_dct[line.find('span').text] = line.find('p').text

    return item_dct

def extract():
    url = 'https://realt.by/sale/flats/?search=eJwryS%2FPi89LzE1VNXXKycwGUi5AlgGQslV1MVC1dAaRThZg0kXVxVDVwhDMdlSLL04tKS0AKi7OLyqJT6pE0pmSWJIaX5RallmcmZ%2BHUFiUmhxfkFoUX5CYDrLH1tgAAPdPJd8%3D'

    item_list = []

    while url:
        page = requests.get(url)
        url = ''
        soup = BeautifulSoup(page.content, 'html.parser')
        res = soup.find_all('a', class_='teaser-title')

        for el in res:
          if el.get('title') is not None and re.fullmatch(r"https://realt.by/sale-flats/object/\d+/", el.get('href')):
            item_list.append(get_item(el.get('href')))

        next_pages_links = soup.find('div', class_='paging-list')
        curr = int(next_pages_links.find('a', class_='active').text)
        print(f'page number {curr} is completed')
        if curr == 10:
            break

        for el in next_pages_links.find_all('a'):
            try:
                if int(el.text) == curr + 1:
                    url = el.get('href')
                    break
            except ValueError:
                pass

    return item_list


def upload_to_s3() -> None:
    with open('../data/realtby.json', 'w') as outfile:
        json.dump(extract(), outfile, ensure_ascii=False)


if __name__ == '__main__':

    upload_to_s3()

