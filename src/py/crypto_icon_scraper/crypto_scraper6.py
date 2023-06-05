import os
import requests
from bs4 import BeautifulSoup
import multiprocessing

def download_image(url, filename, folder):
    response = requests.get(url)
    if response.status_code == 200:
        with open(os.path.join(folder, filename), 'wb') as f:
            f.write(response.content)

def scrape_page(url):
    response = requests.get(url)
    content = response.content
    soup = BeautifulSoup(content, 'html.parser')

    table = soup.find('tbody', {'data-target': 'currencies.contentBox'})
    rows = table.find_all('tr')

    cryptos = []

    for row in rows:
        crypto = {}
        crypto['name'] = row.find('td', class_='coin-name').get_text(strip=True)
        crypto['symbol'] = row.find('span', class_='d-lg-inline').get_text(strip=True)
        crypto['img_url'] = row.find('td', class_='coin-name').find('img')['src']

        cryptos.append(crypto)

    return cryptos

def process_page(page_num):
    print(f"Scraping page {page_num}...")
    url = base_url + str(page_num)
    cryptos = scrape_page(url)

    for crypto in cryptos:
        print(f"Downloading {crypto['name']} image...")
        filename = f"{crypto['symbol']}.png"
        download_image(crypto['img_url'], filename, folder)
        print(f"Downloaded {crypto['name']} image to {folder}/{filename}")

    print(f"Finished downloading images from page {page_num}")

# Create a folder for storing downloaded images
folder = "crypto_image_assets_parallel_proc"
if not os.path.exists(folder):
    os.makedirs(folder)

# Scrape and download images from all 115 pages
base_url = "https://www.coingecko.com/?page="
total_pages = 115

# Parallelize the downloading process using multiple processes
with multiprocessing.Pool() as pool:
    page_nums = list(range(1, total_pages + 1))
    pool.map(process_page, page_nums)

print("All images downloaded.")

