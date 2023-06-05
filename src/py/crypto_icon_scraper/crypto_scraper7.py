import os
import requests
from bs4 import BeautifulSoup
import multiprocessing

def download_image(url, filename, folder):
    response = requests.get(url)
    if response.status_code == 200:
        with open(os.path.join(folder, filename), 'wb') as f:
            f.write(response.content)

def process_page(args):
    page, folder = args
    base_url = "https://www.coingecko.com/?page={}".format(page)
    response = requests.get(base_url)
    soup = BeautifulSoup(response.text, 'html.parser')

    cryptos = soup.find_all('a', class_='tw-hidden lg:tw-flex tw-items-center tw-justify-between')

    for crypto in cryptos:
        img = crypto.find('img')
        if img:
            img_url = img['src']
            filename = img_url.split('/')[-1]
            download_image(img_url, filename, folder)

if __name__ == '__main__':
    folder = "crypto_image_assets_multiproc"
    os.makedirs(folder, exist_ok=True)

    page_args = [(page, folder) for page in range(1, 116)]

    with multiprocessing.Pool() as pool:
        pool.map(process_page, page_args)

