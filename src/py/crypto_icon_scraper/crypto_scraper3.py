import os
import requests
from bs4 import BeautifulSoup

def download_image(url, filename, folder):
    response = requests.get(url)
    if response.status_code == 200:
        with open(os.path.join(folder, filename), 'wb') as f:
            f.write(response.content)

url = "https://www.coingecko.com/?page=1"

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

# Create a folder for storing downloaded images
folder = "crypto_image_assets"
if not os.path.exists(folder):
    os.makedirs(folder)

# Download images
for crypto in cryptos:
    print(f"Downloading {crypto['name']} image...")
    filename = f"{crypto['symbol']}.png"
    download_image(crypto['img_url'], filename, folder)
    print(f"Downloaded {crypto['name']} image to {folder}/{filename}")

print("All images downloaded.")

