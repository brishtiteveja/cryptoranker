import os
import requests
from bs4 import BeautifulSoup

url = "https://www.coingecko.com/?page=1"
response = requests.get(url)
soup = BeautifulSoup(response.text, "html.parser")

coins = soup.find_all("tr", class_="d-none d-lg-table-row")

# Create a folder to store the images
os.makedirs("crypto_symbols", exist_ok=True)

for coin in coins:
    symbol = coin.find("span", class_="d-lg-none font-bold").text
    img_url = coin.find("img")["src"]
    img_response = requests.get(img_url)

    if img_response.status_code == 200:
        with open(f"crypto_symbols/{symbol}.png", "wb") as img_file:
            img_file.write(img_response.content)
            print(f"Downloaded {symbol}.png")

print("Finished downloading symbol images.")

