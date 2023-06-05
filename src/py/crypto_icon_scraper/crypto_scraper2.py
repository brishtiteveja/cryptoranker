import requests
from bs4 import BeautifulSoup

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

for crypto in cryptos:
    print(crypto)
