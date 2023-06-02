import requests
import subprocess
import json
import time
import random

# Get your public IP address
def get_my_ip_address():
    ip = requests.get('https://api.ipify.org').text
    print('My public IP address is:', ip)

# List of countries to connect to
countries = ['US', 'UK', 'DE', 'JP', 'AU']

# Get list of coins from coingecko
def get_coins():
    response = requests.get('https://api.coingecko.com/api/v3/coins/list')
    coins = json.loads(response.text)
    print(coins)

def connect_to_vpn(country):
    subprocess.run(["protonvpn", "c", "--cc", country])

def disconnect_from_vpn():
    subprocess.run(["protonvpn", "d"])

while True:
    get_my_ip_address()
    get_coins()
    time.sleep(10) # Wait for a while before making the next request
    country = random.choice(countries) # Choose a random country
    connect_to_vpn(country)
