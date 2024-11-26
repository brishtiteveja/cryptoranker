import requests
import time

# Fetch categories
categories_url = 'https://api.coingecko.com/api/v3/coins/categories'
categories_response = requests.get(categories_url)
categories_data = categories_response.json()

# Fetch all coins (limited to a smaller number for this example)
coins_url = 'https://api.coingecko.com/api/v3/coins/list'
coins_response = requests.get(coins_url)
coins_data = coins_response.json()

# Limiting the number of coins to avoid hitting rate limits
limited_coins_data = coins_data[:10]  # Adjust the limit as necessary

# Initialize the hashmap
category_map = {category['id']: [] for category in categories_data}

# Fetch coin details and map to categories
for coin in limited_coins_data:
    coin_id = coin['id']
    coin_details_url = f'https://api.coingecko.com/api/v3/coins/{coin_id}'
    try:
        coin_details_response = requests.get(coin_details_url)
        coin_details_data = coin_details_response.json()
        
        # Some coins might not have the 'categories' field
        coin_categories = coin_details_data.get('categories', [])
        
        for category_id in category_map.keys():
            if category_id in coin_categories:
                category_map[category_id].append({
                    'id': coin_id,
                    'symbol': coin['symbol'],
                    'name': coin['name']
                })
                
        # Be respectful to the API's rate limit
        time.sleep(1)
    except Exception as e:
        print(f"Error fetching details for coin {coin_id}: {e}")

# Print or use the category_map as needed
for category, coins in category_map.items():
    print(f"Category: {category}, Coins: {coins}")
