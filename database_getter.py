import json
import requests
import sys # sys.exit()
import os
import time

url_basic_recipes = 'https://api.datawars2.ie/gw2/v2/recipes'
url_mystic_recipes = 'http://gw2profits.com/json/v3'
url_items_tp_data = (   'https://api.datawars2.ie/gw2/v1/items/json?fields='
                        'id,name,buy_price,sell_price,roi,profit,'
                        '7d_buy_listed,7d_buy_sold,7d_sell_listed,7d_sell_sold,'
                        '1d_buy_listed,1d_buy_sold,1d_sell_listed,1d_sell_sold')
url_all_items_data = 'https://api.guildwars2.com/v2/items'

database_directory_name = 'database/'
file_name_basic_recipes = database_directory_name + 'basic_recipes.json'
file_name_mystic_recipes = database_directory_name + 'mystic_recipes.json'
file_name_tp_data = database_directory_name + 'tp_data.json'
file_name_all_items_data = database_directory_name + 'all_items_data.json'
file_name_crafting_profit = database_directory_name + 'crafting_profit.json'
file_name_instant_crafting_profit = database_directory_name + 'instant_crafting_profit.json'
file_name_crafting_profit_csv = database_directory_name + 'crafting_profit.csv'

def get_json_from_url_to_file(url, file_name):
    result = requests.get(url)

    if(result.status_code == 200):
        data = result.json()
        with open(file_name, 'w') as f:
            json.dump(data, f, indent = 4)
    else:
        sys.exit("Status code not 200")

# This function dowloads info about every item in the game
# from offical GW2 API. There is a lot of them so it needs
# to be done in chunks
def accumulate_all_items_data(file_name):
    page_number_iterator = 0
    page_size = 200 #200 is max value

    # First requests.get outside of the loop to extract X-Page-Total
    customized_url = url_all_items_data + '?page=' + str(page_number_iterator) + '&page_size=' + str(page_size)
    result = requests.get(customized_url)
    if(result.status_code == 200):
        number_of_pages = int(result.headers['X-Page-Total'])
        data = result.json()
    else:
        sys.exit("Status code not 200")

    # Rest of the requests in the loop
    for page_number_iterator in range(1, number_of_pages):
        print(str(page_number_iterator) + " / " + str(number_of_pages), end = '\r')

        customized_url = url_all_items_data + '?page=' + str(page_number_iterator) + '&page_size=' + str(page_size)
        result = requests.get(customized_url)
        if(result.status_code == 200):
            data.extend(result.json())

        else:
            sys.exit("Status code not 200")

    # print(json.dumps(data, indent=4))

    with open(file_name, 'w') as f:
        json.dump(data, f, indent = 4)

def get_databases(get_basic_recipes = False, get_mystic_recipes = False, get_tp_data = False, get_all_items_data = False):

    total_time_elapsed = 0

    # Create directory for database if it is not exist
    if(not os.path.isdir(database_directory_name)):
        os.mkdir(database_directory_name)

    if(get_basic_recipes):
        print("GETTING BASIC RECIPES")
        start = time.time()
        get_json_from_url_to_file(url_basic_recipes, file_name_basic_recipes)
        end = time.time()
        total_time_elapsed += end - start
        print("Time elapsed: " + str(round(end - start, 2)))

    if(get_mystic_recipes):
        print("GETTING MYSTIC RECIPES")
        start = time.time()
        get_json_from_url_to_file(url_mystic_recipes, file_name_mystic_recipes)
        end = time.time()
        total_time_elapsed += end - start
        print("Time elapsed: " + str(round(end - start, 2)))

    if(get_tp_data):
        print("GETTING TP DATA")
        start = time.time()
        get_json_from_url_to_file(url_items_tp_data, file_name_tp_data)
        end = time.time()
        total_time_elapsed += end - start
        print("Time elapsed: " + str(round(end - start, 2)))

    if(get_all_items_data):
        print("GETTING ALL ITEMS DATA")
        start = time.time()
        accumulate_all_items_data(file_name_all_items_data)
        end = time.time()
        total_time_elapsed += end - start
        print("Time elapsed: " + str(round(end - start, 2)))

    # print("Total time elapsed: " + str(total_time_elapsed)) 



if __name__ == "__main__":
    get_databases(get_basic_recipes = True, get_mystic_recipes = True, get_tp_data = True, get_all_items_data = True)
    # get_databases(get_tp_data = True)

'''
## Usefull links ##
- API with database of all basic recipes
    https://api.datawars2.ie/gw2/v2/recipes
- API with mystic forge recipes and vendor prices and more
	http://www.gw2profits.com/json

    more info here: http://www.gw2profits.com/json
    
- API with current prices and also every information about items (here just example)
    https://api.datawars2.ie/gw2/v1/items/json?fields=name_fr,id,name
- Available keys for above query
    https://api.datawars2.ie/gw2/v1/items/keys

- All items database
    https://api.guildwars2.com/v2/items?page=0&page_size=200
'''