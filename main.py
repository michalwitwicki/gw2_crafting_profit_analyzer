import pandas as pd 
import json
import sys # sys.exit() and command line arguments
import copy # deepcopy()
import database_getter as dg
import time

tp_data_df = pd.DataFrame()
recipes_df = pd.DataFrame()
all_items_df = pd.DataFrame()

crafting_profit = []

# Column names
ITEM_ID =                       'item_id'
ITEM_NAME =                     'item_name'
SELL_PRICE =                    'sell_price'
BUY_PRICE =                     'buy_price'
FLIP_PROFIT =                   'flip_profit'
FLIP_ROI =                      'flip_roi'
CRAFTING_PRICE =                'crafting_price'
CRAFTING_PROFIT =               'crafting_profit'
CRAFTING_PROFIT_ESTIMATE =      'crf_prf_estimate'
CRAFTING_ROI =                  'crafting_roi'
CRAFTING_PROFIT_INSTANT_SELL =  'crafting_profit_INS'
CRAFTING_ROI_INSTANT_SELL =     'crafting_roi_INS'

SELL_SOLD_7D =                  '7d_sell_sold'
SELL_SOLD_1D =                  '1d_sell_sold'
SELL_LISTED_7D =                '7d_sell_listed'
SELL_LISTED_1D =                '1d_sell_listed'

BUY_SOLD_7D =                   '7d_buy_sold'
BUY_SOLD_1D =                   '1d_buy_sold'
BUY_LISTED_7D =                 '7d_buy_listed'
BUY_LISTED_1D =                 '1d_buy_listed'

SELL_SOLD_DIFF_7D =             '7d_SL_diff'
SELL_SOLD_DIFF_1D =             '1d_SL_diff'
SELL_SOLD_1D_5_PERCENT =        '1d_5%_SS'
BUY_SOLD_1D_5_PERCENT =         '1d_5%_BS'
DISCIPLINES =                   'disciplines'
DISCIPLINES_STR =               'disciplines_str'
DIFFICULT =                     'difficult'
UNOBTAINABLE =                  'unobtainable'

GET_FROM_TP_PRICE = ""
OUTPUT_FILE_NAME = ""

# This function compares buy vs craft cost
# from database and returns lowest
# If item doesn't exists in DB returns 0
# IF item is already registered but is not obtainable return -1
def get_lowest_price(item_id):
    # Extract item from whole list of items
    item_data = list(filter(lambda item: item[ITEM_ID] == item_id, crafting_profit))

    # Check if given item is already registered
    if(not item_data):
        # Item is not yet registered so return 0
        return 0

    if(len(item_data) > 1):
        print("mamy ten przypadek", len(item_data))
        print(item_data)
        input("wait...")

    if((UNOBTAINABLE, True) in item_data[0].items()):
        # print(item_data)

        # Item cant be obtained so return -1
        return -1

    if(CRAFTING_PRICE in item_data[0] and GET_FROM_TP_PRICE in item_data[0] and item_data[0][GET_FROM_TP_PRICE] != 0):
        if(item_data[0][CRAFTING_PRICE] < item_data[0][GET_FROM_TP_PRICE]):
            return item_data[0][CRAFTING_PRICE]
        else:
            return item_data[0][GET_FROM_TP_PRICE]

    elif(GET_FROM_TP_PRICE in item_data[0] and item_data[0][GET_FROM_TP_PRICE] != 0):
        return item_data[0][GET_FROM_TP_PRICE]

    elif(CRAFTING_PRICE in item_data[0]):
        return item_data[0][CRAFTING_PRICE]


    return -1

# This is recursive function returning lowest price
# for getting item of given id. For now we consider
# 2 methods of "getting the item": buy on TP or craft
# so in the end we always must buy something on TP
# Returns 0 if item cant be obtained
def calculate_lowest_price(item_id, top_level_recipe):

    # Check if given item is already registered
    lowest_price = get_lowest_price(item_id)
    if(lowest_price > 0):
        return lowest_price
    elif(lowest_price == -1):
        return 0

    one_row = {}
    one_row[ITEM_ID] = item_id

    # Check if item ID exists in "all items DB"
    # Guild decorations have probelm with that
    # if(all_items_df.isin({"id": [item_id]}).any().any()):
    if(not all_items_df[(all_items_df.id == item_id)].empty):
        tmp = all_items_df.loc[all_items_df['id'] == item_id, 'name']
        one_row[ITEM_NAME] = tmp.iat[0]

    else:
        one_row[ITEM_NAME] = "Unknown"

    one_row[UNOBTAINABLE] = False
    one_row[DIFFICULT] = False

    current_recipe_crafting_price = 0
    current_recipe_all_ing_obtainable = True

    minimal_crafting_price = -1
    minimal_recipe_all_ing_obtainable = True

    buy_price = 0
    sell_price = 0
    get_from_tp_price = 0

    item_is_tradable = False

    current_disciplines = []

    # Check if item is tradable
    if(tp_data_df.isin({"id": [item_id]}).any().any()):

        try:
            # If it is - add trading data to crafting_profit
            one_row[BUY_PRICE] = int(tp_data_df.loc[tp_data_df['id'] == item_id, 'buy_price'].iat[0])
            buy_price = one_row[BUY_PRICE]

            one_row[SELL_PRICE] = int(tp_data_df.loc[tp_data_df['id'] == item_id, 'sell_price'].iat[0])
            sell_price = one_row[SELL_PRICE]
            
            # Save selected item price
            get_from_tp_price = one_row[GET_FROM_TP_PRICE]

            one_row[BUY_LISTED_7D] = int(tp_data_df.loc[tp_data_df['id'] == item_id, '7d_buy_listed'].iat[0])
            one_row[BUY_SOLD_7D] = int(tp_data_df.loc[tp_data_df['id'] == item_id, '7d_buy_sold'].iat[0])
            one_row[SELL_LISTED_7D] = int(tp_data_df.loc[tp_data_df['id'] == item_id, '7d_sell_listed'].iat[0])
            one_row[SELL_SOLD_7D] = int(tp_data_df.loc[tp_data_df['id'] == item_id, '7d_sell_sold'].iat[0])

            one_row[BUY_LISTED_1D] = int(tp_data_df.loc[tp_data_df['id'] == item_id, '1d_buy_listed'].iat[0])
            one_row[BUY_SOLD_1D] = int(tp_data_df.loc[tp_data_df['id'] == item_id, '1d_buy_sold'].iat[0])
            one_row[SELL_LISTED_1D] = int(tp_data_df.loc[tp_data_df['id'] == item_id, '1d_sell_listed'].iat[0])
            one_row[SELL_SOLD_1D] = int(tp_data_df.loc[tp_data_df['id'] == item_id, '1d_sell_sold'].iat[0])
            one_row[SELL_SOLD_1D_5_PERCENT] = int(one_row[SELL_SOLD_1D] * 0.05)
            one_row[BUY_SOLD_1D_5_PERCENT] = int(one_row[BUY_SOLD_1D] * 0.05)

            one_row[SELL_SOLD_DIFF_7D] = int(one_row[SELL_SOLD_7D] - one_row[SELL_LISTED_7D])
            one_row[SELL_SOLD_DIFF_1D] = int(one_row[SELL_SOLD_1D] - one_row[SELL_LISTED_1D])

            one_row[FLIP_PROFIT] = int(tp_data_df.loc[tp_data_df['id'] == item_id, 'profit'].iat[0])

            # Caution!!! This value is rounded
            one_row[FLIP_ROI] = round(tp_data_df.loc[tp_data_df['id'] == item_id, 'roi'].iat[0])
            item_is_tradable = True
        except:
            print("nono")

    # If not tradable and also this is top level recipe
    # there is no point in continuing that tree
    elif(top_level_recipe):
        return


    # Check if item is NOT craftable
    if(recipes_df[(recipes_df.output_item_id == item_id)].empty):

        # 0 here tells that crafting this item is not possible
        minimal_crafting_price = 0

        # Check if item is also not tradable 
        if(item_is_tradable == False):
            one_row[UNOBTAINABLE] = True
    else:
        # Sometimes different recipes can produce same item, here I handle this
        for recipe in recipes_df.loc[recipes_df['output_item_id'] == item_id].itertuples():

            current_recipe_crafting_price = 0
            current_recipe_all_ing_obtainable = True

            # Iterate over ingredients in one recipe and calculate crafting cost
            for ingredient in recipe.ingredients:
                returned_value = calculate_lowest_price(ingredient['item_id'], top_level_recipe = False) * ingredient['count']

                if(returned_value == 0):
                    current_recipe_all_ing_obtainable = False

                current_recipe_crafting_price += returned_value

            current_recipe_crafting_price /= recipe.output_item_count

            if(minimal_crafting_price == -1 or current_recipe_crafting_price < minimal_crafting_price):
                minimal_crafting_price = current_recipe_crafting_price
                minimal_recipe_all_ing_obtainable = current_recipe_all_ing_obtainable
                current_disciplines = recipe.disciplines

        # Add disciplines to the crafting_profit
        one_row[DISCIPLINES] = current_disciplines
        current_disciplines = ','.join(current_disciplines)
        one_row[DISCIPLINES_STR] = current_disciplines

        # Caution!!! This value is rounded
        one_row[CRAFTING_PRICE] = round(minimal_crafting_price)


        if(minimal_recipe_all_ing_obtainable == False):
            # It means that at least one ingredient can't be obtain easly 
            one_row[DIFFICULT] = True

            # If crafting price is zero it means that all ingredients are not obtainable (Fulgurite for example)
            if(item_is_tradable == False and minimal_crafting_price == 0):
                one_row[UNOBTAINABLE] = True

    # Calculate crafting profit and crafting ROI
    if(item_is_tradable and minimal_crafting_price != 0):

        # Caution!!! These values are rounded
        one_row[CRAFTING_PROFIT] = round(sell_price - round(sell_price * 0.05) - round(sell_price * 0.1) - minimal_crafting_price)
        one_row[CRAFTING_PROFIT_ESTIMATE] = one_row[CRAFTING_PROFIT] * one_row[SELL_SOLD_1D_5_PERCENT]
        one_row[CRAFTING_ROI] = round((one_row[CRAFTING_PROFIT]/minimal_crafting_price) * 100)

        # Instant sell profit calculation
        # Caution!!! These values are rounded
        one_row[CRAFTING_PROFIT_INSTANT_SELL] = round(buy_price - round(buy_price * 0.05) - round(buy_price * 0.1) - minimal_crafting_price)
        one_row[CRAFTING_ROI_INSTANT_SELL] = round((one_row[CRAFTING_PROFIT_INSTANT_SELL]/minimal_crafting_price) * 100)

    # Update data base
    crafting_profit.append(copy.deepcopy(one_row))
    one_row.clear()
    
    # Compare prices and return proper value
    if(get_from_tp_price != 0 and minimal_crafting_price != 0):
        if(get_from_tp_price < minimal_crafting_price):
            return get_from_tp_price
        else:
            return minimal_crafting_price

    elif(get_from_tp_price != 0 or minimal_crafting_price != 0):
        if(get_from_tp_price != 0):
            return get_from_tp_price
        else:
            return minimal_crafting_price

    elif(get_from_tp_price == 0 and minimal_crafting_price == 0):
        return 0

    else:
        sys.exit("Wut?")

    return 0

if __name__ == "__main__":

    # Check if there is one cmd line argument and if it matches excpected one
    if(len(sys.argv) == 2 and (sys.argv[1]== "-ins" or sys.argv[1] == "-instant")):
        GET_FROM_TP_PRICE = SELL_PRICE #instant option
        OUTPUT_FILE_NAME = dg.file_name_instant_crafting_profit
    else:
        GET_FROM_TP_PRICE = BUY_PRICE #patient option
        OUTPUT_FILE_NAME = dg.file_name_crafting_profit

    print("Starting time: " + time.strftime("%H:%M:%S", time.localtime()))

    # dg.get_databases(get_basic_recipes = True, get_mystic_recipes = True, get_tp_data = True, get_all_items_data = True)
    dg.get_databases(get_tp_data = True)

    tp_data_df = pd.read_json(dg.file_name_tp_data)
    recipes_df = pd.read_json(dg.file_name_basic_recipes)
    all_items_df = pd.read_json(dg.file_name_all_items_data)

    # calculate_lowest_price(80140, top_level_recipe = True)

    # sys.exit("DEBUG EXIT")

    print("CALCULATING ALL RECIPES WITH: " + GET_FROM_TP_PRICE)
    start = time.time()
    for i, item in enumerate(recipes_df.itertuples()):
        calculate_lowest_price(item.output_item_id, top_level_recipe = True)
        print(i, end = '\r')
        # if(i == 50):
        #     break

    end = time.time()
    print("Time elapsed: " + str(round(end - start, 2)))

    # Save crafting_profit to file
    with open(OUTPUT_FILE_NAME, 'w') as f:
        json.dump(crafting_profit, f, indent=4)

    # Convert crafting_profit in JSON to CSV
    # crafting_profit_df = pd.read_json(dg.file_name_crafting_profit)
    # crafting_profit_df.to_csv(dg.file_name_crafting_profit_csv)