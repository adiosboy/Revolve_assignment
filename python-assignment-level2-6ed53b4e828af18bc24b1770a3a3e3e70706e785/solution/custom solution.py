import argparse
import pandas as pd
import numpy as np
import json
import glob
from tqdm import tqdm
import natsort
import os
import logging

# define logging configurations
logging.basicConfig(level = logging.DEBUG,
                    format = '%(asctime)s:%(levelname)s:%(name)s:%(message)s')

def get_params() -> dict:
    parser = argparse.ArgumentParser(description='DataTest')
    parser.add_argument('--customers_location', required=False, default="./input_data/starter/customers.csv")
    parser.add_argument('--products_location', required=False, default="./input_data/starter/products.csv")
    parser.add_argument('--transactions_location', required=False, default="./input_data/starter/transactions/")
    parser.add_argument('--output_location', required=False, default="./output_data/outputs/")
    return vars(parser.parse_args())

def process_data(customers_location:str, products_location:str, transactions_location:str, output_location:str) -> None:
    # read the different data files
    products = pd.read_csv(products_location)
    customers = pd.read_csv(customers_location)
    transaction_files = glob.glob(os.path.join(transactions_location, "*/transactions.json"))

    logging.info("There are {} products, {} customers and {} transaction files".format(products.shape[0], customers.shape[0], len(transaction_files)))

    # read and append to a list the dictionary of all transactions across different dates
    all_transactions = []
    for a_file in tqdm(transaction_files):
        one_file_data = open(a_file).read().split("\n")
        for trans in one_file_data:
            # in case the one_file_data is empty, pass
            try:
                trans_obj = json.loads(trans)
                all_transactions.append(trans_obj)
            except Exception as e:
                pass

    # convert the dictionary of transactions at customer level into a list of transactions at customer cross product level
    transactions_at_product_level = []
    for transaction in tqdm(all_transactions):
        customer_id = transaction['customer_id']
        date_of_purchase = transaction['date_of_purchase']
        for item in transaction['basket']:
            product_id = item['product_id']
            price = item['price']
            transactions_at_product_level.append([date_of_purchase, customer_id, product_id, price])

    # convert list of transactions into a pandas dataframe for easier processing
    transactions_at_product_level = pd.DataFrame(transactions_at_product_level, columns=['date_of_purchase', 'customer_id', 'product_id', 'price'])

    # get a count of transactions at user cross product level, sort first by customer_id and then product_id
    transactions_grouped_by_customer_id_and_product_id = transactions_at_product_level.groupby(['customer_id', 'product_id']).agg(purchase_count=('price', 'count')).reset_index().sort_values(by=["customer_id", "product_id"], key=natsort.natsort_keygen())

    # make a left join with customers to get loyalty score
    transactions_grouped_by_customer_id_and_product_id_with_loyalty_score = transactions_grouped_by_customer_id_and_product_id.merge(customers, on='customer_id', how='left')

    # make a left join with products to get product category
    transactions_grouped_by_customer_id_and_product_id_with_loyalty_score_with_category = transactions_grouped_by_customer_id_and_product_id_with_loyalty_score.merge(products, on='product_id', how='left')
    final_results = transactions_grouped_by_customer_id_and_product_id_with_loyalty_score_with_category[['customer_id', 'loyalty_score', 'product_id', 'product_category', 'purchase_count']]

    # if output directory does not exist, create it
    if not os.path.isdir(output_location):
        os.makedirs(output_location)
    dest_path = os.path.join(output_location, "final.csv")

    # write the dataframe to the destination as a csv file
    final_results.to_csv(dest_path, index=None)

def main():
    # get command line arguments 
    params = get_params()

    # call custom function to process data
    process_data(params['customers_location'], params['products_location'], params['transactions_location'], params['output_location'])

if __name__ == "__main__":
    main()

# go to root folder (you should be able to see the solutions folder from the root folder)
# run following command to generate the output, provide command line arguments if necessary
# python solutions/custom_solution.py --customers_location ./starter/customers.csv --products_location ./starter/products.csv --transactions_location ./starter/transactions --output_location ./output_data/outputs/