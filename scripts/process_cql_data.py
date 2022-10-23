import pandas as pd
import csv

def process_customer_data(path, output_path):
    df = pd.read_csv(path, header=None)
    df[15] = df[15].apply(lambda x: x * 100) # C_BALANCE

    # save result
    df.to_csv(output_path, index=False)

def process_orderline_data(path, output_path):
    df = pd.read_csv(path, header=None)
    # df[6] = df[6].apply(lambda x: x * 100) # OL_AMOUNT

    # # save result
    # df.to_csv(output_path, index=False)


if __name__ == '__main__':
    # process_customer_data('./data/data_files/customer.csv', './data/data_files/customer_cql.csv')
    process_orderline_data('./data/data_files/order-line.csv', './data/data_files/order-line_cql.csv')
