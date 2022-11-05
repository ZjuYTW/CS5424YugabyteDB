from distutils.errors import LinkError
import pandas as pd
import csv

warehouse_header = ["W_ID", "W_NAME", "W_STREET_1", "W_STREET_2", "W_CITY", "W_STATE", "W_ZIP", "W_TAX", "W_YTD"]
district_header = ["D_W_ID", "D_ID", "D_NAME", "D_STREET_1", "D_STREET_2", "D_CITY", "D_STATE", "D_ZIP", "D_TAX", "D_YTD", "D_NEXT_O_ID"]
customer_header = ["C_W_ID", "C_D_ID", "C_ID", "C_FIRST", "C_MIDDLE", "C_LAST", "C_STREET_1", "C_STREET_2", "C_CITY", "C_STATE", "C_ZIP", "C_PHONE", "C_SINCE", "C_CREDIT", "C_CREDIT_LIM", "C_DISCOUNT", "C_BALANCE", "C_YTD_PAYMENT", "C_PAYMENT_CNT", "C_DELIVERY_CNT", "C_DATA"]
orders_header = ["O_W_ID", "O_D_ID", "O_ID", "O_C_ID", "O_CARRIER_ID", "O_OL_CNT", "O_ALL_LOCAL", "O_ENTRY_D"]
order_max_quantity_header = ["O_W_ID", "O_D_ID", "O_ID", "MAX_QUANTITY", "ITEM_IDS"]
order_non_delivery_head = ["O_W_ID", "O_D_ID", "O_ID", "O_C_ID"]
orderline_header = ["OL_W_ID", "OL_D_ID", "OL_O_ID", "OL_NUMBER", "OL_I_ID", "OL_DELIVERY_D", "OL_AMOUNT", "OL_SUPPLY_W_ID", "OL_QUANTITY", "OL_DIST_INFO"]
stock_header = ["S_W_ID", "S_I_ID", "S_QUANTITY", "S_YTD", "S_ORDER_CNT", "S_REMOTE_CNT", "S_DIST_01", "S_DIST_02", "S_DIST_03", "S_DIST_04", "S_DIST_05", "S_DIST_06", "S_DIST_07", "S_DIST_08", "S_DIST_09", "S_DIST_10", "S_DATA"]
item_header = ["I_ID", "I_NAME", "I_PRICE", "I_IM_ID", "I_DATA"]

def process_customer_data(path, output_path):
    df = pd.read_csv(path, header=None)
    df[14] = df[14].apply(lambda x: int(x * 100)) # C_CREDIT_LIM
    df[15] = df[15].apply(lambda x: int(x * 10000)) # C_DISCOUNT
    df[16] = df[16].apply(lambda x: int(x * 100)) # C_BALANCE
    df[17] = df[17].apply(lambda x: int(x * 100)) # C_YTD_PAYMENT
    # save result
    df.to_csv(output_path, index=False, header = False)

def process_warehouse_data(path, output_path):
    df = pd.read_csv(path, header=None)
    df[7] = df[7].apply(lambda x: int(x * 10000)) # W_TAX
    df[8] = df[8].apply(lambda x: int(x * 100)) # W_YTD
    # save result
    df.to_csv(output_path, index=False, header = False)

def process_district_data(path, output_path):
    df = pd.read_csv(path, header=None)
    df[8] = df[8].apply(lambda x: int(x * 10000)) # D_TAX
    df[9] = df[9].apply(lambda x: int(x * 100)) # D_YTD
    # save result
    df.to_csv(output_path, index=False, header = False)

def process_orderline_data(path, output_path):
    df = pd.read_csv(path, header=None)
    df[6] = df[6].apply(lambda x: int(x * 100)) # OL_AMOUNT
    # save result
    df.to_csv(output_path, index=False, header = False)

def process_item_data(path, output_path):
    df = pd.read_csv(path, header=None)
    df[2] = df[2].apply(lambda x: int(x * 100)) # I_PRICE
    # save result
    df.to_csv(output_path, index=False, header = False)

def process_stock_data(path, output_path):
    df = pd.read_csv(path, header=None)
    df[3] = df[3].apply(lambda x: int(x * 100)) # S_YTD
    # save result
    df.to_csv(output_path, index=False, header = False)


def join_customer_district(path_customer, path_district, output_path):
    df_customer = pd.read_csv(path_customer, names=customer_header)
    df_district = pd.read_csv(path_district, names=district_header)

    # join on C_W_ID and D_W_ID
    print("Joining customer and district tables...")
    df = pd.merge(df_customer, df_district, left_on="C_D_ID", right_on="D_ID")
    df.to_csv(output_path, index=False)

def join_orders_orderline(path_orders, path_orderline, output_path):
    df_orders = pd.read_csv(path_orders, names=orders_header)
    df_orderline = pd.read_csv(path_orderline, names=orderline_header)

    # join on O_W_ID, O_D_ID, O_ID and OL_W_ID, OL_D_ID, OL_O_ID
    print("Joining orders and orderline tables...")
    df = pd.merge(df_orders, df_orderline, left_on=["O_W_ID", "O_D_ID", "O_ID"], right_on=["OL_W_ID", "OL_D_ID", "OL_O_ID"])
    df.to_csv(output_path, index=False)

def join_stock_item(path_stock, path_item, output_path):
    df_stock = pd.read_csv(path_stock, names=stock_header)
    df_item = pd.read_csv(path_item, names=item_header)

    # join on S_I_ID and I_ID
    print("Joining stock and item tables...")
    df = pd.merge(df_stock, df_item, left_on="S_I_ID", right_on="I_ID")
    df.to_csv(output_path, index=False)

def process_order_max_quantity(path_order_line, output_path):
    df_orderline = pd.read_csv(path_order_line, names=orderline_header)
    # (o_w_id, o_d_id, o_id), max_quantity, item_ids
    def func(data):
        max_quantity = max([line for line in data["OL_QUANTITY"]])
        # print("Max Quantity: " , max_quantity)
        ids = []
        total_amount = 0
        for i in range(data.values.shape[0]):
            line = data.values[i]
            if(line[3] == max_quantity):
                ids.append(str(int(line[4])))
            total_amount += line[5] * 100 
        data["item_ids"] = '{' + ",".join(ids) + '}'
        data.drop("OL_I_ID", axis=1, inplace = True)
        data.drop("OL_AMOUNT", axis=1, inplace = True)
        data = data.drop_duplicates()
        data["total_amount"] = int(total_amount)
        # data = data.reset_index(drop=True)
        return data
    tmp_df = df_orderline[['OL_W_ID', 'OL_D_ID', 'OL_O_ID', "OL_QUANTITY", "OL_I_ID", "OL_AMOUNT"]].groupby(['OL_W_ID', 'OL_D_ID', 'OL_O_ID'], as_index=False, group_keys=False).apply(func)
    tmp_df.to_csv(output_path, index = False, header = False)

def process_order_non_delivery(path_order, output_path):
    df_order = pd.read_csv(path_order, names=orders_header)
    print("Filter on orders where o_carrier_id = null")
    df = df_order.loc[df_order['O_CARRIER_ID'].isnull()].filter(items=['O_W_ID', 'O_D_ID', 'O_ID', 'O_C_ID'])
    print(df)
    df.to_csv(output_path, index=False, header=False)

def main():
    data_path = "./data/data_files/"
    join_customer_district(data_path + "customer.csv", data_path + "district.csv", data_path + "customer_district.csv")
    join_orders_orderline(data_path + "orders.csv", data_path + "orderline.csv", data_path + "orders_orderline.csv")
    join_stock_item(data_path + "stock.csv", data_path + "item.csv", data_path + "stock_item.csv")

if __name__ == '__main__':
    # process_warehouse_data('./data/data_files/warehouse.csv', './data/data_files/warehouse_cql.csv')
    # process_district_data('./data/data_files/district.csv', './data/data_files/district_cql.csv')
    # process_customer_data('./data/data_files/customer.csv', './data/data_files/customer_cql.csv')
    # process_orderline_data('./data/data_files/order-line.csv', './data/data_files/order-line_cql.csv')
    process_order_max_quantity('./data/data_files/order-line.csv', './data/data_files/order_max_quantity_cql.csv')
    # process_item_data('./data/data_files/item.csv', './data/data_files/item_cql.csv')
    # process_stock_data('./data/data_files/stock.csv', './data/data_files/stock_cql.csv')
    # process_order_non_delivery('./data/data_files/order.csv', './data/data_files/order_non_delivery_cql.csv')

    # join_stock_item('./data/data_files/stock.csv', './data/data_files/item.csv', './data/data_files/stock_item.csv')

    # main()
