import csv
import psycopg2
import psycopg2.extras

from tqdm import tqdm
import itertools

config = {
    'host': "127.0.0.1",
    'port': '5433',
    'dbName': 'yugabyte',
    'dbUser': 'yugabyte',
    'dbPassword': 'yugabyte',
    'sslMode': '',
    'sslRootCert': ''
}

data_cfg = [
    {   
        'name': 'warehouse',
        'file': 'data/data_files/warehouse.csv',
        'table': 'data/table_files/warehouse.csv',
        'foreign_keys': []
    },{ 
        'name': 'district',
        'file': 'data/data_files/district.csv',
        'table': 'data/table_files/district.csv',
        'foreign_keys': ["FOREIGN KEY (D_W_ID) REFERENCES warehouse(W_ID)"]
    },{
        'name': 'customer',
        'file': 'data/data_files/customer.csv',
        'table': 'data/table_files/customer.csv',
        'foreign_keys': ["FOREIGN KEY (C_W_ID, C_D_ID) REFERENCES district(D_W_ID, D_ID)"]
    },{ 
        'name': 'orders',
        'file': 'data/data_files/order.csv',
        'table': 'data/table_files/order.csv',
        'foreign_keys': ["FOREIGN KEY (O_W_ID, O_D_ID, O_C_ID) REFERENCES customer(C_W_ID, C_D_ID, C_ID)"]
    },{ 
        'name': 'item',
        'file': 'data/data_files/item.csv',
        'table': 'data/table_files/item.csv',
        'foreign_keys': []
    },{
        'name': 'order-line',
        'file': 'data/data_files/order-line.csv',
        'table': 'data/table_files/order-line.csv',
        'foreign_keys': ["FOREIGN KEY (OL_W_ID,OL_D_ID,OL_O_ID) REFERENCES orders(O_W_ID,O_D_ID,O_ID)",
                         "FOREIGN KEY (OL_I_ID) REFERENCES item(I_ID)"]
    },{
        'name': 'stock',
        'file': 'data/data_files/stock.csv',
        'table': 'data/table_files/stock.csv',
        'foreign_keys': ["FOREIGN KEY (S_W_ID) REFERENCES warehouse(W_ID)",
                         "FOREIGN KEY (S_I_ID) REFERENCES item(I_ID)"]
    }]


def parse_table(file, name):
    """
    parse table from csv file and return CREATE TABLE statement
    """
    with open(file, 'r') as f:
        reader = csv.reader(f)
        headers = [row for row in reader]
    
    create_table_stmt = f"CREATE TABLE IF NOT EXISTS {name} (\n"
    fields = ",\n".join([" ".join(header) for header in headers])
    create_table_stmt += fields + "\n)"
    return create_table_stmt

def create_table(yb, create_table_stmt, name):
    try:
        with yb.cursor() as yb_cursor:
            yb_cursor.execute(f"DROP TABLE IF EXISTS {name} CASCADE;")
            yb_cursor.execute(create_table_stmt)
        yb.commit()
    except Exception as e:
        print("Exception while creating tables")
        print(e)
        exit(1)

    print(">>>> Successfully created table.")

def alter_table(yb, foreign_keys, name):
    if len(foreign_keys) == 0:
        return

    alter_table_stmt = f"ALTER TABLE {name}\n"
    alter_table_stmt += ",\n".join(["ADD " + key for key in foreign_keys]) + ";"
    print(alter_table_stmt)
    try:
        with yb.cursor() as yb_cursor:
            yb_cursor.execute(alter_table_stmt)
        yb.commit()
    except Exception as e:
        print("Exception while altering tables")
        print(e)
        exit(1)

    print(">>>> Successfully altered table.")

def parse_data(data, name):
    """
    parse data from csv file and return INSERT INTO statement
    """
    for i in range(len(data)):
        if not data[i].isnumeric():
            if data[i] == "null":
                data[i] = "NULL"
            else:
                data[i] = f"'{data[i]}'"
    insert_stmt = f"INSERT INTO {name} VALUES (\n"
    fields = ", ".join(data)

    insert_stmt += fields + "\n);"
    return insert_stmt

def write_data(yb, cfg):
    with open(cfg['file'], 'r') as f:
        reader = csv.reader(f)
        try:
            for row in tqdm(reader):
                insert_stmt = parse_data(row, cfg['name'])
                with yb.cursor() as yb_cursor:
                    yb_cursor.execute(insert_stmt)
            yb.commit()
        except Exception as e:
            print("Exception while writing data")
            print(e)
            exit(1)
    
    print(f">>>> Successfully wrote data for {cfg['name']}.")

def batch_read(reader, batch_size):
    while True:
        lines = list(itertools.islice(reader, batch_size))
        if not lines:
            break
        yield lines

def parse_data_multi(lines, name):
    """
    parse data from csv file and return INSERT INTO statement
    """
    for line in lines:
        for i in range(len(line)):
            if not line[i].isnumeric():
                if line[i] == "null":
                    line[i] = "NULL"
                else:
                    line[i] = f"'{line[i]}'"

    insert_stmt = f"INSERT INTO {name} VALUES \n"
    fields = []
    for line in lines:
        fields.append("(" + ", ".join(line) + ")")
    fields = ",\n".join(fields)
    insert_stmt += fields + ";"
    return insert_stmt

def write_data_multi(yb, cfg):
    with open(cfg['file'], 'r') as f:
        reader = csv.reader(f)
        try:
            for row in tqdm(batch_read(reader, 30)):
                insert_stmt = parse_data_multi(row, cfg['name'])
                with yb.cursor() as yb_cursor:
                    yb_cursor.execute(insert_stmt)
            yb.commit()
        except Exception as e:
            print("Exception while writing data")
            print(e)
            exit(1)
    
    print(f">>>> Successfully wrote data for {cfg['name']}.")

def main(conf):
    print(">>>> Connecting to YugabyteDB!")

    try:
        if conf['sslMode'] != '':
            yb = psycopg2.connect(host=conf['host'], port=conf['port'], database=conf['dbName'],
                                  user=conf['dbUser'], password=conf['dbPassword'],
                                  sslmode=conf['sslMode'], sslrootcert=conf['sslRootCert'],
                                  connect_timeout=10)
        else:
            yb = psycopg2.connect(host=conf['host'], port=conf['port'], database=conf['dbName'],
                                  user=conf['dbUser'], password=conf['dbPassword'],
                                  connect_timeout=10)
    except Exception as e:
        print("Exception while connecting to YugabyteDB")
        print(e)
        exit(1)

    print(">>>> Successfully connected to YugabyteDB!")
    
    for cfg in data_cfg:
        print(f">>>> Creating table for {cfg['name']}")
        create_table_stmt = parse_table(cfg['table'], cfg['name'])
        create_table(yb, create_table_stmt, cfg['name'])
        print(f">>>> Writing data for {cfg['name']}")
        # write_data(yb, cfg)
        write_data_multi(yb, cfg)
        print(f">>>> Altering table for {cfg['name']}")
        alter_table(yb, cfg['foreign_keys'], cfg['name'])

    yb.close()

if __name__ == "__main__":
    main(config)
    # parse_table('data/table_files/item.csv')
