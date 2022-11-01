import csv
import psycopg2
import psycopg2.extras

from tqdm import tqdm
import itertools

config = {
    'host': "192.168.48.244",
    'port': '5433',
    'dbName': 'yugabyte',
    'dbUser': 'yugabyte',
    'dbPassword': 'yugabyte',
    'sslMode': '',
    'sslRootCert': ''
}

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
    
    with yb.cursor() as yb_cursor:
        for i in range(1, 11):
            test_query = f"SELECT MIN(O_ID) as O_ID FROM orders WHERE O_W_ID = {9} AND O_D_ID " + \
                f"= {i} AND O_CARRIER_ID is NULL"
            result = yb_cursor.execute(test_query)
            print(result)
    yb.commit()

    yb.close()

if __name__ == "__main__":
    main(config)
    # parse_table('data/table_files/item.csv')
