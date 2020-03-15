import os
import tensorflow as tf
import pandas as pd
from addressnet.predict import predict_one, predict


def get_gnaf_dataset_labels():
    labels_list = [
        'building_name',  # 1
        'level_number_prefix',  # 2
        'level_number',  # 3
        'level_number_suffix',  # 4
        'level_type',  # 5
        'flat_number_prefix',  # 6
        'flat_number',  # 7
        'flat_number_suffix',  # 8
        'flat_type',  # 9
        'number_first_prefix',  # 10
        'number_first',  # 11
        'number_first_suffix',  # 12
        'number_last_prefix',  # 13
        'number_last',  # 14
        'number_last_suffix',  # 15
        'street_name',  # 16
        'street_suffix',  # 17
        'street_type',  # 18
        'locality_name',  # 19
        'state',  # 20
        'postcode'  # 21
    ]
    return labels_list


if __name__ == "__main__":
    print(tf.__version__)
    print(predict_one("casa del gelato, 10A 24-26 high street road mount waverley vic 3183"))

    # load CSV
    df_in = pd.read_csv(os.path.join(os.getcwd(), 'data/full_address.csv'), header=0)
    print(df_in.shape)
    # get a list of addresses to parse
    addresses_to_parse = df_in['FullAddress'].tolist()
    # make predictions
    print('Parsing addresses...')
    parsed_addresses = predict(addresses_to_parse)
    # save predictions into a dataframe
    df_out = pd.DataFrame()
    idx = 0
    for parsed_address_dict in parsed_addresses:
        print('\nidx:', idx)
        print('input:', addresses_to_parse[idx])
        print('output:', parsed_address_dict)
        df_address = pd.DataFrame(parsed_address_dict, index=[0])
        df_out = df_out.append(df_address, ignore_index=True)
        idx += 1
    # reorder columns
    df_out = df_out.reindex(get_gnaf_dataset_labels(), axis=1)
    # reset index
    df_out = df_out.reset_index(drop=True)
    # add inputs back
    df_out = pd.concat([df_in, df_out], axis=1)

    # export data to CSV
    df_out.to_csv('data/outputs.csv', index=False, header=True)
    print(df_out.shape)

    # write data to SQL table
    import pyodbc
    from sqlalchemy import create_engine
    server_name = 'DATA05'
    database_name = 'dbPriceFinderRental'
    table_name = 'STAGING.AddressNet'
    engine = create_engine('mssql+pyodbc://' + server_name + '/' + database_name
                           + '?driver=SQL Server?Trusted_Connection=yes')
    conn = engine.connect()
    df_out.to_sql(table_name, con=engine, index=False, if_exists='replace')
    conn.close()
