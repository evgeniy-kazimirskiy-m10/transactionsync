import psycopg2
import pandas as pd

# Database connection details
db1_conn_details = {
    'dbname': 'account-balance',
    # Fill in the following details for the first database
    'user': '',
    'password': '',
    'host': '',
    'port': ''
}

db2_conn_details = {
    'dbname': 'business_account_service',
    # Fill in the following details for the second database
    'user': '',
    'password': '',
    'host': '',
    'port': ''
}

# List of user_id and operation_time tuples
users_and_dates = [
    ('partners-topaz-d6219e', '2024-07-19 11:45:01.050000 +00:00'),
    ('partners-goalplus-mkn', '2024-06-24 13:15:06.996000 +00:00'),
    ('partners-azerimed-mkn', '2024-07-12 04:50:40.211000 +00:00'),
    ('partners-binance-mkng', '2024-06-07 09:27:54.159000 +00:00'),
    ('online-acq-prod-check', '2024-05-21 11:05:31.201000 +00:00'),
]

# SQL query to fetch data from the first database
sql_query = """
SELECT 
    operation_id, 
    operation_type, 
    operation_time, 
    operation_status, 
    user_id, 
    amount, 
    currency, 
    from_masked_card_number, 
    from_account_id, 
    to_account_id, 
    from_wallet_id, 
    to_wallet_id, 
    wallet_id, 
    account_id, 
    payment_operation_type, 
    external_operation_id, 
    original_operation_id, 
    category, 
    from_phone, 
    to_phone, 
    merchant_name 
FROM accounts_service.operation_history
WHERE user_id = %s
  AND operation_time < %s
ORDER BY operation_time DESC
"""

# Connect to the first database and fetch data
def fetch_data_from_db1(user_id, operation_time):
    conn = psycopg2.connect(**db1_conn_details)
    df = pd.read_sql_query(sql_query, conn, params=(user_id, operation_time))
    conn.close()
    return df

# Map and transform data
def transform_data(df):
    mapping = {
        'operation_id': 'id',
        'operation_type': 'type',
        'operation_time': 'transaction_time',
        'operation_status': 'status',
        'user_id': 'user_id',
        'amount': 'amount',
        'currency': 'currency',
        'from_masked_card_number': 'from_masked_card_number',
        'from_account_id': 'from_account_id',
        'to_account_id': 'to_account_id',
        'from_wallet_id': 'from_wallet_id',
        'to_wallet_id': 'to_wallet_id',
        'wallet_id': 'wallet_id',
        'account_id': 'account_id',
        'payment_operation_type': 'payment_transaction_type',
        'external_operation_id': 'order_id',
        'original_operation_id': 'original_transaction_id',
        'category': 'category',
        'from_phone': 'from_phone',
        'to_phone': 'to_phone',
        'merchant_name': 'merchant_name'
    }

    df_mapped = df.rename(columns=mapping)

    # Drop columns not in the target table
    df_mapped = df_mapped[list(mapping.values())]

    return df_mapped

# Insert transformed data into the second database
def insert_data_into_db2(df):
    conn = psycopg2.connect(**db2_conn_details)
    cursor = conn.cursor()

    for idx, row in df.iterrows():
        if idx % 100 == 0:
            print(f"Inserting row {idx}/{len(df)}")
        try:
            cursor.execute("""
            INSERT INTO business_account_service.transaction (
                id, type, status, amount, net_amount, currency, user_id, wallet_id,
                account_id, from_account_id, from_wallet_id, from_phone, to_account_id,
                to_wallet_id, to_phone, payment_transaction_type, order_id, original_transaction_id,
                category, merchant_name, transaction_time
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                row['id'], row['type'], row['status'], row['amount'], None, row['currency'],
                row['user_id'], row['wallet_id'], row['account_id'], row['from_account_id'],
                row['from_wallet_id'], row['from_phone'], row['to_account_id'], row['to_wallet_id'],
                row['to_phone'], row['payment_transaction_type'], row['order_id'], row['original_transaction_id'],
                row['category'], row['merchant_name'], row['transaction_time']
            ))
        except Exception as e:
            print(f"Error inserting row {idx}: {row}")
            print(e)

    conn.commit()
    cursor.close()
    conn.close()

# Main function to run the script
def main():
    for user_id, operation_time in users_and_dates:
        # Step 1: Fetch data from the first database
        df = fetch_data_from_db1(user_id, operation_time)
        print(f'Fetched data for user {user_id} from the first database')

        # Step 2: Transform and map data
        df_transformed = transform_data(df)
        print(f'Transformed data for user {user_id}')

        # Step 3: Insert transformed data into the second database
        print(f'Inserting data into the second database for user {user_id}')
        insert_data_into_db2(df_transformed)
        print(f'Inserted data into the second database for user {user_id}')

if __name__ == "__main__":
    main()
