import psycopg2
import pandas as pd
from datetime import datetime

# Database connection details
db1_conn_details = {
    'dbname': 'account-balance',
    # Fill in the following details for the first database
    'user': 'app',
    'password': 'postgres',
    'host': 'localhost',
    'port': '5433'
}

db2_conn_details = {
    'dbname': 'business_account_service',
    # Fill in the following details for the second database
    'user': 'app',
    'password': 'postgres',
    'host': 'localhost',
    'port': '5433'
}

# List of user_id and operation_time tuples
users_and_dates = [
    ('partners-peeky-123456', '2025-10-17 11:45:01.050000 +00:00'),
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

    total_rows = len(df)
    success_count = 0
    failure_count = 0
    failed_rows = []

    print(f"Starting insert of {total_rows} rows into business_account_service.transaction")
    start_time = datetime.now()

    for idx, row in df.iterrows():
        if idx % 100 == 0:
            print(f"Progress: {idx}/{total_rows} rows processed ({(idx/total_rows*100):.1f}%) - Success: {success_count}, Failed: {failure_count}")
        try:
            cursor.execute("""
            INSERT INTO business_account_service.transaction (
                id, type, status, amount, net_amount, currency, user_id, wallet_id,
                account_id, from_account_id, from_wallet_id, from_phone, to_account_id,
                to_wallet_id, to_phone, payment_transaction_type, order_id, original_transaction_id,
                category, merchant_name, transaction_time
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                row['id'], row['type'], row['status'], row['amount'] / 100, row['amount'] / 100, row['currency'],
                row['user_id'], row['wallet_id'], row['account_id'], row['from_account_id'],
                row['from_wallet_id'], row['from_phone'], row['to_account_id'], row['to_wallet_id'],
                row['to_phone'], row['payment_transaction_type'], row['order_id'], row['original_transaction_id'],
                row['category'], row['merchant_name'], row['transaction_time']
            ))
            conn.commit()
            success_count += 1
        except Exception as e:
            failure_count += 1
            failed_rows.append({'index': idx, 'id': row.get('id', 'N/A'), 'error': str(e)})
            print(f"Error inserting row {idx} (id: {row.get('id', 'N/A')}): {e}")
            conn.rollback()

    cursor.close()
    conn.close()

    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()

    print(f"\n{'='*80}")
    print(f"INSERT SUMMARY:")
    print(f"  Total rows:      {total_rows}")
    print(f"  Successful:      {success_count} ({(success_count/total_rows*100):.1f}%)")
    print(f"  Failed:          {failure_count} ({(failure_count/total_rows*100):.1f}%)")
    print(f"  Duration:        {duration:.2f} seconds")
    print(f"  Throughput:      {(total_rows/duration):.1f} rows/sec")

    if failed_rows:
        print(f"\nFailed rows details:")
        for failed in failed_rows[:10]:  # Show first 10 failures
            print(f"  - Index {failed['index']}, ID: {failed['id']}, Error: {failed['error']}")
        if len(failed_rows) > 10:
            print(f"  ... and {len(failed_rows) - 10} more failures")
    print(f"{'='*80}\n")

    return {'total': total_rows, 'success': success_count, 'failed': failure_count, 'duration': duration}

# Main function to run the script
def main():
    print(f"\n{'='*80}")
    print(f"TRANSACTION SYNC JOB STARTED")
    print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Users to process: {len(users_and_dates)}")
    print(f"{'='*80}\n")

    job_start_time = datetime.now()
    overall_stats = {
        'total_users': len(users_and_dates),
        'total_fetched': 0,
        'total_inserted': 0,
        'total_failed': 0,
        'users_processed': []
    }

    for idx, (user_id, operation_time) in enumerate(users_and_dates, 1):
        print(f"\n[USER {idx}/{len(users_and_dates)}] Processing user: {user_id}")
        print(f"Operation time filter: {operation_time}")
        user_start_time = datetime.now()

        try:
            # Step 1: Fetch data from the first database
            print(f"[FETCH] Querying dev-account-balance database...")
            df = fetch_data_from_db1(user_id, operation_time)
            rows_fetched = len(df)
            print(f"[FETCH] Retrieved {rows_fetched} rows for user {user_id}")
            overall_stats['total_fetched'] += rows_fetched

            if rows_fetched == 0:
                print(f"[SKIP] No data found for user {user_id}, skipping insert")
                overall_stats['users_processed'].append({
                    'user_id': user_id,
                    'fetched': 0,
                    'inserted': 0,
                    'failed': 0,
                    'status': 'skipped'
                })
                continue

            # Step 2: Transform and map data
            print(f"[TRANSFORM] Mapping columns and transforming data...")
            df_transformed = transform_data(df)
            print(f"[TRANSFORM] Transformation complete")

            # Step 3: Insert transformed data into the second database
            print(f"[INSERT] Starting insert into business_account_service database...")
            insert_stats = insert_data_into_db2(df_transformed)
            overall_stats['total_inserted'] += insert_stats['success']
            overall_stats['total_failed'] += insert_stats['failed']

            user_duration = (datetime.now() - user_start_time).total_seconds()
            overall_stats['users_processed'].append({
                'user_id': user_id,
                'fetched': rows_fetched,
                'inserted': insert_stats['success'],
                'failed': insert_stats['failed'],
                'duration': user_duration,
                'status': 'completed'
            })

            print(f"[USER {idx}/{len(users_and_dates)}] Completed in {user_duration:.2f} seconds\n")

        except Exception as e:
            print(f"[ERROR] Failed to process user {user_id}: {e}")
            overall_stats['users_processed'].append({
                'user_id': user_id,
                'status': 'error',
                'error': str(e)
            })

    job_end_time = datetime.now()
    job_duration = (job_end_time - job_start_time).total_seconds()

    # Final summary
    print(f"\n{'='*80}")
    print(f"JOB COMPLETED")
    print(f"{'='*80}")
    print(f"End time:          {job_end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Total duration:    {job_duration:.2f} seconds ({job_duration/60:.1f} minutes)")
    print(f"\nUSER STATISTICS:")
    print(f"  Users processed: {len([u for u in overall_stats['users_processed'] if u['status'] in ['completed', 'skipped']])}/{overall_stats['total_users']}")
    print(f"  Users failed:    {len([u for u in overall_stats['users_processed'] if u['status'] == 'error'])}")
    print(f"\nROW STATISTICS:")
    print(f"  Total fetched:   {overall_stats['total_fetched']}")
    print(f"  Total inserted:  {overall_stats['total_inserted']}")
    print(f"  Total failed:    {overall_stats['total_failed']}")
    if overall_stats['total_fetched'] > 0:
        print(f"  Success rate:    {(overall_stats['total_inserted']/overall_stats['total_fetched']*100):.1f}%")

    print(f"\nPER-USER DETAILS:")
    for user_stat in overall_stats['users_processed']:
        if user_stat['status'] == 'completed':
            print(f"  {user_stat['user_id']}: {user_stat['inserted']}/{user_stat['fetched']} inserted ({user_stat['duration']:.1f}s)")
        elif user_stat['status'] == 'skipped':
            print(f"  {user_stat['user_id']}: No data (skipped)")
        elif user_stat['status'] == 'error':
            print(f"  {user_stat['user_id']}: ERROR - {user_stat.get('error', 'Unknown error')}")

    print(f"{'='*80}\n")

if __name__ == "__main__":
    main()
