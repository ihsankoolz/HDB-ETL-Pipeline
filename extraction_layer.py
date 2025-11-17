import json
import boto3
import pandas as pd
import requests
import hashlib
from datetime import datetime
import time

# Configuration
S3_BUCKET = 'hdb-resale-raw-data-ihsan'
S3_RAW_PREFIX = 'raw/'
S3_METADATA_PREFIX = 'metadata/'
API_BASE_URL = 'https://data.gov.sg/api/action/datastore_search'

# Dataset configuration
DATASETS = {
    'historical': [
        {'name': '1990-1999', 'id': 'd_ebc5ab87086db484f88045b47411ebc5'},
        {'name': '2000-2012', 'id': 'd_43f493c6c50d54243cc1eab0df142d6a'},
        {'name': '2012-2014', 'id': 'd_2d5ff9ea31397b66239f245f57751537'},
        {'name': '2015-2016', 'id': 'd_ea9ed51da2787afaf8e51f827c304208'}, 
    ],
    'incremental': [
        {'name': '2017-onwards', 'id': 'd_8b84c4ee58e3cfc0ece0d773c8ca6abc'}
    ]
}

s3_client = boto3.client('s3')


def lambda_handler(event, context):
    """
    Main Lambda handler function.
    
    TODO:
    1. Iterate through all datasets
    2. For historical datasets, check if already downloaded (one-time download)
    3. For incremental dataset, always check for updates
    4. Return summary of what was processed
    """
    print("Starting HDB resale data extraction...")
    
    # Check if we should process specific type
    process_mode = event.get('mode', 'all')  # 'all', 'historical', or 'incremental'
    
    results = []
    
    # Process based on mode
    if process_mode in ['all', 'historical']:
        print("\n=== Processing Historical Datasets ===")
        for dataset_info in DATASETS['historical']:
            result = process_dataset(dataset_info, dataset_type='historical')
            results.append(result)
            print(f"  {result['dataset']}: {result['status']}")
    
    if process_mode in ['all', 'incremental']:
        print("\n=== Processing Incremental Datasets ===")
        for dataset_info in DATASETS['incremental']:
            result = process_dataset(dataset_info, dataset_type='incremental')
            results.append(result)
            print(f"  {result['dataset']}: {result['status']}")
    
    # Create summary
    processed_count = sum(1 for r in results if r['status'] == 'processed')
    skipped_count = sum(1 for r in results if r['status'] == 'skipped')
    error_count = sum(1 for r in results if r['status'] == 'error')
    
    summary = {
        'statusCode': 200,
        'body': {
            'message': f'ETL pipeline completed (mode: {process_mode})',
            'summary': {
                'mode': process_mode,
                'total_datasets': len(results),
                'processed': processed_count,
                'skipped': skipped_count,
                'errors': error_count
            },
            'details': results
        }
    }
    
    print(f"\n=== Summary ===")
    print(f"Mode: {process_mode}")
    print(f"Total: {len(results)} | Processed: {processed_count} | Skipped: {skipped_count} | Errors: {error_count}")
    
    return summary


def fetch_all_records(dataset_id, limit=15000):
    """
    Fetch all records from data.gov.sg API using pagination.
    
    Args:
        dataset_id: The resource_id from data.gov.sg
        limit: Number of records per request (max usually 1000-5000)
    
    Returns:
        List of all records (list of dictionaries)
    
    TODO:
    1. Initialize empty list for all_records
    2. Set offset = 0
    3. Loop until no more records:
        a. Build API URL with dataset_id, offset, limit
        b. Make GET request
        c. Extract records from response JSON
        d. If records is empty, break loop
        e. Add records to all_records
        f. Increment offset by limit
        g. Sleep briefly (0.5s) to be nice to API
    4. Return all_records
    
    Hint: API response structure is:
    {
        "result": {
            "records": [
                {...},  # row 1
                {...},  # row 2
            ],
            "total": 50000
        }
    }
    """
    # all_records = []
    # offset = 0
    # limit = 15000
    
    # while True:
    #     # make api call with offset
    #     url = "https://data.gov.sg/api/action/datastore_search?resource_id=" + dataset_id + "&offset=" + str(offset) + "&limit=" + str(limit)
    #     response = requests.get(url)
    #     # print(response.json())
    #     output = response.json()
    #     records = output['result']['records']
        
    #     if(len(records) == 0):
    #         break
        
    #     all_records.extend(records)
    #     offset += limit
    #     print(f"Downloaded {len(all_records)} rows so far...")
    #     time.sleep(0.1)
    # return all_records
    all_records = []
    offset = 0
    initial_limit = limit  # Store the initial limit value
    
    while True:
        try:
            # Build URL
            url = f"{API_BASE_URL}?resource_id={dataset_id}&offset={offset}&limit={limit}"
            
            # Make API request with timeout
            response = requests.get(url, timeout=30)
            
            # Check if request was successful
            response.raise_for_status()
            
            # Parse JSON
            output = response.json()
            
            # Validate response structure
            if 'result' not in output or 'records' not in output['result']:
                print(f"❌ Unexpected API response structure")
                raise ValueError("Invalid API response structure")
            
            records = output['result']['records']
            
            # Check if we're done
            if len(records) == 0:
                print(f"✓ Download complete!")
                break
            
            # Add to collection
            all_records.extend(records)
            offset += limit
            print(f"Downloaded {len(all_records)} rows so far...")
            
            # Be nice to API
            time.sleep(0.3)
            
        except requests.exceptions.HTTPError as e:
            # Check if it's a "Payload Too Large" error
            if e.response.status_code == 413:
                # Reduce limit and retry
                old_limit = limit
                limit = max(1000, limit // 2)  # Cut in half, minimum 1000
                
                print(f"⚠️ Payload too large with limit={old_limit}")
                print(f"↓ Reducing to limit={limit} and retrying...")
                
                if limit < 1000:
                    print(f"❌ Limit too small, cannot proceed")
                    raise
                
                # Don't increment offset - retry same batch with smaller limit
                continue
            else:
                # Other HTTP error - fail
                print(f"❌ API request failed at offset {offset}")
                print(f"Status code: {e.response.status_code}")
                print(f"Response: {e.response.text[:500]}")
                raise
        
        except requests.exceptions.Timeout:
            print(f"⚠️ Request timeout at offset {offset}")
            print(f"Retrying in 5 seconds...")
            time.sleep(5)
            continue
            
        except requests.exceptions.RequestException as e:
            print(f"❌ API request failed at offset {offset}")
            print(f"Error: {e}")
            raise
            
        except Exception as e:
            print(f"❌ Unexpected error at offset {offset}")
            print(f"Error type: {type(e).__name__}")
            print(f"Error: {e}")
            raise
    
    return all_records

# print(fetch_all_records('d_ebc5ab87086db484f88045b47411ebc5'))

def calculate_hash(data):
    """
    Calculate MD5 hash of data.
    
    Args:
        data: String or bytes to hash
    
    Returns:
        Hash string (hexdigest)
    
    TODO:
    1. Convert data to bytes if it's a string (use .encode())
    2. Create MD5 hash object
    3. Update hash with data
    4. Return hexdigest (string representation of hash)
    
    Hint: Use hashlib.md5()
    """
    # convert data to bytes if it is a string
    if isinstance(data,str):
        data = data.encode()
    
    # create md5 has object and update with data
    hash_object = hashlib.md5(data)
    
    # return hexdigest (string representation of hash)
    return hash_object.hexdigest()



def get_last_hash_from_s3(dataset_name):
    """
    Retrieve the last hash for a dataset from S3 metadata.
    
    Args:
        dataset_name: Name of the dataset (e.g., '2017-onwards')
    
    Returns:
        Hash string if exists, None if doesn't exist
    
    TODO:
    1. Build S3 key for metadata file: f'{S3_METADATA_PREFIX}{dataset_name}_hash.txt'
    2. Try to get object from S3
    3. If exists, read and return the hash
    4. If doesn't exist (first time), return None
    
    Hint: Use s3_client.get_object() and handle exceptions
    """
    # build s3 key for metadata file
    s3_key = f'{S3_METADATA_PREFIX}{dataset_name}_hash.txt'
    
    # getting object from s3
    try:
        response = s3_client.get_object(Bucket=S3_BUCKET,Key=s3_key)
        
        # read and return the hash from response body
        hash_value = response['Body'].read().decode('utf-8')
        return hash_value
    except Exception as e: #catching all exceptions
        # file doesnt exist
        print(f"No existing hash found for {dataset_name} (first run)")
        return None

def save_hash_to_s3(dataset_name, hash_value):
    """
    Save hash to S3 metadata for future comparison.
    
    Args:
        dataset_name: Name of the dataset
        hash_value: Hash string to save
    
    TODO:
    1. Build S3 key for metadata file
    2. Upload hash_value as a text file to S3
    
    Hint: Use s3_client.put_object()
    """
    # build the s3 key for metadata file
    s3_key = f'{S3_METADATA_PREFIX}{dataset_name}_hash.txt'
    
    # upload hash_value as a text file to S3
    s3_client.put_object(Bucket=S3_BUCKET,Key=s3_key,Body=hash_value)


def should_process_dataset(dataset_name, current_hash):
    """
    Determine if dataset should be processed based on hash comparison.
    
    Args:
        dataset_name: Name of the dataset
        current_hash: Hash of current data
    
    Returns:
        True if should process, False if should skip
    
    TODO:
    1. Get last hash from S3
    2. If no last hash exists (first time), return True
    3. Compare current_hash with last_hash
    4. If different, return True
    5. If same, return False
    """
    last_hash = get_last_hash_from_s3(dataset_name)
    if not last_hash:
        return True
    else:
        if current_hash == last_hash:
            return False
        else:
            return True


def upload_to_s3(dataframe, dataset_name):
    """
    Upload DataFrame to S3 as CSV.
    
    Args:
        dataframe: pandas DataFrame
        dataset_name: Name of the dataset
    
    Returns:
        S3 key where file was uploaded
    
    TODO:
    1. Convert DataFrame to CSV string (use .to_csv())
    2. Add timestamp to filename for versioning
    3. Build S3 key: f'{S3_RAW_PREFIX}{dataset_name}_{timestamp}.csv'
    4. Upload CSV to S3
    5. Return the S3 key
    
    Hint: Use s3_client.put_object()
    """
    # convert dataframe to csv
    csv_string = dataframe.to_csv(index=False)
    
    # add timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # buld s3 key
    s3_key = f'{S3_RAW_PREFIX}{dataset_name}_{timestamp}.csv'
    
    # upload csv to s3
    s3_client.put_object(Bucket=S3_BUCKET,Key=s3_key,Body=csv_string)
    
    #return s3 key
    return s3_key


def check_if_already_downloaded(dataset_name, dataset_type='historical'):
    """
    Check if a historical dataset was already downloaded.
    For historical datasets, we just check if ANY file exists in S3.
    For incremental datasets, we use hash checking.
    
    Args:
        dataset_name: Name of dataset
        dataset_type: 'historical' or 'incremental'
    
    Returns:
        True if already downloaded, False if needs download
    """
    if dataset_type == 'incremental':
        # Incremental datasets need hash checking
        return False  # Always check hash for incremental
    
    # For historical: check if any file exists with this dataset name
    try:
        s3_key_prefix = f'{S3_RAW_PREFIX}{dataset_name}_'
        response = s3_client.list_objects_v2(
            Bucket=S3_BUCKET,
            Prefix=s3_key_prefix,
            MaxKeys=1  # Just need to know if ANY file exists
        )
        
        if 'Contents' in response and len(response['Contents']) > 0:
            print(f"  ✓ Historical dataset already exists in S3")
            return True
        else:
            print(f"  ✗ Historical dataset not found, will download")
            return False
            
    except Exception as e:
        print(f"  Error checking S3: {e}")
        return False  # If error, try to download


def process_dataset(dataset_info, dataset_type='historical'):
    """
    Process a single dataset: fetch, check hash, upload if changed.
    For historical datasets, skip if already in S3.
    
    Args:
        dataset_info: Dictionary with 'name' and 'id' keys
        dataset_type: 'historical' or 'incremental'
    
    Returns:
        Dictionary with processing result
    """
    dataset_name = None  # Initialize here so we can use in except block
    
    try:
        dataset_name = dataset_info['name']
        dataset_id = dataset_info['id']
        
        print(f'Processing dataset: {dataset_name} (type: {dataset_type})')
        
        # Smart skip for historical datasets
        if dataset_type == 'historical':
            if check_if_already_downloaded(dataset_name, dataset_type):
                return {
                    'dataset': dataset_name,
                    'status': 'skipped',
                    'message': 'Historical dataset already exists in S3'
                }
        
        # Download records
        print(f"Fetching records from API...")
        records = fetch_all_records(dataset_id)
        print(f"✓ Downloaded {len(records)} records")
        
        # Convert to DataFrame
        df = pd.DataFrame(records)
        
        # Calculate hash
        csv_data = df.to_csv(index=False)
        current_hash = calculate_hash(csv_data)
        print(f"✓ Hash calculated: {current_hash[:16]}...")
        
        # For incremental datasets, check if data changed
        if dataset_type == 'incremental':
            if not should_process_dataset(dataset_name, current_hash):
                return {
                    'dataset': dataset_name,
                    'status': 'skipped',
                    'message': 'No changes detected (hash match)'
                }
        
        # Upload to S3
        print(f"Uploading to S3...")
        s3_key = upload_to_s3(df, dataset_name)
        save_hash_to_s3(dataset_name, current_hash)
        
        return {
            'dataset': dataset_name,
            'status': 'processed',
            'message': f'Successfully uploaded {len(records)} rows to {s3_key}',
            'rows': len(records)
        }
        
    except Exception as e:
        # Better error logging
        import traceback
        error_details = traceback.format_exc()
        print(f"❌ ERROR processing {dataset_name}:")
        print(f"Error type: {type(e).__name__}")
        print(f"Error message: {str(e)}")
        print(f"Full traceback:\n{error_details}")
        
        return {
            'dataset': dataset_name if dataset_name else 'unknown',
            'status': 'error',
            'message': f'{type(e).__name__}: {str(e)}'
        }