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
    pass


def fetch_all_records(dataset_id, limit=10):
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
    all_records = []
    offset = '0'
    limit = '5'
    
    while True:
        # make api call with offset
        url = "https://data.gov.sg/api/action/datastore_search?resource_id=" + dataset_id + "&offset=" + offset + "&limit=" + limit
        response = requests.get(url)
        # print(response.json())
        output = response.json()
        records = output['result']['records']
        
        if(len(records) == 0):
            break
        
        all_records.extend(records)
        offset += limit
        print(f"Downloaded {len(all_records)} rows so far...")
        time.sleep(0.5)
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
    except s3_client.exceptions.NoSuchKey:
        # file doesnt exist
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
    s3_key = f'{S3_METADATA_PREFIX}{dataset_name}_{timestamp}.csv'
    
    # upload csv to s3
    s3_client.put_object(Bucket=S3_BUCKET,Key=s3_key,Body=csv_string)
    
    #return s3 key
    return s3_key


def process_dataset(dataset_info):
    """
    Process a single dataset: fetch, check hash, upload if changed.
    
    Args:
        dataset_info: Dictionary with 'name' and 'id' keys
    
    Returns:
        Dictionary with processing result:
        {'dataset': name, 'status': 'processed'/'skipped'/'error', 'message': '...'}
    
    TODO:
    1. Extract dataset name and ID
    2. Log processing start
    3. Fetch all records using fetch_all_records()
    4. Convert records to DataFrame
    5. Calculate hash of the data
    6. Check if should process using should_process_dataset()
    7. If should process:
        a. Upload to S3
        b. Save new hash
        c. Return success result
    8. If should skip:
        a. Return skipped result
    9. Handle any exceptions and return error result
    """
    pass