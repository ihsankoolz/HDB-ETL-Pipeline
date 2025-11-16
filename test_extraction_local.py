"""
Local testing script for extraction layer
This tests functions WITHOUT needing AWS Lambda
"""

import pandas as pd
import requests
import hashlib
from datetime import datetime
import time

# Use same config as main script
API_BASE_URL = 'https://data.gov.sg/api/action/datastore_search'

# Test with just ONE small dataset first
TEST_DATASET = {
    'name': '1990-1999',
    'id': 'd_ebc5ab87086db484f88045b47411ebc5'
}

# ============================================
# Copy your corrected functions here
# ============================================

def fetch_all_records(dataset_id, limit=1000):
    """Your corrected version"""
    all_records = []
    offset = 0        # Integer, not string
    limit = 1000      # Integer, not string
    
    while True:
        url = f"{API_BASE_URL}?resource_id={dataset_id}&offset={offset}&limit={limit}"
        print(f"Fetching: offset={offset}, limit={limit}")
        
        response = requests.get(url)
        output = response.json()
        records = output['result']['records']
        
        if len(records) == 0:
            print("No more records - finished!")
            break
        
        all_records.extend(records)
        offset += limit
        print(f"Downloaded {len(all_records)} rows so far...")
        time.sleep(0.5)
    
    return all_records


def calculate_hash(data):
    """Your version - already correct"""
    if isinstance(data, str):
        data = data.encode()
    
    hash_object = hashlib.md5(data)
    return hash_object.hexdigest()


# ============================================
# Test Functions
# ============================================

def test_fetch_records():
    """Test 1: Can we fetch data from API?"""
    print("\n" + "="*60)
    print("TEST 1: Fetching records from API")
    print("="*60)
    
    try:
        print(f"Testing dataset: {TEST_DATASET['name']}")
        print(f"Dataset ID: {TEST_DATASET['id']}")
        
        records = fetch_all_records(TEST_DATASET['id'])
        
        print(f"\n✅ SUCCESS!")
        print(f"Total records fetched: {len(records)}")
        print(f"\nFirst record sample:")
        print(records[0])  # Show first record
        
        return records
    except Exception as e:
        print(f"\n❌ FAILED!")
        print(f"Error: {e}")
        return None


def test_convert_to_dataframe(records):
    """Test 2: Can we convert to DataFrame?"""
    print("\n" + "="*60)
    print("TEST 2: Converting to DataFrame")
    print("="*60)
    
    if not records:
        print("❌ No records to convert (previous test failed)")
        return None
    
    try:
        df = pd.DataFrame(records)
        
        print(f"\n✅ SUCCESS!")
        print(f"DataFrame shape: {df.shape} (rows, columns)")
        print(f"\nColumn names:")
        print(df.columns.tolist())
        print(f"\nFirst 3 rows:")
        print(df.head(3))
        
        return df
    except Exception as e:
        print(f"\n❌ FAILED!")
        print(f"Error: {e}")
        return None


def test_calculate_hash(df):
    """Test 3: Can we calculate hash?"""
    print("\n" + "="*60)
    print("TEST 3: Calculating hash")
    print("="*60)
    
    if df is None:
        print("❌ No DataFrame to hash (previous test failed)")
        return None
    
    try:
        csv_data = df.to_csv(index=False)
        hash_value = calculate_hash(csv_data)
        
        print(f"\n✅ SUCCESS!")
        print(f"Hash calculated: {hash_value}")
        print(f"Hash length: {len(hash_value)} characters")
        
        # Test hash consistency
        csv_data2 = df.to_csv(index=False)
        hash_value2 = calculate_hash(csv_data2)
        
        if hash_value == hash_value2:
            print(f"✅ Hash is consistent (same data = same hash)")
        else:
            print(f"⚠️ WARNING: Hash inconsistent!")
        
        return hash_value
    except Exception as e:
        print(f"\n❌ FAILED!")
        print(f"Error: {e}")
        return None


def test_save_csv_locally(df):
    """Test 4: Can we save CSV locally?"""
    print("\n" + "="*60)
    print("TEST 4: Saving CSV to local disk")
    print("="*60)
    
    if df is None:
        print("❌ No DataFrame to save (previous test failed)")
        return
    
    try:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"test_output_{TEST_DATASET['name']}_{timestamp}.csv"
        
        df.to_csv(filename, index=False)
        
        print(f"\n✅ SUCCESS!")
        print(f"File saved: {filename}")
        print(f"You can open this file to verify the data looks correct")
        
    except Exception as e:
        print(f"\n❌ FAILED!")
        print(f"Error: {e}")


# ============================================
# Main Test Runner
# ============================================

def run_all_tests():
    """Run all tests in sequence"""
    print("\n" + "="*60)
    print("STARTING LOCAL TESTS FOR EXTRACTION LAYER")
    print("="*60)
    print(f"Testing with dataset: {TEST_DATASET['name']}")
    print(f"This may take 1-2 minutes...")
    
    # Test 1: Fetch records
    records = test_fetch_records()
    if not records:
        print("\n⚠️ Stopping tests - fetch failed")
        return
    
    # Test 2: Convert to DataFrame
    df = test_convert_to_dataframe(records)
    if df is None:
        print("\n⚠️ Stopping tests - DataFrame conversion failed")
        return
    
    # Test 3: Calculate hash
    hash_value = test_calculate_hash(df)
    
    # Test 4: Save locally
    test_save_csv_locally(df)
    
    # Summary
    print("\n" + "="*60)
    print("TEST SUMMARY")
    print("="*60)
    print(f"✅ All basic tests passed!")
    print(f"✅ Dataset: {TEST_DATASET['name']}")
    print(f"✅ Total rows: {len(records)}")
    print(f"✅ Hash: {hash_value}")
    print(f"\nNext step: Deploy to Lambda and test with AWS S3")


if __name__ == "__main__":
    run_all_tests()