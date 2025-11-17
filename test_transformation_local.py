"""
Local testing script for transformation layer
Tests transformation functions WITHOUT needing AWS Lambda
"""

import pandas as pd
import boto3
from io import StringIO
from datetime import datetime
from transformation_layer import read_csv_from_s3, standardize_schema,convert_data_types,remove_duplicates,handle_nulls,derive_location_intelligence,derive_price_metrics,derive_property_characteristics, derive_time_fields, validate_hdb_data,standardize_text

# Import your transformation functions
# (Copy your transformation_layer.py functions here, or import if structured as module)

# ============================================
# TEST CONFIGURATION
# ============================================

# We'll download one actual CSV from your S3 bucket
TEST_BUCKET = 'hdb-resale-raw-data-ihsan'
TEST_KEY = 'raw/1990-1999_20251114_112515.csv'  # Use your actual filename!

# Or if you prefer, use a local CSV file:
# TEST_FILE_PATH = 'test_data_2015-2016.csv'

s3_client = boto3.client('s3')

# ============================================
# TEST FUNCTIONS
# ============================================

def test_read_csv():
    """Test 1: Can we read CSV from S3?"""
    print("\n" + "="*60)
    print("TEST 1: Reading CSV from S3")
    print("="*60)
    
    try:
        print(f"Bucket: {TEST_BUCKET}")
        print(f"Key: {TEST_KEY}")
        
        # Use your read_csv_from_s3 function
        df = read_csv_from_s3(TEST_BUCKET, TEST_KEY)
        
        print(f"\n✅ SUCCESS!")
        print(f"Rows: {len(df)}")
        print(f"Columns: {len(df.columns)}")
        print(f"\nColumn names:")
        print(df.columns.tolist())
        print(f"\nFirst 3 rows:")
        print(df.head(3))
        print(f"\nData types:")
        print(df.dtypes)
        
        return df
    except Exception as e:
        print(f"\n❌ FAILED!")
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return None


def test_schema_standardization(df):
    """Test 2: Schema standardization"""
    print("\n" + "="*60)
    print("TEST 2: Schema Standardization")
    print("="*60)
    
    if df is None:
        print("❌ No DataFrame (previous test failed)")
        return None
    
    try:
        print(f"Columns before: {df.columns.tolist()}")
        print(f"Has 'remaining_lease'? {'remaining_lease' in df.columns}")
        
        df_std = standardize_schema(df.copy())
        
        print(f"\n✅ SUCCESS!")
        print(f"Columns after: {df_std.columns.tolist()}")
        print(f"Has 'remaining_lease' now? {'remaining_lease' in df_std.columns}")
        
        return df_std
    except Exception as e:
        print(f"\n❌ FAILED!")
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return None


def test_type_conversion(df):
    """Test 3: Data type conversion"""
    print("\n" + "="*60)
    print("TEST 3: Data Type Conversion")
    print("="*60)
    
    if df is None:
        print("❌ No DataFrame (previous test failed)")
        return None
    
    try:
        print("Data types before:")
        print(df.dtypes)
        
        df_converted = convert_data_types(df.copy())
        
        print(f"\n✅ SUCCESS!")
        print("Data types after:")
        print(df_converted.dtypes)
        
        # Check specific conversions
        print(f"\nVerification:")
        print(f"month is datetime? {pd.api.types.is_datetime64_any_dtype(df_converted['month'])}")
        print(f"resale_price is numeric? {pd.api.types.is_numeric_dtype(df_converted['resale_price'])}")
        print(f"floor_area_sqm is numeric? {pd.api.types.is_numeric_dtype(df_converted['floor_area_sqm'])}")
        
        return df_converted
    except Exception as e:
        print(f"\n❌ FAILED!")
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return None


def test_duplicate_removal(df):
    """Test 4: Duplicate removal"""
    print("\n" + "="*60)
    print("TEST 4: Duplicate Removal")
    print("="*60)
    
    if df is None:
        print("❌ No DataFrame (previous test failed)")
        return None
    
    try:
        original_count = len(df)
        print(f"Original rows: {original_count}")
        
        df_clean, duplicates_removed = remove_duplicates(df.copy())
        
        print(f"\n✅ SUCCESS!")
        print(f"Duplicates removed: {duplicates_removed}")
        print(f"Remaining rows: {len(df_clean)}")
        
        return df_clean, duplicates_removed
    except Exception as e:
        print(f"\n❌ FAILED!")
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return None, 0


def test_null_handling(df):
    """Test 5: Null handling"""
    print("\n" + "="*60)
    print("TEST 5: Null Handling")
    print("="*60)
    
    if df is None:
        print("❌ No DataFrame (previous test failed)")
        return None
    
    try:
        print("Null counts before:")
        print(df.isnull().sum())
        
        df_clean, nulls_removed, null_counts = handle_nulls(df.copy())
        
        print(f"\n✅ SUCCESS!")
        print(f"Rows removed (critical nulls): {nulls_removed}")
        print(f"Remaining rows: {len(df_clean)}")
        print(f"\nNull counts after:")
        print(df_clean.isnull().sum())
        
        return df_clean, nulls_removed, null_counts
    except Exception as e:
        print(f"\n❌ FAILED!")
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return None, 0, {}


def test_derived_fields(df):
    """Test 6: All derived fields"""
    print("\n" + "="*60)
    print("TEST 6: Derived Fields")
    print("="*60)
    
    if df is None:
        print("❌ No DataFrame (previous test failed)")
        return None
    
    try:
        print(f"Columns before: {len(df.columns)}")
        
        # Apply all derivation functions
        df = derive_time_fields(df.copy())
        print("✓ Time fields added")
        
        df = derive_price_metrics(df.copy())
        print("✓ Price metrics added")
        
        df = derive_property_characteristics(df.copy())
        print("✓ Property characteristics added")
        
        df = derive_location_intelligence(df.copy())
        print("✓ Location intelligence added")
        
        print(f"\n✅ SUCCESS!")
        print(f"Columns after: {len(df.columns)}")
        print(f"\nNew columns added:")
        
        # Show sample of new columns
        new_cols = ['year', 'month_num', 'quarter', 'price_per_sqm', 
                    'price_per_sqft', 'storey_lower', 'storey_upper',
                    'lease_age', 'flat_age_at_sale', 'region', 'estate_maturity']
        
        for col in new_cols:
            if col in df.columns:
                print(f"  - {col}: {df[col].iloc[0]} (sample value)")
        
        return df
    except Exception as e:
        print(f"\n❌ FAILED!")
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return None


def test_validation(df):
    """Test 7: Data validation"""
    print("\n" + "="*60)
    print("TEST 7: Data Validation")
    print("="*60)
    
    if df is None:
        print("❌ No DataFrame (previous test failed)")
        return None
    
    try:
        rows_before = len(df)
        print(f"Rows before validation: {rows_before}")
        
        df_valid, invalid_removed = validate_hdb_data(df.copy())
        
        print(f"\n✅ SUCCESS!")
        print(f"Invalid rows removed: {invalid_removed}")
        print(f"Remaining rows: {len(df_valid)}")
        print(f"Data quality: {(len(df_valid)/rows_before)*100:.2f}%")
        
        return df_valid, invalid_removed
    except Exception as e:
        print(f"\n❌ FAILED!")
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return None, 0


def test_text_standardization(df):
    """Test 8: Text standardization"""
    print("\n" + "="*60)
    print("TEST 8: Text Standardization")
    print("="*60)
    
    if df is None:
        print("❌ No DataFrame (previous test failed)")
        return None
    
    try:
        print("Sample text before:")
        print(f"Town: {df['town'].iloc[0]}")
        print(f"Flat type: {df['flat_type'].iloc[0]}")
        
        df_std = standardize_text(df.copy())
        
        print(f"\n✅ SUCCESS!")
        print("Sample text after:")
        print(f"Town: {df_std['town'].iloc[0]}")
        print(f"Flat type: {df_std['flat_type'].iloc[0]}")
        
        return df_std
    except Exception as e:
        print(f"\n❌ FAILED!")
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return None


def test_save_locally(df):
    """Test 9: Save processed data locally"""
    print("\n" + "="*60)
    print("TEST 9: Save Processed CSV Locally")
    print("="*60)
    
    if df is None:
        print("❌ No DataFrame (previous test failed)")
        return
    
    try:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"test_processed_{timestamp}.csv"
        
        df.to_csv(filename, index=False)
        
        print(f"\n✅ SUCCESS!")
        print(f"File saved: {filename}")
        print(f"Rows: {len(df)}")
        print(f"Columns: {len(df.columns)}")
        print(f"\nYou can open this file to verify the transformations!")
        
    except Exception as e:
        print(f"\n❌ FAILED!")
        print(f"Error: {e}")


# ============================================
# MAIN TEST RUNNER
# ============================================

def run_all_tests():
    """Run complete transformation pipeline test"""
    print("\n" + "="*60)
    print("STARTING TRANSFORMATION LAYER LOCAL TESTS")
    print("="*60)
    print(f"Testing with: {TEST_KEY}")
    
    # Test 1: Read CSV
    df = test_read_csv()
    if df is None:
        print("\n⚠️ Stopping - cannot read CSV")
        return
    
    # Store original for comparison
    df_original = df.copy()
    original_count = len(df)
    
    # Test 2: Schema standardization
    df = test_schema_standardization(df)
    if df is None:
        print("\n⚠️ Stopping - schema standardization failed")
        return
    
    # Test 3: Type conversion
    df = test_type_conversion(df)
    if df is None:
        print("\n⚠️ Stopping - type conversion failed")
        return
    
    # Test 4: Remove duplicates
    df, duplicates_removed = test_duplicate_removal(df)
    if df is None:
        print("\n⚠️ Stopping - duplicate removal failed")
        return
    
    # Test 5: Handle nulls
    df, nulls_removed, null_counts = test_null_handling(df)
    if df is None:
        print("\n⚠️ Stopping - null handling failed")
        return
    
    # Test 6: Derive fields
    df = test_derived_fields(df)
    if df is None:
        print("\n⚠️ Stopping - derived fields failed")
        return
    
    # Test 7: Validate
    df, invalid_removed = test_validation(df)
    if df is None:
        print("\n⚠️ Stopping - validation failed")
        return
    
    # Test 8: Standardize text
    df = test_text_standardization(df)
    if df is None:
        print("\n⚠️ Stopping - text standardization failed")
        return
    
    # Test 9: Save locally
    test_save_locally(df)
    
    # Final Summary
    print("\n" + "="*60)
    print("TRANSFORMATION PIPELINE SUMMARY")
    print("="*60)
    print(f"Original rows:          {original_count}")
    print(f"Duplicates removed:     {duplicates_removed}")
    print(f"Null rows removed:      {nulls_removed}")
    print(f"Invalid rows removed:   {invalid_removed}")
    print(f"Final rows:             {len(df)}")
    print(f"Data quality score:     {(len(df)/original_count)*100:.2f}%")
    print(f"\nOriginal columns:       {len(df_original.columns)}")
    print(f"Final columns:          {len(df.columns)}")
    print(f"New columns added:      {len(df.columns) - len(df_original.columns)}")
    print(f"\n✅ ALL TESTS PASSED!")
    print(f"\nNext step: Deploy to Lambda and set up S3 trigger")


if __name__ == "__main__":
    run_all_tests()