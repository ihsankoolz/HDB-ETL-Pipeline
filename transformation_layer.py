"""
HDB Resale Price Data Transformation Lambda
============================================

Business Purpose:
- Transform raw HDB resale data into analysis-ready format
- Enable price trend analysis, property comparison, and investment insights
- Support stakeholders: home buyers, investors, researchers, policy makers

Transformation Tiers:
- Tier 1 (Essential): Data quality, time dimensions, basic price metrics
- Tier 2 (Very Useful): Property characteristics, lease analysis
- Tier 3 (Advanced): Regional insights, estate maturity, advanced metrics
"""

import json
import boto3
import pandas as pd
from datetime import datetime
from io import StringIO, BytesIO

# ============================================
# CONFIGURATION
# ============================================

S3_BUCKET_RAW = 'hdb-resale-raw-data-ihsan'
S3_BUCKET_PROCESSED = 'hdb-resale-processed-data-ihsan'
S3_RAW_PREFIX = 'raw/'
S3_PROCESSED_PREFIX = 'processed/'
S3_QUALITY_PREFIX = 'quality_reports/'

s3_client = boto3.client('s3')

# Region mapping for Singapore towns
REGION_MAPPING = {
    # Central
    'BISHAN': 'Central',
    'BUKIT MERAH': 'Central',
    'BUKIT TIMAH': 'Central',
    'CENTRAL AREA': 'Central',
    'GEYLANG': 'Central',
    'WHAMPOA': 'Central',
    'QUEENSTOWN': 'Central',
    'TOA PAYOH': 'Central',
    
    # North
    'ANG MO KIO': 'North',
    'SEMBAWANG': 'North',
    'WOODLANDS': 'North',
    'YISHUN': 'North',
    
    # North-East
    'HOUGANG': 'North-East',
    'SENGKANG': 'North-East',
    'PUNGGOL': 'North-East',
    'SERANGOON': 'North-East',
    
    # East
    'BEDOK': 'East',
    'PASIR RIS': 'East',
    'TAMPINES': 'East',
    'KALLANG': 'East',
    'MARINE PARADE': 'East',

    # West
    'BUKIT BATOK': 'West',
    'BUKIT PANJANG': 'West',
    'CHOA CHU KANG': 'West',
    'CLEMENTI': 'West',
    'JURONG EAST': 'West',
    'JURONG WEST': 'West',
}

# Mature estates (established before 1997)
MATURE_ESTATES = [
    'ANG MO KIO', 'BEDOK', 'BISHAN', 'BUKIT MERAH', 'BUKIT TIMAH',
    'CENTRAL AREA', 'CLEMENTI', 'GEYLANG', 'KALLANG/WHAMPOA',
    'MARINE PARADE', 'QUEENSTOWN', 'SERANGOON', 'TAMPINES', 
    'TOA PAYOH'
]


# ============================================
# LAMBDA HANDLER
# ============================================

def lambda_handler(event, context):
    """
    Main Lambda handler - triggered by S3 event when new file uploaded to raw/
    
    Event structure:
    {
        "Records": [{
            "s3": {
                "bucket": {"name": "hdb-resale-raw-data-ihsan"},
                "object": {"key": "raw/2017-onwards_20241114.csv"}
            }
        }]
    }
    
    TODO:
    1. Parse S3 event to extract bucket and key
    2. Extract dataset name from key (e.g., "2017-onwards")
    3. Call transform_dataset() to process the file
    4. Return success/error response
    5. Handle exceptions gracefully
    
    Returns:
        dict: Response with statusCode and processing summary
    """
    print("="*60)
    print("HDB TRANSFORMATION LAYER - Lambda Handler")
    print("="*60)

    try:
        # 1. Parse S3 event to extract bucket and key
        # Event comes from S3 trigger when file is uploaded to raw/
        record = event['Records'][0]
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']

        print(f"\nS3 Event Received:")
        print(f"  Bucket: {bucket}")
        print(f"  Key: {key}")

        # 2. Extract dataset name for logging
        dataset_name = extract_dataset_name(key)
        print(f"  Dataset: {dataset_name}")

        # 3. Call transform_dataset() to process the file
        print(f"\nStarting transformation pipeline...")
        summary = transform_dataset(bucket, key)

        # 4. Return success response
        response = {
            'statusCode': 200,
            'body': {
                'message': 'Transformation completed successfully',
                'dataset': dataset_name,
                'summary': summary
            }
        }

        print(f"\n{'='*60}")
        print("SUCCESS: Transformation completed")
        print(f"{'='*60}")

        return response

    except KeyError as e:
        # Handle malformed S3 event
        error_msg = f"Invalid S3 event structure: {str(e)}"
        print(f"\n❌ ERROR: {error_msg}")
        return {
            'statusCode': 400,
            'body': {
                'message': 'Invalid event structure',
                'error': error_msg
            }
        }

    except Exception as e:
        # Handle any other errors
        import traceback
        error_msg = str(e)
        error_trace = traceback.format_exc()

        print(f"\n❌ ERROR: Transformation failed")
        print(f"Error type: {type(e).__name__}")
        print(f"Error message: {error_msg}")
        print(f"Traceback:\n{error_trace}")

        return {
            'statusCode': 500,
            'body': {
                'message': 'Transformation failed',
                'error': error_msg,
                'error_type': type(e).__name__
            }
        }


# ============================================
# MAIN TRANSFORMATION ORCHESTRATOR
# ============================================

def transform_dataset(bucket, key):
    """
    Main transformation pipeline orchestrator
    
    Args:
        bucket: S3 bucket name
        key: S3 object key (e.g., "raw/2017-onwards_20241114.csv")
    
    Returns:
        dict: Processing summary with metrics
    
    TODO:
    1. Read CSV from S3 into DataFrame
    2. Track original row count
    3. Apply transformations in order:
       - standardize_schema()
       - convert_data_types()
       - remove_duplicates()
       - handle_nulls()
       - derive_time_fields()
       - derive_price_metrics()
       - derive_property_characteristics()
       - derive_location_intelligence()
       - standardize_text()
       - validate_hdb_data()
    4. Generate quality metrics
    5. Save processed data to S3
    6. Save quality report to S3
    7. Return summary
    """
    print(f"Starting transformation for {key}")

    # 1. Read CSV from S3
    print("Reading CSV from S3...")
    df_original = read_csv_from_s3(bucket, key)
    original_count = len(df_original)
    print(f"✓ Loaded {original_count} rows")

    # Keep a copy for quality metrics
    df = df_original.copy()

    # 2. Apply transformations in order
    print("\n=== Data Quality Transformations ===")

    # Standardize schema
    print("Standardizing schema...")
    df = standardize_schema(df)

    # Convert data types
    print("Converting data types...")
    df = convert_data_types(df)

    # Remove duplicates
    print("Removing duplicates...")
    df, duplicates_removed = remove_duplicates(df)
    print(f"  Removed {duplicates_removed} duplicates")

    # Handle nulls
    print("Handling null values...")
    df, nulls_removed, null_counts = handle_nulls(df)
    print(f"  Removed {nulls_removed} rows with null critical fields")

    print("\n=== Derived Fields ===")

    # Derive time fields (needs to be before property characteristics)
    print("Deriving time fields...")
    df = derive_time_fields(df)

    # Derive price metrics
    print("Deriving price metrics...")
    df = derive_price_metrics(df)

    # Derive property characteristics
    print("Deriving property characteristics...")
    df = derive_property_characteristics(df)

    # Derive location intelligence
    print("Deriving location intelligence...")
    df = derive_location_intelligence(df)

    # Standardize text
    print("Standardizing text fields...")
    df = standardize_text(df)

    print("\n=== Validation ===")

    # Validate data
    print("Validating HDB data...")
    df, invalid_removed = validate_hdb_data(df)
    print(f"  Removed {invalid_removed} invalid rows")

    # 3. Generate quality metrics
    print("\n=== Generating Quality Metrics ===")
    quality_metrics = generate_quality_metrics(
        df_original, df, duplicates_removed, nulls_removed, invalid_removed, null_counts
    )
    print(f"  Data quality score: {quality_metrics['data_quality_score']}%")
    print(f"  Final rows: {quality_metrics['final_rows']}")

    # 4. Extract dataset name and save to S3
    print("\n=== Saving to S3 ===")
    dataset_name = extract_dataset_name(key)
    s3_keys = save_to_s3(df, dataset_name, quality_metrics)
    print(f"✓ Saved processed data to: {s3_keys['csv_key']}")
    print(f"✓ Saved quality report to: {s3_keys['json_key']}")

    # 5. Return summary
    summary = {
        'dataset_name': dataset_name,
        'original_rows': original_count,
        'final_rows': len(df),
        'transformations_applied': [
            'standardize_schema', 'convert_data_types', 'remove_duplicates',
            'handle_nulls', 'derive_time_fields', 'derive_price_metrics',
            'derive_property_characteristics', 'derive_location_intelligence',
            'standardize_text', 'validate_hdb_data'
        ],
        'quality_score': quality_metrics['data_quality_score'],
        's3_csv_key': s3_keys['csv_key'],
        's3_quality_key': s3_keys['json_key']
    }

    print("\n=== Transformation Complete ===")
    return summary


# ============================================
# TIER 1: DATA QUALITY - SCHEMA STANDARDIZATION
# ============================================

def standardize_schema(df):
    """
    Ensure all datasets have consistent schema
    
    Business Purpose:
    - Handle different column schemas across time periods
    - Ensure downstream analysis works on all datasets
    - Add missing columns with appropriate defaults
    
    Args:
        df: Raw DataFrame
    
    Returns:
        DataFrame with standardized schema
    
    TODO:
    1. Check if 'remaining_lease' column exists
    2. If not, add it (we'll calculate later in derive functions)
    3. Ensure all expected columns are present
    4. Reorder columns to standard order (optional)
    
    Expected columns:
    - id, month, town, flat_type, block, street_name
    - storey_range, floor_area_sqm, flat_model
    - lease_commence_date, resale_price
    - remaining_lease (add if missing)
    """
    # Check if 'remaining_lease' column exists
    if 'remaining_lease' not in df.columns:
        # Add it with None values (we'll calculate it later)
        df['remaining_lease'] = None
        print("Added missing 'remaining_lease' column")

    return df



# ============================================
# TIER 1: DATA QUALITY - TYPE CONVERSION
# ============================================

def convert_data_types(df):
    """
    Convert columns to appropriate data types
    
    Business Purpose:
    - Enable mathematical operations (sum, average, etc.)
    - Enable date-based filtering and sorting
    - Prevent type-related errors in analysis
    
    Args:
        df: DataFrame with raw types
    
    Returns:
        DataFrame with corrected types
    
    TODO:
    Convert these columns:
    - month → datetime
    - resale_price → int
    - floor_area_sqm → float
    - lease_commence_date → int
    - Handle conversion errors gracefully (invalid values → null)
    
    Hint: Use pd.to_datetime(), pd.to_numeric() with errors='coerce'
    """
    # Convert month to datetime
    df['month'] = pd.to_datetime(df['month'], errors='coerce')

    # Convert resale_price to float first, round, then to Int64 (nullable integer)
    df['resale_price'] = pd.to_numeric(df['resale_price'], errors='coerce')
    df['resale_price'] = df['resale_price'].round().astype('Int64')

    # Convert floor_area_sqm to float
    df['floor_area_sqm'] = pd.to_numeric(df['floor_area_sqm'], errors='coerce')

    # Convert lease_commence_date to float first, round, then to Int64 (nullable integer)
    df['lease_commence_date'] = pd.to_numeric(df['lease_commence_date'], errors='coerce')
    df['lease_commence_date'] = df['lease_commence_date'].round().astype('Int64')

    return df



# ============================================
# TIER 1: DATA QUALITY - DUPLICATE REMOVAL
# ============================================

def remove_duplicates(df):
    """
    Remove duplicate rows
    
    Business Purpose:
    - Prevent double-counting in aggregations
    - Ensure accurate statistics
    - Remove API/data collection errors
    
    Args:
        df: DataFrame potentially with duplicates
    
    Returns:
        tuple: (cleaned DataFrame, number of duplicates removed)
    
    TODO:
    1. Count duplicates before removal
    2. Remove duplicates (keep first occurrence)
    3. Return cleaned DataFrame and count
    
    Hint: Use df.duplicated() and df.drop_duplicates()
    """
    # 1. Count duplicates before removal
    duplicates_count = df.duplicated().sum()

    # 2. Remove duplicates (keep first occurrence)
    df = df.drop_duplicates()

    # 3. Return cleaned DataFrame and count
    return (df, duplicates_count)


# ============================================
# TIER 1: DATA QUALITY - NULL HANDLING
# ============================================

def handle_nulls(df):
    """
    Handle missing values based on field criticality
    
    Business Purpose:
    - Remove rows with missing critical data (can't analyze without price)
    - Preserve rows with missing optional data
    - Track data completeness
    
    Args:
        df: DataFrame with potential nulls
    
    Returns:
        tuple: (cleaned DataFrame, number of rows removed, null counts dict)
    
    TODO:
    Critical fields (must have value - drop row if null):
    - resale_price
    - town
    - flat_type
    - floor_area_sqm
    - month
    
    Optional fields (can be null):
    - remaining_lease (old datasets don't have this)
    - flat_model
    
    1. Count nulls in each column
    2. Drop rows where critical fields are null
    3. Track how many rows removed
    4. Return cleaned DataFrame, count, and null summary
    """
    # 1. Count nulls in each column (before removal)
    null_counts = df.isnull().sum().to_dict()

    # Track original row count
    original_count = len(df)

    # 2. Define critical fields (must not be null)
    critical_fields = ['resale_price', 'town', 'flat_type', 'floor_area_sqm', 'month']

    # 3. Drop rows where ANY critical field is null
    # subset parameter: only check these columns for nulls
    df = df.dropna(subset=critical_fields)

    # 4. Calculate how many rows were removed
    rows_removed = original_count - len(df)

    # 5. Return tuple: (cleaned DataFrame, rows removed count, null counts dict)
    return (df, rows_removed, null_counts)
    


# ============================================
# TIER 2: DATA QUALITY - VALIDATION
# ============================================

def validate_hdb_data(df):
    """
    Validate data against HDB-specific business rules
    
    Business Purpose:
    - Remove outliers and erroneous data
    - Ensure data quality for analysis
    - Flag suspicious transactions
    
    Args:
        df: DataFrame to validate
    
    Returns:
        tuple: (validated DataFrame, number of invalid rows removed)
    
    TODO:
    Validation rules based on Singapore HDB constraints:
    
    1. Resale price range:
       - Minimum: $50,000 (too cheap = likely error)
       - Maximum: $2.500,000 (HDB ceiling)
    
    2. Floor area range:
       - Minimum: 20 sqm (smallest HDB units)
       - Maximum: 400 sqm (largest HDB units)
    
    3. Price per sqm sanity check:
       - Minimum: $1,000/sqm (unrealistically cheap)
       - Maximum: $20,000/sqm (unrealistically expensive for HDB)
    
    4. Lease commence date:
       - Minimum: 1960 (before HDB existed)
       - Maximum: current year (future dates invalid)
    
    5. Storey range format:
       - Must contain "TO" (e.g., "01 TO 03")
    
    Remove rows that violate any rule and return count
    """
    # Track original row count
    original_count = len(df)

    # Get current year for validation
    current_year = datetime.now().year

    # Calculate price per sqm (needed for validation rule 3)
    df['price_per_sqm_temp'] = df['resale_price'] / df['floor_area_sqm']

    # Create validation masks (True = valid, False = invalid) Each mask checks ONE rule

    # 1. Resale price range: $50,000 - $2,500,000
    valid_price = (df['resale_price'] >= 50000) & (df['resale_price'] <= 2500000)

    # 2. Floor area range: 20 - 400 sqm
    valid_area = (df['floor_area_sqm'] >= 20) & (df['floor_area_sqm'] <= 400)

    # 3. Price per sqm: $1,000 - $20,000
    valid_price_per_sqm = (df['price_per_sqm_temp'] >= 1000) & (df['price_per_sqm_temp'] <= 20000)

    # 4. Lease commence date: 1960 - current year
    valid_lease_date = (df['lease_commence_date'] >= 1960) & (df['lease_commence_date'] <= current_year)

    # 5. Storey range must contain "TO"
    valid_storey = df['storey_range'].str.contains('TO', case=False, na=False)

    # Combine all validation masks (row must pass ALL rules)
    all_valid = valid_price & valid_area & valid_price_per_sqm & valid_lease_date & valid_storey

    # Keep only valid rows
    df = df[all_valid]

    # Remove temporary column
    df = df.drop(columns=['price_per_sqm_temp'])

    # Calculate how many rows were removed
    rows_removed = original_count - len(df)

    return (df, rows_removed)



# ============================================
# TIER 1: DERIVED FIELDS - TIME DIMENSIONS
# ============================================

def derive_time_fields(df):
    """
    Extract time-based fields for temporal analysis
    
    Business Purpose:
    - Enable year-over-year price trend analysis
    - Identify seasonal patterns in HDB sales
    - Support time-series forecasting
    - Allow quarterly/monthly aggregations
    
    Args:
        df: DataFrame with 'month' as datetime
    
    Returns:
        DataFrame with added time fields
    
    TODO:
    Extract from 'month' column:
    1. year (integer) - for yearly trends
    2. month_num (1-12) - for seasonality analysis
    3. quarter (1-4) - for quarterly reports
    
    Hint: Use .dt accessor (e.g., df['month'].dt.year)
    """
    # 1. Extract year (integer)
    df['year'] = df['month'].dt.year

    # 2. Extract month number (1-12)
    df['month_num'] = df['month'].dt.month

    # 3. Extract quarter (1-4)
    df['quarter'] = df['month'].dt.quarter

    return df



# ============================================
# TIER 1 & 3: DERIVED FIELDS - PRICE METRICS
# ============================================

def derive_price_metrics(df):
    """
    Calculate price comparison metrics
    
    Business Purpose:
    - Fair comparison across different flat sizes
    - Identify overpriced/underpriced units
    - Support property valuation models
    - Provide industry-standard metrics (PSF)
    
    Args:
        df: DataFrame with price and area columns
    
    Returns:
        DataFrame with price metrics
    
    TODO:
    1. price_per_sqm = resale_price / floor_area_sqm
    2. floor_area_sqft = floor_area_sqm * 10.764 (conversion factor)
    3. price_per_sqft = resale_price / floor_area_sqft
    
    Round to 2 decimal places for readability
    """
    # 1. Calculate price per sqm (rounded to 2 decimals)
    df['price_per_sqm'] = (df['resale_price'] / df['floor_area_sqm']).round(2)

    # 2. Calculate floor area in square feet (rounded to 2 decimals)
    df['floor_area_sqft'] = (df['floor_area_sqm'] * 10.764).round(2)

    # 3. Calculate price per sqft (rounded to 2 decimals)
    df['price_per_sqft'] = (df['resale_price'] / df['floor_area_sqft']).round(2)

    return df


# ============================================
# TIER 2 & 3: DERIVED FIELDS - PROPERTY CHARACTERISTICS
# ============================================

# 1. Parse storey_range ("01 TO 03" → storey_lower=1, storey_upper=3)
    # Split by "TO" and extract lower and upper bounds
def parse_storey_range(storey_str):
    """Parse storey range like '01 TO 03' into lower and upper values"""
    try:
        if pd.isna(storey_str):
                return None, None
        parts = str(storey_str).split('TO')
        if len(parts) == 2:
            lower = int(parts[0].strip())
            upper = int(parts[1].strip())
            return lower, upper
        else:
            return None, None
    except:
        return None, None
        
def derive_property_characteristics(df):
    """
    Extract and calculate property characteristic fields
    
    Business Purpose:
    - Analyze floor level premium/discount (higher floors = higher price?)
    - Understand lease depreciation impact on pricing
    - Support property age analysis
    - Enable comparison of similar properties
    
    Args:
        df: DataFrame with storey_range, lease_commence_date
    
    Returns:
        DataFrame with property characteristics
    
    TODO:
    1. Parse storey_range ("01 TO 03" → storey_lower=1, storey_upper=3)
    2. Calculate storey_median = (storey_lower + storey_upper) / 2
    3. Calculate lease_age = current_year - lease_commence_date
    4. Calculate flat_age_at_sale = year (of sale) - lease_commence_date
    5. For old datasets without remaining_lease:
       - estimated_remaining_lease = 99 - lease_age
       - Format as "X years" to match newer datasets
    
    Handle parsing errors gracefully (set to null if can't parse)
    """
    # Get current year for calculations
    current_year = datetime.now().year

    

    # Apply parsing to create two new columns
    df[['storey_lower', 'storey_upper']] = df['storey_range'].apply(
        lambda x: pd.Series(parse_storey_range(x))
    )

    # 2. Calculate storey_median (average of lower and upper)
    df['storey_median'] = ((df['storey_lower'] + df['storey_upper']) / 2).round(1)

    # 3. Calculate lease_age (how old the building is now)
    df['lease_age'] = current_year - df['lease_commence_date']

    # 4. Calculate flat_age_at_sale (how old was the building when it was sold)
    df['flat_age_at_sale'] = df['year'] - df['lease_commence_date']

    # 5. Handle remaining_lease for old datasets
    # Check if remaining_lease has actual values or is all None/null
    has_remaining_lease = df['remaining_lease'].notna().any()

    if not has_remaining_lease:
        # Old datasets: calculate estimated remaining lease
        df['estimated_remaining_lease'] = 99 - df['lease_age']
        # Format as "X years" to match newer datasets
        df['remaining_lease'] = df['estimated_remaining_lease'].apply(
            lambda x: f"{int(x)} years" if pd.notna(x) and x > 0 else None
        )

    return df



# ============================================
# TIER 3: DERIVED FIELDS - LOCATION INTELLIGENCE
# ============================================

def derive_location_intelligence(df):
    """
    Add location-based intelligence fields
    
    Business Purpose:
    - Regional price comparison (North vs South vs Central)
    - Understand mature estate premium
    - Enable geographical analysis and mapping
    - Support location-based investment decisions
    
    Args:
        df: DataFrame with 'town' column
    
    Returns:
        DataFrame with location fields
    
    TODO:
    1. Map town to region using REGION_MAPPING dictionary
       - Use df['town'].map(REGION_MAPPING)
       - Fill unmapped towns with 'Others'
    
    2. Determine estate maturity
       - If town in MATURE_ESTATES → 'Mature'
       - Else → 'Non-Mature'
    
    These fields enable:
    - "Average price by region"
    - "Mature vs non-mature estate price comparison"
    - "Which region has highest appreciation?"
    """
    # 1. Map town to region using REGION_MAPPING dictionary
    df['region'] = df['town'].map(REGION_MAPPING)

    # Fill unmapped towns with 'Others'
    df['region'] = df['region'].fillna('Others')

    # 2. Determine estate maturity
    # Check if town is in MATURE_ESTATES list
    df['estate_maturity'] = df['town'].apply(
        lambda x: 'Mature' if x in MATURE_ESTATES else 'Non-Mature'
    )

    return df



# ============================================
# TIER 2: TEXT STANDARDIZATION
# ============================================

def standardize_text(df):
    """
    Standardize text fields for consistency
    
    Business Purpose:
    - Consistent filtering and grouping in analysis
    - Prevent case-sensitivity issues
    - Remove extra whitespace that causes mismatches
    - Support accurate aggregations
    
    Args:
        df: DataFrame with text columns
    
    Returns:
        DataFrame with standardized text
    
    TODO:
    Apply to these columns:
    - town
    - flat_type
    - street_name
    - flat_model
    - region
    - estate_maturity
    
    Operations:
    1. Convert to uppercase (.str.upper())
    2. Strip leading/trailing whitespace (.str.strip())
    
    Example: "  ang mo kio  " → "ANG MO KIO"
    """
    # List of text columns to standardize
    text_columns = ['town', 'flat_type', 'street_name', 'flat_model', 'region', 'estate_maturity']

    # Apply standardization to each text column
    for column in text_columns:
        if column in df.columns:
            # Chain operations: strip whitespace, then convert to uppercase
            df[column] = df[column].str.strip().str.upper()

    return df



# ============================================
# DATA QUALITY METRICS
# ============================================

def generate_quality_metrics(df_original, df_final, duplicates_removed, 
                             nulls_removed, invalid_removed, null_counts):
    """
    Generate comprehensive data quality report
    
    Business Purpose:
    - Track data quality over time
    - Identify data collection issues
    - Build trust in analysis
    - Support data governance
    
    Args:
        df_original: Original DataFrame
        df_final: Final cleaned DataFrame
        duplicates_removed: Count of duplicates
        nulls_removed: Count of null rows removed
        invalid_removed: Count of invalid rows removed
        null_counts: Dictionary of null counts per column
    
    Returns:
        dict: Quality metrics report
    
    TODO:
    Create dictionary with:
    1. Row counts (original, final, removed)
    2. Data quality score (% of rows retained)
    3. Null counts per column
    4. Value ranges (min/max/mean for numeric fields)
    5. Processing timestamp
    
    Example structure:
    {
        "original_rows": 200000,
        "final_rows": 199500,
        "duplicates_removed": 300,
        "nulls_removed": 150,
        "invalid_removed": 50,
        "data_quality_score": 99.75,
        "null_counts": {...},
        "value_ranges": {
            "resale_price": {"min": 150000, "max": 1200000, "mean": 450000},
            "price_per_sqm": {"min": 3500, "max": 9000, "mean": 5200}
        },
        "processed_at": "2024-11-14T15:30:00Z"
    }
    """
    # 1. Calculate basic counts
    original_rows = len(df_original)
    final_rows = len(df_final)
    total_removed = original_rows - final_rows

    # 2. Calculate data quality score (% of rows retained)
    data_quality_score = (final_rows / original_rows * 100) if original_rows > 0 else 0

    # 3. Calculate value ranges for numeric fields
    # CRITICAL: Convert numpy/pandas types to native Python types
    value_ranges = {
        'resale_price': {
            'min': int(df_final['resale_price'].min()),
            'max': int(df_final['resale_price'].max()),
            'mean': int(df_final['resale_price'].mean())
        },
        'price_per_sqm': {
            'min': float(df_final['price_per_sqm'].min()),
            'max': float(df_final['price_per_sqm'].max()),
            'mean': float(df_final['price_per_sqm'].mean())
        },
        'floor_area_sqm': {
            'min': float(df_final['floor_area_sqm'].min()),
            'max': float(df_final['floor_area_sqm'].max()),
            'mean': float(df_final['floor_area_sqm'].mean())
        }
    }

    # 4. Convert null_counts dictionary values to native Python int
    # This is the KEY fix - pandas Series.sum() returns int64
    null_counts_clean = {k: int(v) for k, v in null_counts.items()}

    # 5. Build quality metrics dictionary
    quality_metrics = {
        'original_rows': int(original_rows),           # Convert to Python int
        'final_rows': int(final_rows),                 # Convert to Python int
        'total_removed': int(total_removed),           # Convert to Python int
        'duplicates_removed': int(duplicates_removed), # Convert to Python int
        'nulls_removed': int(nulls_removed),           # Convert to Python int
        'invalid_removed': int(invalid_removed),       # Convert to Python int
        'data_quality_score': float(round(data_quality_score, 2)),  # Convert to Python float
        'null_counts': null_counts_clean,              # Use cleaned dict
        'value_ranges': value_ranges,
        'processed_at': datetime.now().isoformat()
    }

    return quality_metrics

# ============================================
# S3 OPERATIONS
# ============================================

def read_csv_from_s3(bucket, key):
    """
    Read CSV file from S3 into DataFrame
    
    Args:
        bucket: S3 bucket name
        key: S3 object key
    
    Returns:
        pandas DataFrame
    
    TODO:
    1. Use s3_client.get_object() to fetch file
    2. Read Body content
    3. Use pd.read_csv() with StringIO
    4. Return DataFrame
    
    Hint: 
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    df = pd.read_csv(obj['Body'])
    """
    obj = s3_client.get_object(Bucket=bucket,Key=key)
    df = pd.read_csv(obj['Body'])
    return df


def save_to_s3(df, dataset_name, quality_metrics):
    """
    Save processed DataFrame and quality report to S3
    
    Business Purpose:
    - Store cleaned data for loading layer
    - Preserve quality metrics for monitoring
    - Enable traceability of transformations
    
    Args:
        df: Processed DataFrame
        dataset_name: Name of dataset (e.g., "2017-onwards")
        quality_metrics: Quality metrics dictionary
    
    Returns:
        dict: S3 keys where files were saved
    
    TODO:
    1. Convert DataFrame to CSV
    2. Generate timestamped filename
    3. Upload CSV to: processed/{dataset_name}_clean_{timestamp}.csv
    4. Convert metrics to JSON
    5. Upload JSON to: quality_reports/{dataset_name}_quality_{timestamp}.json
    6. Return both S3 keys
    
    Use s3_client.put_object()
    """
    # 1. Convert DataFrame to CSV string
    csv_string = df.to_csv(index=False)

    # 2. Generate timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

    # 3. Build S3 keys
    csv_key = f'{S3_PROCESSED_PREFIX}{dataset_name}_clean_{timestamp}.csv'
    json_key = f'{S3_QUALITY_PREFIX}{dataset_name}_quality_{timestamp}.json'

    # 4. Upload CSV to S3
    s3_client.put_object(
        Bucket=S3_BUCKET_PROCESSED,
        Key=csv_key,
        Body=csv_string
    )

    # 5. Convert metrics to JSON and upload
    json_string = json.dumps(quality_metrics, indent=2)
    s3_client.put_object(
        Bucket=S3_BUCKET_PROCESSED,
        Key=json_key,
        Body=json_string
    )

    # 6. Return both S3 keys
    return {
        'csv_key': csv_key,
        'json_key': json_key
    }
    


# ============================================
# HELPER FUNCTIONS
# ============================================

def extract_dataset_name(s3_key):
    """
    Extract dataset name from S3 key
    
    Args:
        s3_key: e.g., "raw/2017-onwards_20241114_153045.csv"
    
    Returns:
        str: Dataset name (e.g., "2017-onwards")
    
    TODO:
    1. Remove prefix ("raw/")
    2. Split by underscore
    3. Return first part (dataset name)
    
    Example: "raw/2017-onwards_20241114_153045.csv" → "2017-onwards"
    """
    to_return = s3_key[len('raw/'):]
    mylist = to_return.split('_')
    return mylist[0]