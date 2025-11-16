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
    'KALLANG/WHAMPOA': 'Central',
    'MARINE PARADE': 'Central',
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
    pass


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
    pass


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
    pass


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
    pass


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
    pass


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
    pass


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
       - Minimum: $100,000 (too cheap = likely error)
       - Maximum: $1,500,000 (HDB ceiling)
    
    2. Floor area range:
       - Minimum: 30 sqm (smallest HDB units)
       - Maximum: 200 sqm (largest HDB units)
    
    3. Price per sqm sanity check:
       - Minimum: $2,000/sqm (unrealistically cheap)
       - Maximum: $15,000/sqm (unrealistically expensive for HDB)
    
    4. Lease commence date:
       - Minimum: 1960 (before HDB existed)
       - Maximum: current year (future dates invalid)
    
    5. Storey range format:
       - Must contain "TO" (e.g., "01 TO 03")
    
    Remove rows that violate any rule and return count
    """
    pass


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
    pass


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
    pass


# ============================================
# TIER 2 & 3: DERIVED FIELDS - PROPERTY CHARACTERISTICS
# ============================================

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
    pass


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
    pass


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
    pass


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
    pass


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
    pass


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
    pass


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
    pass