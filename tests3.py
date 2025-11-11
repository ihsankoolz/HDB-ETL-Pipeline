import boto3
from datetime import datetime

# Create S3 client
s3 = boto3.client('s3')

# Test data
test_content = f"Test file created at {datetime.now()}"

# Upload to your bucket
bucket_name = 'hdb-resale-raw-data-ihsan'  # Change to your bucket name
file_name = 'test_upload.txt'

try:
    s3.put_object(
        Bucket=bucket_name,
        Key=file_name,
        Body=test_content
    )
    print(f"✅ Successfully uploaded {file_name} to {bucket_name}")
    
    # List files in bucket
    response = s3.list_objects_v2(Bucket=bucket_name)
    print("\n📁 Files in bucket:")
    for obj in response.get('Contents', []):
        print(f"  - {obj['Key']}")
        
except Exception as e:
    print(f"❌ Error: {e}")