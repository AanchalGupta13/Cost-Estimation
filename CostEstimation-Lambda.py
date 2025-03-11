import json
import boto3
import pandas as pd
import io
import re
import os
import logging

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Create AWS Clients
ec2_client = boto3.client('ec2')
s3_client = boto3.client('s3')
pricing_client = boto3.client('pricing', region_name='us-east-1')  # AWS Pricing API works in us-east-1

# Fetch Environment variables for S3
BUCKET_NAME = os.environ.get('S3_BUCKET_NAME', 'price-inventory')
FILE_KEY = os.environ.get('S3_FILE_KEY', 'physical_servers_inventory.xlsx')
CSV_FILE_KEY = "matched_ec2_instances.csv"  # CSV file name

# Fetch Available EC2 instance types
def fetch_ec2_instance_types():
    try:
        response = ec2_client.describe_instance_types()
        instance_data = {}
        
        for instance in response['InstanceTypes']:
            instance_data[instance['InstanceType']] = {
                "vCPUs": instance['VCpuInfo']['DefaultVCpus'],
                "MemoryMiB": instance['MemoryInfo']['SizeInMiB'] // 1024  # Convert MB to GB
            }
        return instance_data      #returns a dictionary of Instance Type with vCPUs and Memory in GB

    except Exception as e:
        logger.error(f"Error fetching EC2 instance types: {e}")
        return {}

# Fetch and read the Excel file from S3
def fetch_requirements_from_s3():
    try:
        s3_object = s3_client.get_object(Bucket=BUCKET_NAME, Key=FILE_KEY)
        file_stream = io.BytesIO(s3_object['Body'].read())
        df = pd.read_excel(file_stream)
        return df.to_dict(orient="records")

    except Exception as e:
        logger.error(f"Error fetching file from S3: {e}")
        return []

# Extract CPU and RAM from requirements
def extract_cpu_ram(requirements):
    filtered_requirements = []
    for req in requirements:
        cpu_match = re.search(r'(\d+)\s+Cores', req['CPU'])
        ram_match = re.search(r'(\d+)GB', req['RAM'])
        
        if cpu_match and ram_match:
            filtered_requirements.append({
                'Server Name': req['Server Name'],
                'IP Address': req['IP Address'],
                'CPU': int(cpu_match.group(1)),
                'RAM': int(ram_match.group(1))
            })
    return filtered_requirements  #returns a list with server name, ip, cpu and ram in  numeric only

# Find best matching instances
def find_best_match(filtered_data, ec2_instances):
    matched_instances = []
    
    for req in filtered_data:
        best_match = None
        for instance_name, instance in ec2_instances.items():
            if instance["vCPUs"] >= req["CPU"] and instance["MemoryMiB"] >= req["RAM"]:
                if best_match is None or (instance["vCPUs"] < best_match["vCPUs"] or instance["MemoryMiB"] < best_match["MemoryMiB"]):
                    best_match = {
                        "InstanceType": instance_name,
                        "vCPUs": instance["vCPUs"],
                        "MemoryMiB": instance["MemoryMiB"]
                    }

        if best_match:
            matched_instances.append({
                "Server Name": req["Server Name"],
                "IP Address": req["IP Address"],
                **best_match
            })
    
    return matched_instances        #returns a list of matched instances with server details

# Get instance pricing
def get_instance_price(instance_type, region='US East (N. Virginia)'):
    try:
        response = pricing_client.get_products(
            ServiceCode='AmazonEC2',
            Filters=[
                {'Type': 'TERM_MATCH', 'Field': 'instanceType', 'Value': instance_type},
                {'Type': 'TERM_MATCH', 'Field': 'location', 'Value': region},
                {'Type': 'TERM_MATCH', 'Field': 'operatingSystem', 'Value': 'Linux'},
                {'Type': 'TERM_MATCH', 'Field': 'tenancy', 'Value': 'Shared'},
                {'Type': 'TERM_MATCH', 'Field': 'preInstalledSw', 'Value': 'NA'},
                {'Type': 'TERM_MATCH', 'Field': 'capacitystatus', 'Value': 'Used'}
            ]
        )

        price_data = response['PriceList']
        if not price_data:
            return None

        price_json = json.loads(price_data[0])
        price_per_hour = float(next(iter(price_json['terms']['OnDemand'].values()))['priceDimensions'].values().__iter__().__next__()['pricePerUnit']['USD'])
        return price_per_hour    #return the hourly price for an instance

    except Exception as e:
        logger.error(f"Error fetching price for {instance_type}: {e}")
        return None

# Function to store results in S3 as a CSV file
def store_results_in_s3_csv(data, bucket=BUCKET_NAME, key=CSV_FILE_KEY):
    try:
        # Convert list of dictionaries to DataFrame
        df = pd.DataFrame(data)

        # Convert DataFrame to CSV format
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)

        # Upload to S3
        s3_client.put_object(
            Bucket=bucket, 
            Key=key, 
            Body=csv_buffer.getvalue(), 
            ContentType='text/csv'
        )

        logger.info(f"Results successfully uploaded to S3: s3://{bucket}/{key}")
        return True
    except Exception as e:
        logger.error(f"Error uploading CSV to S3: {e}")
        return False

# Lambda handler function
def lambda_handler(event, context):
    try:
        # Fetch Data
        ec2_instances = fetch_ec2_instance_types()
        requirements = fetch_requirements_from_s3()
        
        if not requirements:
            return {"statusCode": 500, "body": json.dumps("Failed to fetch requirements from S3.")}

        # Process Data
        filtered_data = extract_cpu_ram(requirements)
        matched_instances = find_best_match(filtered_data, ec2_instances)

        # Estimate Monthly Cost
        for instance in matched_instances:
            hourly_price = get_instance_price(instance['InstanceType'])
            instance['Monthly Cost'] = round(hourly_price * 24 * 30, 2) if hourly_price is not None else "Price Not Available"

        # Store results as CSV in S3
        success = store_results_in_s3_csv(matched_instances)
        if not success:
            return {"statusCode": 500, "body": json.dumps("Failed to upload CSV results to S3.")}

        return {"statusCode": 200, "body": json.dumps(f"CSV stored successfully at s3://{BUCKET_NAME}/{CSV_FILE_KEY}")}

    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return {"statusCode": 500, "body": json.dumps(f"Unexpected error: {e}")}
