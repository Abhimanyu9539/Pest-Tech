import json
import csv
from avro.datafile import DataFileReader
from avro.io import DatumReader
import multiprocessing
import logging
import smtplib
from email.mime.text import MIMEText

# Configure logging
logging.basicConfig(filename='data_processing.log', level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')

# Function to send email alert
def send_email_alert(subject, body):
    sender_email = "your_email@example.com"  # Change to your email
    receiver_email = "recipient@example.com"  # Change to recipient's email
    password = "your_email_password"  # Change to your email password

    message = MIMEText(body)
    message["Subject"] = subject
    message["From"] = sender_email
    message["To"] = receiver_email

    server = smtplib.SMTP("smtp.example.com", 587)  # Change to your SMTP server
    server.starttls()
    server.login(sender_email, password)
    server.sendmail(sender_email, receiver_email, message.as_string())
    server.quit()

# Function to ingest JSON data
def ingest_json(file_path):
    try:
        with open(file_path, 'r') as f:
            for line in f:
                yield json.loads(line)
    except Exception as e:
        logging.error(f"Error ingesting JSON data: {e}")
        send_email_alert("Error in data processing", f"Error ingesting JSON data: {e}")

# Function to ingest CSV data
def ingest_csv(file_path):
    try:
        with open(file_path, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                yield row
    except Exception as e:
        logging.error(f"Error ingesting CSV data: {e}")
        send_email_alert("Error in data processing", f"Error ingesting CSV data: {e}")

# Function to ingest Avro data
def ingest_avro(file_path):
    try:
        reader = DataFileReader(open(file_path, "rb"), DatumReader())
        for record in reader:
            yield record
        reader.close()
    except Exception as e:
        logging.error(f"Error ingesting Avro data: {e}")
        send_email_alert("Error in data processing", f"Error ingesting Avro data: {e}")

# Function to process ad impressions
def process_impressions(impressions):
    try:
        processed_impressions = []
        for imp in impressions:
            # Processing logic for ad impressions
            imp['timestamp'] = imp['timestamp'].replace(' ', 'T')  # Convert timestamp format
            imp['country'] = get_country(imp['website'])  # Enrich with country information
            processed_impressions.append(imp)
        return processed_impressions
    except Exception as e:
        logging.error(f"Error processing ad impressions: {e}")
        send_email_alert("Error in data processing", f"Error processing ad impressions: {e}")

# Function to process clicks and conversions
def process_clicks_conversions(data):
    try:
        processed_data = []
        for d in data:
            # Processing logic for clicks and conversions
            if d['conversion_type'] == 'signup':  # Filter only sign-up conversions
                processed_data.append(d)
        return processed_data
    except Exception as e:
        logging.error(f"Error processing clicks and conversions: {e}")
        send_email_alert("Error in data processing", f"Error processing clicks and conversions: {e}")

# Function to correlate ad impressions with clicks and conversions
def correlate_data(impressions, clicks_conversions):
    try:
        correlated_data = []
        for imp in impressions:
            for cc in clicks_conversions:
                if imp['user_id'] == cc['user_id']:
                    correlated_data.append({'user_id': imp['user_id'], 'timestamp': imp['timestamp'], 'action': 'click' if 'ad_campaign_id' in cc else 'conversion'})
        return correlated_data
    except Exception as e:
        logging.error(f"Error correlating data: {e}")
        send_email_alert("Error in data processing", f"Error correlating data: {e}")

# Dummy function to get country from website (replace with actual implementation)
def get_country(website):
    return "US"

if __name__ == "__main__":
    # Define file paths
    json_file = 'ad_impressions.json'
    csv_file = 'clicks_conversions.csv'
    avro_file = 'bid_requests.avro'

    # Create multiprocessing Pool
    pool = multiprocessing.Pool()

    # Ingest data in parallel
    json_data = pool.map(ingest_json, [json_file])[0]
    csv_data = pool.map(ingest_csv, [csv_file])[0]
    avro_data = pool.map(ingest_avro, [avro_file])[0]

    # Process data in parallel
    standardized_impressions = pool.map(process_impressions, [json_data])[0]
    unique_clicks_conversions = pool.map(process_clicks_conversions, [csv_data])[0]
    correlated_data = pool.map(correlate_data, [standardized_impressions, unique_clicks_conversions])[0]

    # Print correlated data
    for item in correlated_data:
        print(item)

    # Close the pool
    pool.close()
    pool.join()
