from apify_client import ApifyClient
import config
import json
import pandas as pd
import os
import requests

# Initialize the ApifyClient with your Apify API token
client = ApifyClient(config.Apify_key)

# Load usernames from an Excel file
excel_file = "User.xlsx"  # Replace with your Excel file path
username_column = "username"  # Replace with the column name containing usernames

data = pd.read_excel(excel_file)
usernames = data[username_column].dropna().str.strip().tolist()

# Prepare the Actor input
run_input = {
    "user_names": usernames,  # List of usernames from the Excel file
    "user_ids": [], # Optionally, specify user IDs if known
    "maxFollowers": 1000000, # Specify the maximum number of followers to retrieve
    "maxFollowings": 1000000, # Specify the maximum number of following to retrieve
    "getFollowers": True,
    "getFollowing": True,
}

# Function to resurrect a failed run
def resurrect_run(run_id, api_token):
    url = f"https://api.apify.com/v2/actor-runs/{run_id}/resurrect?token={api_token}"
    response = requests.post(url)
    if response.status_code == 200:
        print(f"Run {run_id} resurrected successfully.")
        return response.json()
    else:
        print(f"Failed to resurrect run {run_id}: {response.text}")
        return None

# Run the Actor and wait for it to finish
try:
    run = client.actor("kaitoeasyapi/premium-x-follower-scraper-following-data").call(run_input=run_input)
    run_id = run["id"]  # Save the run ID immediately
    print(f'The run ID is:{run_id}')
    # Print the dataset URL
    print("💾 Check your data here: https://console.apify.com/storage/datasets/" + run["defaultDatasetId"])

    # Fetch the Actor results and save them to a JSON file
    results = []
    for item in client.dataset(run["defaultDatasetId"]).iterate_items():
        results.append(item)

    # Extract specific fields and save them to a CSV file
    fields = ["target_username", "type", "id_str", "name", "screen_name", "description", "location", "url", 
        "protected", "followers_count", "friends_count", "listed_count", "created_at", 
        "verified", "profile_image_url_https", "profile_banner_url", "statuses_count", "media_count"]

    # Convert the results to a DataFrame and save to CSV
    df = pd.DataFrame(results, columns=fields)
    df["id_str"] = df["id_str"].astype(str)
    csv_file_path = "following_followers_Accounts.csv"
    df.to_csv(csv_file_path, index=False, encoding= 'utf-8-sig')

    # Automatically open the CSV file
    os.startfile(csv_file_path)

    print("Results saved to excel file")

except Exception as e:
    print(f"An error occurred: {e}")
    # Attempt to resurrect the run if it failed
    try:
        run_id = run["id"]  # Check if run ID was created
    except NameError:
        print("Run ID not available. Resurrection cannot proceed.")
    else:
        resurrect_run(run_id, config.Apify_key)
