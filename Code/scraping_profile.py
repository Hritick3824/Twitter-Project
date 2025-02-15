import tweepy
import pandas as pd
from tqdm import tqdm
import time
import config
from datetime import datetime

# Initialize the Tweepy Client
client = tweepy.Client(bearer_token=config.Bearer_token)

# Load the Excel file containing usernames
input_file_path = r'Input_data_for_scraping_userprofiles\English_Users.xlsx'
twitter_handles_df = pd.read_excel(input_file_path)

# Clean the usernames: Remove leading/trailing whitespaces and drop blanks
twitter_handles_df['username'] = twitter_handles_df['username'].str.strip()
twitter_handles_df = twitter_handles_df[twitter_handles_df['username'].notna()]

# Prepare a list to store scraped data
scraped_data = []

# Counter for partial file versions
partial_file_counter = 1

# Function to monitor rate limits
def check_rate_limit(response):
    remaining = int(response.headers.get('x-rate-limit-remaining', 1))
    reset_time = int(response.headers.get('x-rate-limit-reset', time.time() + 60))
    return remaining, reset_time

# Use tqdm for a progress bar
for username in tqdm(twitter_handles_df['username'], desc="Scraping Twitter Data"):
    username = username.strip()  # Ensure no leading or trailing spaces
    while True:
        try:
            user = client.get_user(
                username=username,
                user_fields=[
                    'id', 'name', 'username', 'created_at', 'description',
                    'public_metrics', 'location', 'entities', 'verified',
                    'profile_image_url', 'protected', 'profile_banner_url'
                ]
            )

            # Handle missing user data
            if user.data is None:
                print(f"[WARNING] No data found for username: {username}. Skipping.")
                scraped_data.append({
                    'User ID': 'Not Found',
                    'Name': 'Not Found',
                    'Username': username,
                    'Profile URL': f"https://twitter.com/{username}",
                    'Profile Image URL': 'Not Found',
                    'Profile Banner URL': 'Not Found',
                    'Protected Status': 'Not Found',
                    'Description': 'Not Found',
                    'Location': 'Not Found',
                    'Followers Count': 'Not Found',
                    'Following Count': 'Not Found',
                    'Tweet Count': 'Not Found',
                    'Media Count': 'Not Found',
                    'Listed Count': 'Not Found',
                    'Account Creation Date': 'Not Found',
                    'Verified Status': 'Not Found',
                    'External Link in Bio': 'Not Found'
                })
                break  # Skip to the next username

            external_link = 'N/A'
            if user.data.entities:
                if 'url' in user.data.entities and 'urls' in user.data.entities['url']:
                    urls = user.data.entities['url']['urls']
                    if len(urls) > 0 and 'expanded_url' in urls[0]:
                        external_link = urls[0]['expanded_url']

            # Construct profile URL
            profile_url = f"https://twitter.com/{user.data.username}"

            # Extract data
            profile_image_url = user.data.profile_image_url if hasattr(user.data, 'profile_image_url') else 'N/A'
            profile_banner_url = user.data.profile_banner_url if hasattr(user.data, 'profile_banner_url') else 'N/A'
            listed_count = user.data.public_metrics.get('listed_count', 'N/A')
            media_count = user.data.public_metrics.get('media_count', 'N/A')  # line to retrieve media count
            protected_status = user.data.protected

            scraped_data.append({
                'User ID': user.data.id,
                'Name': user.data.name,
                'Username': user.data.username,
                'Profile URL': profile_url,
                'Profile Image URL': profile_image_url,
                'Profile Banner URL': profile_banner_url,
                'Protected Status': protected_status,
                'Description': user.data.description,
                'Location': user.data.location if user.data.location else 'N/A',
                'Followers Count': user.data.public_metrics['followers_count'],
                'Following Count': user.data.public_metrics['following_count'],
                'Tweet Count': user.data.public_metrics['tweet_count'],
                'Media Count': media_count,  # Include the media count in the output
                'Listed Count': listed_count,
                'Account Creation Date': user.data.created_at,
                'Verified Status': user.data.verified,
                'External Link in Bio': external_link})

            break  # Exit the while loop on successful fetch
        except tweepy.TooManyRequests as e:
            # Save progress to a local CSV file with incrementing filename
            partial_output_file = f'Partial_Scraped_data\Twitter_Scraped_Data_Partial_german_users{partial_file_counter}.csv'
            pd.DataFrame(scraped_data).to_csv(partial_output_file, index=False, encoding='utf-8-sig')
            print(f"[PROGRESS SAVED] Partial data saved locally to {partial_output_file}")
            partial_file_counter = partial_file_counter + 1  # Increment the counter for the next file

            # Handle rate limit exception
            remaining, reset_time = check_rate_limit(e.response)
            sleep_time = max(0, reset_time - time.time())
            print(f"[PAUSE] Rate limit reached. Sleeping for {sleep_time} seconds...")
            time.sleep(sleep_time)
            print("[RESUME] Resuming data scraping...")
        except tweepy.TweepyException as e:
            # Log other errors and continue with the next username
            print(f"[ERROR] Failed to fetch data for username: {username}. Error: {e}")
            scraped_data.append({
                'User ID': 'Error',
                'Name': 'Error',
                'Username': username,
                'Profile URL': f"https://twitter.com/{username}",
                'Profile Image URL': 'Error',
                'Profile Banner URL': 'Error',
                'Protected Status': 'Error',
                'Description': 'Error',
                'Location': 'Error',
                'Followers Count': 'Error',
                'Following Count': 'Error',
                'Tweet Count': 'Error',
                'Media Count': 'Error',  # Add media count to error case
                'Listed Count': 'Error',
                'Account Creation Date': 'Error',
                'Verified Status': 'Error',
                'External Link in Bio': str(e)
            })
            break  # Skip to the next username on other errors

# Convert the list of scraped data to a DataFrame
scraped_data_df = pd.DataFrame(scraped_data)
scraped_data_df['Account Creation Date'] = pd.to_datetime(scraped_data_df['Account Creation Date'], errors='coerce')
scraped_data_df['Account Creation Date'] = scraped_data_df['Account Creation Date'].dt.strftime('%a %b %d %H:%M:%S %z %Y')
# Save the DataFrame to a final CSV file
output_file_path = 'Partial_Scraped_data\Twitter_Scraped_Data_userprofiles.csv'
scraped_data_df.to_csv(output_file_path, index=False, encoding='utf-8-sig')
print(f"Final scraped data has been saved locally to {output_file_path}")

# Cleaning the data
# Load the data
data = pd.read_csv(output_file_path)

# Function to clean string columns by stripping leading/trailing whitespace and collapsing multiple spaces
def clean_text(text):
    if pd.isna(text):
        return text
    else:
        # Strip leading/trailing whitespace and replace multiple spaces with a single space
        return ' '.join(text.strip().split())

# Apply the cleaning function to all string columns in the dataframe
string_columns = data.select_dtypes(include=['object']).columns
data_cleaned = data.copy()
for column in string_columns:
    data_cleaned[column] = data[column].apply(clean_text)

data_cleaned["User ID"] = data_cleaned['User ID'].astype(str)
# Save the cleaned data
cleaned_file_path = "Output_Scraped_data\Cleaned_Twitter_Scraped_Data_Profiles_English_0.xlsx"
data_cleaned.to_excel(cleaned_file_path, index=False)

print(f"Final scraped data has been cleaned and saved locally to {cleaned_file_path}")
