import tweepy
import csv
import pandas as pd
import config  # Replace with your configuration module containing the Bearer Token

# --- Twitter Data Fetching ---
# Initialize Tweepy client
client = tweepy.Client(bearer_token=config.Bearer_token)

# File to save raw data
raw_output_file = r"Partial_Scraped_data\MS_Drug_tweets.csv"

# Initialize tweet counter
tweet_count = 0

# Create a CSV file and write headers
with open(raw_output_file, mode="w", encoding="utf-8-sig", newline="") as file:
    writer = csv.DictWriter(file, fieldnames=[
        "tweet_id", "text", "created_at", "author_id", "username", "name","account_creation_date", "description",
        "location", "verified", "followers_count", "following_count", "tweets_count", "media_count",
        "listed_count", "profile_url", "profile_image_url", "profile_banner_url",
        "protected_status", "external_link", "tweet_link",
        "media_key", "media_type", "media_url", "alt_text","tweet_status","original_tweet_id", "original_tweet_text", "original_tweet_link"
    ])
    writer.writeheader()

    # Define the search query and parameters

    # query = '("Americas Committee for the Treatment and Research" OR "ACTRIMS 2024" OR "CMSC" OR "Consortium of Multiple Sclerosis Centers" OR "American academy of neurology") lang:en -is:retweet'
    query = '''(IBD OR Crohns OR Colitis OR "Crohns" OR "Crohn's" OR "Ulcerative Colitis") (Skyrizi OR Stelara OR risankizumab OR ustekinumab) -is:retweet profile_country:US'''
    
    start_time = "2024-01-01T00:00:00Z"
    end_time = "2025-02-05T23:59:59Z"
    max_results = 100
    next_token = None

    while True:
        try:
            # Fetch tweets with pagination
            response = client.search_all_tweets(
                query=query,
                start_time=start_time,
                end_time=end_time,
                max_results=max_results,
                tweet_fields=["created_at", "text", "referenced_tweets"],
                user_fields=["username", "location", "verified", "public_metrics", "name", "description",
                             "created_at", "profile_image_url", "protected", "profile_banner_url", "entities"],
                expansions=["author_id", "attachments.media_keys", "referenced_tweets.id"],
                media_fields=["media_key", "type", "url", "alt_text"],
                next_token=next_token
            )

            if response.data:
                # Map users and media
                users = {user["id"]: user for user in response.includes.get("users", [])}
                referenced_tweets = {tweet.id: tweet.text for tweet in response.includes.get("tweets", [])}
                media_map = {media.media_key: media for media in response.includes.get("media", [])}

                for tweet in response.data:
                    tweet_count += 1
                    
                    # Get user details
                    user = users.get(tweet.author_id, {})
                    username = user.get("username", "NA")
                    name = user.get("name", "NA")
                    description = user.get("description", "NA")
                    location = user.get("location", "NA")
                    verified = user.get("verified", "NA")
                    followers_count = user.get("public_metrics", {}).get("followers_count", "NA")
                    following_count = user.get("public_metrics", {}).get("following_count", "NA")
                    tweets_count = user.get("public_metrics", {}).get("tweet_count", "NA")
                    listed_count = user.get("public_metrics", {}).get("listed_count", "NA")
                    media_count = user.get("public_metrics", {}).get("media_count", "NA")
                    profile_image_url = user.get("profile_image_url", "NA")
                    profile_banner_url = user.get("profile_banner_url", "NA")
                    protected_status = user.get("protected", "NA")
                    account_creation_date = user.get("created_at", "NA")

                    external_link = "NA"
                    if user.get("entities") and "url" in user["entities"]:
                        urls = user["entities"]["url"].get("urls", [])
                        if urls and "expanded_url" in urls[0]:
                            external_link = urls[0]["expanded_url"]
                        
                    original_tweet_text = "NA"
                    original_tweet_id = "NA"
                    tweet_status = "Original"

                    if tweet.referenced_tweets:
                        for ref in tweet.referenced_tweets:
                            if ref.type == "retweeted":
                                original_tweet_text = referenced_tweets.get(ref.id, "Protected or Deleted")
                                original_tweet_id = str(ref.id)
                                tweet_status = "Retweet"
                            elif ref.type == "quoted":
                                original_tweet_text = referenced_tweets.get(ref.id, "Protected or Deleted")
                                original_tweet_id = str(ref.id)
                                tweet_status = "Quoted Tweet"
                            elif ref.type == "replied_to":
                                original_tweet_text = referenced_tweets.get(ref.id, "Protected or Deleted")
                                original_tweet_id = str(ref.id)
                                tweet_status = "Reply Tweet"

                    # Correctly classify Threaded Tweets (Self-Replies)
                    original_tweet_author = None

                    if tweet.referenced_tweets:
                        for ref in tweet.referenced_tweets:
                            if ref.id in referenced_tweets:
                                # Get the referenced tweet from response.includes["tweets"]
                                referenced_tweet = next((t for t in response.includes.get("tweets", []) if t.id == ref.id), None)
                                
                                if referenced_tweet:
                                    original_tweet_author = referenced_tweet.author_id  # Extract correct author_id

                    if tweet_status == "Reply Tweet" and original_tweet_author and str(tweet.author_id) == str(original_tweet_author):
                        tweet_status = "Threaded Tweet"


                        
                    # Get media details
                    media_key = "NA"
                    media_type = "NA"
                    media_url = "NA"
                    alt_text = "NA"
                    if "attachments" in tweet and "media_keys" in tweet.attachments:
                        for key in tweet.attachments["media_keys"]:
                            media = media_map.get(key)
                            if media:
                                media_key = media.media_key
                                media_type = media.type
                                media_url = media.url
                                alt_text = media.alt_text or "NA"

                    writer.writerow({
                        "tweet_id": str(tweet.id),
                        "text": tweet.text,
                        "original_tweet_text": original_tweet_text,
                        "tweet_status": tweet_status,
                        "created_at": tweet.created_at,
                        "author_id": str(tweet.author_id),
                        "username": username,
                        "name": name,
                        "account_creation_date": account_creation_date,
                        "description": description,
                        "location": location,
                        "verified": verified,
                        "followers_count": followers_count,
                        "following_count": following_count,
                        "tweets_count": tweets_count,
                        "media_count": media_count,
                        "listed_count": listed_count,
                        "profile_url": f"https://x.com/i/user/{tweet.author_id}",
                        "profile_image_url": profile_image_url,
                        "profile_banner_url": profile_banner_url,
                        "protected_status": protected_status,
                        "external_link": external_link,
                        "tweet_link": f"https://twitter.com/i/web/status/{tweet.id}",
                        "media_key": media_key,
                        "media_type": media_type,
                        "media_url": media_url,
                        "alt_text": alt_text,
                        "original_tweet_link": f"https://twitter.com/i/web/status/{original_tweet_id}" if original_tweet_id != "NA" else "NA",
                        "original_tweet_id" : str(original_tweet_id)
                    })
                    print(f"Tweet:{tweet_count}, Date:{tweet.created_at}")

                next_token = response.meta.get("next_token")
                if not next_token:
                    print(f"All tweets fetched. Total tweets retrieved: {tweet_count}")
                    break
            else:
                print(f"No tweets found. Total tweets retrieved: {tweet_count}")
                break

        except tweepy.TooManyRequests:
            print("Rate limit reached. Pausing for 15 minutes...")
            print(f"All tweets fetched. Total tweets retrieved: {tweet_count}")
            # time.sleep(15 * 60)


        except Exception as e:
            print(f"An error occurred: {e}")
            break

print(f"Done! Total tweets saved: {tweet_count}")

# --- Data Cleaning ---
# Load the raw data
data = pd.read_csv(raw_output_file,dtype={'original_tweet_id': str,'author_id': str,'tweet_id':str})

# Function to clean string columns
def clean_text(text):
    if pd.isna(text):
        return text
    elif isinstance(text, str):
        return ' '.join(text.strip().split())
    else:
        return text

string_columns = data.select_dtypes(include=['object']).columns
for column in string_columns:
    data[column] = data[column].apply(clean_text)
data['original_tweet_id'] = (data['original_tweet_id'].astype(str).replace("nan", "").replace("NaN", ""))

# Save the cleaned data
# cleaned_file_path = "Cleaned_tweets_data_Germany_All.csv"
# data.to_csv(cleaned_file_path, index=False, encoding='utf-8-sig')

# --- Replace Retweets with Original Tweets ---

# Replace text column with original tweet text where available

# data['text'] = data['original_tweet_text'].combine_first(data['text'])
data['tweet_id'] = data["tweet_id"].astype(str)
data["author_id"]  = data["author_id"].astype(str)
data['original_tweet_id'] = data["original_tweet_id"].astype(str)
# Save the cleaned and updated data
updated_file_path = r"Output_Scraped_data\test.csv"
data.to_csv(updated_file_path, index=False, encoding="utf-8-sig")
print(f"Updated file saved to: {updated_file_path}")

