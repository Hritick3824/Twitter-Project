import tweepy
import csv
import pandas as pd
import config  # Replace with your configuration module containing the Bearer Token

# --- Twitter Data Fetching ---
# Initialize Tweepy client
client = tweepy.Client(bearer_token=config.Bearer_token)

# Input file containing Tweet IDs in the 'Permalink' column
input_file = "test.xlsx"  # Ensure this file contains a column named 'Permalink'

# Output file to save tweet details
raw_output_file = "MS_Drug_tweets_updated_1.csv"

data = pd.read_excel(input_file)

def extract_tweet_id(permalink):
    try:
        return permalink.split("status/")[-1]  # Extract tweet ID from URL
    except:
        return None

# Extract Tweet IDs from the 'Permalink' column

# data['tweet_id'] = data['Permalink'].apply(extract_tweet_id)
# tweet_ids = data['tweet_id'].dropna().astype(str).tolist()

tweet_ids = data['tweet_id'].dropna().astype(str).tolist()

# Create a CSV file and write headers
with open(raw_output_file, mode="w", encoding="utf-8-sig", newline="") as file:
    writer = csv.DictWriter(file, fieldnames=[
        "tweet_id", "text", "created_at", "author_id", "username", "name","account_creation_date", "description",
        "location", "verified", "followers_count", "following_count", "tweets_count", "media_count",
        "listed_count", "profile_url", "profile_image_url", "profile_banner_url",
        "protected_status", "external_link", "tweet_link",
        "media_key", "media_type", "media_url", "alt_text","tweet_status", "original_tweet_id","original_tweet_text", "original_tweet_link"
    ])
    writer.writeheader()

    tweet_count = 0

    for i in range(0, len(tweet_ids), 100):  # Twitter API allows max 100 IDs per request
        batch_ids = tweet_ids[i:i+100]
        try:
            # Fetch tweets
            response = client.get_tweets(
                ids=batch_ids,
                tweet_fields=["created_at", "text", "referenced_tweets"],
                user_fields=["username", "location", "verified", "public_metrics", "name", "description",
                             "created_at", "profile_image_url", "protected", "profile_banner_url", "entities"],
                expansions=["author_id", "attachments.media_keys", "referenced_tweets.id"],
                media_fields=["media_key", "type", "url", "alt_text"]
            )

            if response.data:
                users = {user["id"]: user for user in response.includes.get("users", [])}
                referenced_tweets = {tweet.id: tweet.text for tweet in response.includes.get("tweets", [])}
                media_map = {media.media_key: media for media in response.includes.get("media", [])}

                for tweet in response.data:
                    tweet_count += 1
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

        except tweepy.TooManyRequests:
            print("Rate limit reached. Pausing for 15 minutes...")
            # time.sleep(15 * 60)
        except Exception as e:
            print(f"An error occurred: {e}")
            break

print(f"Done! Total tweets saved: {tweet_count}")