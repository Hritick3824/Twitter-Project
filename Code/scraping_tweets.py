import tweepy
import csv
import pandas as pd
import config  # Replace with your configuration module containing the Bearer Token

# --- Twitter Data Fetching ---
# Initialize Tweepy client
client = tweepy.Client(bearer_token=config.Bearer_token)

# File to save raw data
raw_output_file = "tweets_data_English_tweets_4-5.csv"

# Initialize tweet counter
tweet_count = 0

# Create a CSV file and write headers
with open(raw_output_file, mode="w", encoding="utf-8-sig", newline="") as file:
    writer = csv.DictWriter(file, fieldnames=[
        "tweet_id", "text", "created_at", "author_id", "username", "name","account_creation_date", "description",
        "location", "verified", "followers_count", "following_count", "tweet_count", "media_count",
        "listed_count", "profile_url", "profile_image_url", "profile_banner_url",
        "protected_status", "external_link", "tweet_link",
        "media_key", "media_type", "media_url", "alt_text", "original_tweet_text", "tweet_status"
    ])
    writer.writeheader()

    # Define the search query and parameters
    # Full English query
    query = '(Glaucoma OR Wet Macular Degeneration OR Dry Macular Degeneration OR Macula OR Ophtha OR Corneal Dystrophy OR Diabetic Retinopathy OR Diabetic Macular Edema OR Vabysmo OR Eylea OR Lucentis OR Ranibizumab OR Aflibercept OR Faricimab OR Pegcetacoplan OR Syfovre OR Ophthalmology OR Vascular endothelial growth factor OR Neovascular Age-Related Macular Degeneration) lang:en'

    # English query
    # query = '(Glaucoma OR Wet Macular Degeneration OR Dry Macular Degeneration OR Macula lutea OR Ophtha OR Corneal Dystrophy OR Diabetic Retinopathy OR Diabetic Macular Edema OR Vabysmo OR Eylea OR Lucentis OR Ranibizumab OR Aflibercept OR Faricimab OR Pegcetacoplan OR Syfovre OR Ophthalmology OR Vascular endothelial growth factor) place_country:JA'
   
    # Translated query
    # query = '(緑内障 OR 湿性加齢黄斑変性症 OR 乾性加齢黄斑変性症 OR 黄斑  OR 角膜ジストロフィー OR 糖尿病網膜症 OR 糖尿病黄斑浮腫 OR Vabysmo OR Eylea OR Lucentis OR Ranibizumab OR Aflibercept OR Faricimab OR Pegcetacoplan OR Syfovre OR 眼科学 OR 血管内皮増殖因子) lang:ja'
    start_time = "2024-02-01T00:00:00Z"
    end_time = "2024-05-30T19:36:59Z"
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
                    tweet_count = user.get("public_metrics", {}).get("tweet_count", "NA")
                    listed_count = user.get("public_metrics", {}).get("listed_count", "NA")
                    media_count = user.get("public_metrics", {}).get("media_count", "NA")
                    # profile_url = f"https://twitter.com/{username}" if username != "NA" else "NA"
                    profile_image_url = user.get("profile_image_url", "NA")
                    profile_banner_url = user.get("profile_banner_url", "NA")
                    protected_status = user.get("protected", "NA")
                    account_creation_date = user.get("created_at", "NA")

                    external_link = "NA"
                    if user.get("entities") and "url" in user["entities"]:
                        urls = user["entities"]["url"].get("urls", [])
                        if urls and "expanded_url" in urls[0]:
                            external_link = urls[0]["expanded_url"]

                    # Determine the tweet status and initialize original tweet text
                    original_tweet_text = "NA"
                    tweet_status = "Original"

                    if tweet.referenced_tweets:
                        for ref in tweet.referenced_tweets:
                            if ref.type == "retweeted":
                                original_tweet_text = referenced_tweets.get(ref.id, "NA")
                                tweet_status = "Retweet"
                            elif ref.type == "quoted":
                                tweet_status = "Quoted Tweet"

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
                        "tweet_count": tweet_count,
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
data = pd.read_csv(raw_output_file)

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

# Save the cleaned data
# cleaned_file_path = "Cleaned_tweets_data_Germany_All.csv"
# data.to_csv(cleaned_file_path, index=False, encoding='utf-8-sig')

# --- Replace Retweets with Original Tweets ---
# Replace text column with original tweet text where available
data['text'] = data['original_tweet_text'].combine_first(data['text'])
data['tweet_id'] = data["tweet_id"].astype(str)
data["author_id"]  = data["author_id"].astype(str)
# Save the cleaned and updated data
updated_file_path = r"Output_Scraped_data\English_tweets\tweets_data_English_tweets_4-5.csv"
data.to_csv(updated_file_path, index=False, encoding="utf-8-sig")
print(f"Updated file saved to: {updated_file_path}")

