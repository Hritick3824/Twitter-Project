from twarc import Twarc2
import pandas as pd
import config

# Authenticate Twarc with your Bearer Token
t = Twarc2(bearer_token=config.Bearer_token)

# Function to get replies for multiple tweet IDs from an Excel file within a given timeframe
def get_replies_from_excel(input_file, output_file, start_time=None, end_time=None):
    # Read tweet IDs from Excel file
    df_input = pd.read_excel(input_file, engine="openpyxl")
    
    if 'tweet_id' not in df_input.columns:
        print("Error: 'tweet_id' column not found in the input file.")
        return

    user_map = {}

    # Open the Excel file once and append after each batch
    with pd.ExcelWriter(output_file, engine="openpyxl", mode="w") as writer:
        for tweet_id in df_input["tweet_id"]:
            print(f"Fetching replies for tweet ID: {tweet_id} within the timeframe...")
            search_query = f"conversation_id:{tweet_id}"
            all_replies = []

            # Fetch tweets in the same conversation (i.e., replies) with the specified time range
            for response in t.search_all(
                query=search_query, 
                max_results=100,
                start_time=start_time, 
                end_time=end_time
            ):
                # Extract users and create a user map (name, username, bio, location)
                if "includes" in response and "users" in response["includes"]:
                    for user in response["includes"]["users"]:
                        user_map[user["id"]] = {
                            "name": user.get("name", "NA"),
                            "username": user.get("username", "NA"),
                            "bio": user.get("description", "NA"),
                            "location": user.get("location", "NA")
                        }

                # Extract relevant fields
                if "data" in response:
                    for tweet in response["data"]:
                        author_id = tweet["author_id"]
                        user_info = user_map.get(author_id, {
                            "name": "NA", 
                            "username": "NA", 
                            "bio": "NA", 
                            "location": "NA"
                        })
                        
                        all_replies.append({
                            "reply_tweet_id": tweet["id"],
                            "reply_text": tweet["text"],
                            "reply_author_id": author_id,
                            "reply_name": user_info["name"],  # Added user full name
                            "reply_username": user_info["username"],  # Added username
                            "reply_user_bio": user_info["bio"],  # Added user bio
                            "reply_user_location": user_info["location"],  # Added user location
                            "reply_created_at": tweet["created_at"],
                            "conversation_id": tweet["conversation_id"],
                            "in_reply_to_tweet_id": tweet["referenced_tweets"][0]["id"] if "referenced_tweets" in tweet else "NA",
                            "reply_like_count": tweet["public_metrics"]["like_count"],
                            "reply_retweet_count": tweet["public_metrics"]["retweet_count"],
                            "reply_count": tweet["public_metrics"]["reply_count"],
                            "quote_count": tweet["public_metrics"]["quote_count"],
                            "reply_tweet_link": f"https://twitter.com/i/web/status/{tweet['id']}"
                        })
            
            # Save results after processing each tweet ID to avoid data loss
            if all_replies:
                df_replies = pd.DataFrame(all_replies)
                df_replies.to_excel(writer, index=False, sheet_name="Replies")

                print(f"Saved replies for tweet ID {tweet_id} to {output_file}.")

    print(f"All replies saved to {output_file}.")

# Example Usage with Time Filtering
input_excel = "test_replies.xlsx"  # Input file containing tweet IDs
output_excel = "replies_with_user_details.xlsx"  # Output file to save replies with user details

# Specify the time range (Format: "YYYY-MM-DDTHH:MM:SSZ")
start_time = "2022-01-01T00:00:00Z"  # Adjust the start date/time
end_time = "2025-02-02T23:59:59Z"    # Adjust the end date/time

get_replies_from_excel(input_excel, output_excel, start_time, end_time)
