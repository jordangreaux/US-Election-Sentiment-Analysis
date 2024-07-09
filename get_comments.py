# Dataset creation using the free tier Reddit API
import praw
import pandas as pd
from datetime import datetime
import time
import schedule
import spacy

ner = spacy.load('en_core_web_sm')

reddit = praw.Reddit(
    client_id = "CLIENT_ID_HERE",
    client_secret = "CLIENT_SECRET_HERE",
    user_agent = "USER_AGENT_HERE"
)

subreddit_name = 'politics'
subreddit = reddit.subreddit(subreddit_name)

# Get Comments
def scrape_comments(post):
    comments_with_timestamps = []
    try:
        post.comments.replace_more(limit=None)
        for comment in post.comments.list():
            timestamp = datetime.utcfromtimestamp(comment.created_utc).strftime('%Y-%m-%d')
            comments_with_timestamps.append({'comment': comment.body, 'timestamp': timestamp})
    except Exception as e:
        print(f'Error while scraping comments: {e}')
    return comments_with_timestamps

all_comments = []

# Schedule fetch, handle rate limit error
def scheduled_fetch():
    global all_comments
    retry_attempts = 5
    backoff_time = 1

    while retry_attempts > 0:
        try:
            top_posts_today = subreddit.top(time_filter='day', limit=100)
            for post in top_posts_today:
                print(f'Scraping comments from post: {post.title}')
                comments = scrape_comments(post)
                comments_df = pd.DataFrame(comments)
                all_comments = pd.concat([all_comments, comments_df], ignore_index=True)
            break  # Exit the loop if successful
        except praw.exceptions.APIException as e:
            if e.error_type == 'RATELIMIT':
                print(f'Rate limit exceeded. Retrying in {backoff_time} seconds...')
                time.sleep(backoff_time)
                retry_attempts -= 1
                backoff_time *= 2  # Exponential backoff
            else:
                print(f'APIException: {e}')
                break
        except Exception as e:
            print(f'Unexpected error: {e}')
            break

all_comments = pd.DataFrame(all_comments)

def get_entities(df):
    timestamp = datetime.now().strftime("%Y-%m-%d_%H:%M:%S")
    file_name = f"entities_{timestamp}.csv"
    data = {
        'comment': [],
        'entities': []
    }

    print('\nGetting named entities...\n')
    
    for index, row in df.iterrows():
        text = row['comment']
        doc = ner(text)
        entities = [(ent.text, ent.label_) for ent in doc.ents]

        if entities:
            data['comment'].append(text)
            data['entities'].append(entities)

    output_df = pd.DataFrame(data)
    output_df = pd.concat([all_comments['timestamp'], output_df], axis=1) # Concatenate timestamp column to prevent duplicate columns
    output_df.to_csv(file_name, index=False)
    print(f'Data saved to {file_name}')

schedule.every(12).hours.do(scheduled_fetch)
schedule.every(12).hours.do(get_entities, all_comments)

scheduled_fetch()
get_entities(all_comments)

# Run continuously
while True:
    schedule.run_pending()
    time.sleep(1)
