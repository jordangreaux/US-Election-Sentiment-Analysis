# Dataset creation using the free tier Reddit API
import praw
import pandas as pd
from datetime import datetime
import time
import schedule

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

# Run on schedule
def scheduled_fetch():
    # Exception Handling
    retry_attempts = 5
    backoff_time = 1

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_name = f"data_{timestamp}.csv"

    while retry_attempts > 0:
        try:
            top_posts_today = subreddit.top(time_filter='day', limit=100)
            for post in top_posts_today:
                print(f'Scraping comments from post: {post.title}')
                comments = scrape_comments(post)
                all_comments.extend(comments)
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

    if all_comments:
        all_comments_df = pd.DataFrame(all_comments)
        all_comments_df.to_csv(file_name, index=False)
        print(f'Data saved to {file_name}')
    else:
        print('No comments were collected.')

schedule.every(12).hours.do(scheduled_fetch)

scheduled_fetch()

# Run continuously
while True:
    schedule.run_pending()
    time.sleep(1)
