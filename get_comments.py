# Dataset creation using the free tier Reddit API
import praw
import pandas as pd
from datetime import datetime

reddit = praw.Reddit(
    client_id = "CLIENT_ID_HERE",
    client_secret = "CLIENT_SECRET_HERE",
    user_agent = "USER_AGENT_HERE"
)

subreddit_name = 'politics'

subreddit = reddit.subreddit(subreddit_name)
top_posts_today = subreddit.top(time_filter='day', limit=100)

def scrape_comments(post):
    comments_with_timestamps = []
    post.comments.replace_more(limit=None)
    for comment in post.comments.list():
        timestamp = datetime.utcfromtimestamp(comment.created_utc).strftime('%Y-%m-%d %H:%M:%S')
        comments_with_timestamps.append({'comment': comment.body, 'timestamp': timestamp})
    return comments_with_timestamps

all_comments = []
for post in top_posts_today:
    print(f'Scraping comments from post: {post.title}')
    comments = scrape_comments(post)
    all_comments.extend(comments)

all_comments = pd.DataFrame(all_comments)
all_comments.to_csv('2024-06-30.csv')
