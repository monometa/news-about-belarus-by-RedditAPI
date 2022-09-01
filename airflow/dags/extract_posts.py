import pandas as pd
import praw
from datetime import datetime
from dotenv import load_dotenv
import os
from pathlib import Path


load_dotenv()
now = datetime.now()
CURRENT_TIME = now.strftime("%H.%M")
RAW_DATA_PATH = (Path(__file__).parent / '../../data/raw/').resolve()

CLIENT_ID = os.environ["CLIENT_ID"]
SECRET_KEY = os.environ["SECRET_KEY"]
USER_NAME = os.environ["USER_NAME"]
USER_PASSWORD = os.environ["USER_PASSWORD"]
USER_AGENT = os.environ["USER_AGENT"]

SUBREDDIT = 'belarus'
TIME_FILTER = "day"
LIMIT = 5

POST_FIELDS = (
    "id",
    "title",
    "score",
    "num_comments",
    "author",
    "created_utc",  # time when the message was created / Unix Time
    "url",
    "upvote_ratio",
    "spoiler",
)


def api_connect():
    """Connect to Reddit API"""
    try:
        reddit = praw.Reddit(
            client_id=CLIENT_ID,
            client_secret=SECRET_KEY,
            password=USER_PASSWORD,
            user_agent=USER_AGENT,
            username=USER_NAME,
        )
        return reddit
    except Exception as e:
        print(f"Unable to connect to API. Error: {e}")


def get_subreddit_posts(reddit):
    """Create posts object for Reddit obj"""
    try:
        subreddit = reddit.subreddit(SUBREDDIT)
        posts = subreddit.top(time_filter=TIME_FILTER, limit=LIMIT)
        return posts
    except Exception as e:
        print(f"Cannot get posts generator. Error: {e}")


def extract_data(posts):
    """Extract Data to Pandas DataFrame"""
    list_of_posts = []
    try:
        for submission in posts:
            total_data = vars(submission)
            selected_data = {field: total_data[field] for field in POST_FIELDS}
            list_of_posts.append(selected_data)
            extracted_data_df = pd.DataFrame(list_of_posts)
    except Exception as e:
        print(f"Cannot get posts data. Error: {e}")
    return extracted_data_df


def main():
    reddit_connection = api_connect()
    posts_gen = get_subreddit_posts(reddit_connection)
    data = extract_data(posts_gen)
    data.to_csv(f"{RAW_DATA_PATH}/posts_{CURRENT_TIME}.csv", index=False)


if __name__ == "__main__":
    main()