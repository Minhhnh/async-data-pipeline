import csv
from datetime import datetime
import os
import tracemalloc
import aiofiles
import asyncio

from asyncdatapipeline.monitoring import PipelineMonitor
from asyncdatapipeline.config import PipelineConfig
from asyncdatapipeline.sources import twitter_source


# Enable tracemalloc to track memory allocations
tracemalloc.start()


async def write_batch_to_file(filename, batch):
    """Write a batch of tweets to CSV file"""
    try:
        async with aiofiles.open(filename, 'a', newline='', encoding='utf-8') as f:
            for tweet_time, tweet_text in batch:
                await f.write(f"{tweet_time},{tweet_text}\n")
    except Exception as e:
        # Log but don't crash the application
        monitor = PipelineMonitor()
        monitor.log_event(f"Error writing to file {filename}: {str(e)}")


async def main():
    """Main function to run the Twitter client"""
    # Load credentials and initialize client
    config = PipelineConfig(
        max_concurrent_tasks=5
    )
    monitor = PipelineMonitor()

    # Get current date and date 2 years ago for query
    current_date = datetime.now().strftime("%Y-%m-%d")
    two_years_ago = datetime.now().replace(year=datetime.now().year - 2).strftime("%Y-%m-%d")

    QUERY = f'(from:elonmusk) lang:en until:{current_date} since:{two_years_ago}'

    tweet_count = 0
    max_tweets = 100  # Set a limit for the number of tweets to process

    # Create CSV file with timestamp in filename
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_filename = f"inputs/tweets_{timestamp}.csv"
    # Check if file exists, if not create it
    if not os.path.exists(csv_filename):
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(csv_filename), exist_ok=True)

    # Create CSV file and write header
    with open(csv_filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["Tweet_count", "Username", "Text", "Created At", "Retweets", "Likes"])

    monitor.log_event(f"Created CSV file: {csv_filename}")

    async for tweet in twitter_source(config.twitter_credentials, monitor, query=QUERY):
        print(f"Tweet {tweet}")  # Print first 50 chars of tweet
        tweet_count += 1
        # Append tweet to CSV file
        tweet['Text'] = tweet['Text'].replace(",", " ").replace("\n", " ")
        async with aiofiles.open(csv_filename, "a", newline="", encoding="utf-8") as f:
            csv_line = f"{tweet['Timestamp']},{tweet['Username']},{tweet['Text']},{tweet['Created At']},{tweet['Retweets']},{tweet['Likes']}\n"
            await f.write(csv_line)
        if tweet_count >= max_tweets:
            break

if __name__ == "__main__":
    # Use asyncio.run to properly run the async main function
    asyncio.run(main())
