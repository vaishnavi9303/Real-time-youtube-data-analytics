from kafka import KafkaProducer
import requests
import json
import time
from datetime import datetime


# Kafka broker configuration
bootstrap_servers = 'localhost:9092'

# YouTube Data API configuration
API_KEY = 'AIzaSyCPPO5lFzvLTa1Ht1NmD7ZQNvmSWfxxybI'
BASE_URL = 'https://www.googleapis.com/youtube/v3/'

# Create Kafka Producer instance
producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

def fetch_youtube_data(api_key):
    """
    Fetches data from the YouTube Data API.

    Args:
        api_key (str): Your YouTube Data API key.

    Returns:
        dict: YouTube data.
    """
    url = f"{BASE_URL}videos?part=snippet%2Cstatistics&chart=mostPopular&maxResults=50&key={api_key}"
    response = requests.get(url)
    return response.json()
    
def fetch_popular_searches(api_key):
    """
    Fetches popular searches from the YouTube Data API.

    Args:
        api_key (str): Your YouTube Data API key.

    Returns:
        dict: Popular searches data.
    """
    url = f"{BASE_URL}search?part=snippet&chart=mostPopular&maxResults=50&type=video&key={api_key}"
    response = requests.get(url)
    return response.json()
    

def fetch_video_comments(api_key, video_id, max_results=10):
    """
    Fetches the latest comments for a YouTube video using the YouTube Data API v3.

    Args:
        api_key (str): Your YouTube Data API key.
        video_id (str): The ID of the YouTube video.
        max_results (int): Maximum number of comments to fetch. Default is 10.

    Returns:
        list: List of dictionaries containing information about the latest comments for the video.
    """

    # Construct the URL for the API request
    url = f"{BASE_URL}commentThreads?part=snippet&videoId={video_id}&maxResults=50&order=time&key={api_key}"

    # Make the HTTP request to the YouTube API
    response = requests.get(url)

    # Check if the response was successful
    if response.status_code != 200:
        #print(f"Failed to fetch comments: {response.status_code} {response.text}")
        return []

    # Parse the JSON response
    comments_data = response.json()
    comments = []

    # Extract the relevant data from the response
    for comment in comments_data.get('items', []):
        snippet = comment['snippet']['topLevelComment']['snippet']
        comment_info = {
            'comment_id': comment['id'],
            'comment': snippet['textDisplay'],  # Directly use the text display
            'replies_count': comment['snippet']['totalReplyCount'],
            'like_count': snippet.get('likeCount', 0),
            'channel_title': snippet['authorDisplayName'],
            'published_at': snippet['publishedAt']
        }
        comments.append(comment_info)

    return comments
    
def format_search_data(search):
    """
    Formats YouTube search data.

    Args:
        search (dict): YouTube search data.

    Returns:
        dict: Formatted search data.
    """
    search_id = search['id']['videoId']
    snippet = search['snippet']
    
    formatted_data = {
        'search_id': search_id,
        'title': snippet['title'],
        'channel_title': snippet['channelTitle'],
        'published_at': snippet['publishedAt']
    }
    return formatted_data



def format_video_data(video):
    """
    Formats YouTube video data.

    Args:
        video (dict): YouTube video data.

    Returns:
        dict: Formatted video data.
    """
    video_id = video['id']
    snippet = video['snippet']
    statistics = video['statistics']
    
    
    formatted_data = {
        #'timestamp': datetime.now().isoformat(),  # Current timestamp in ISO 8601 format
        'title': snippet['title'],
        'channel_title': snippet['channelTitle'],
        'like_count': statistics.get('likeCount', 0),
        'view_count': statistics.get('viewCount', 0),
        'dislike_count': statistics.get('dislikeCount', 0),
        'comment_count': statistics.get('commentCount', 0)
    }
    return formatted_data

def produce_youtube_data():
    """
    Produces YouTube data to Kafka topics.
    """
    while True:
        # Fetch popular searches
        searches_data = fetch_popular_searches(API_KEY)
        
        # Check if 'items' key exists in the response
        if 'items' in searches_data:
            # Produce search data to Kafka topic
            for search in searches_data['items']:
                search_data = format_search_data(search)
                #print(search_data)
                producer.send('youtube_searches', value=search_data)
        else:
            print("No 'items' found in the API response. Skipping this iteration.")

        # Fetch video data
        video_data = fetch_youtube_data(API_KEY)
        
        # Check if 'items' key exists in the response
        if 'items' in video_data:
            # Produce video data to Kafka topic
            for video in video_data['items']:
                video_data_formatted = format_video_data(video)
                #print(video_data_formatted)
                producer.send('youtube_videos', value=video_data_formatted)
                
                # Fetch and produce comments data
                comment_data = fetch_video_comments(API_KEY, video['id'])
                #print(comment_data)
                producer.send('youtube_comments', value=comment_data)
        else:
            print("No 'items' found in the API response. Skipping this iteration.")

        # Wait for 60 seconds before fetching data again
        time.sleep(60)


 

if __name__ == '__main__':
    produce_youtube_data()
    
    #https://www.googleapis.com/youtube/v3/videos?part=snippet%2Cstatistics&chart=mostPopular&maxResults=5&key=AIzaSyAE4O1EIOWgj1axIO0dgYthK_9Ji-Oo7dk
