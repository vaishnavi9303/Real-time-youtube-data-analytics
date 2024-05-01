import mysql.connector
from kafka import KafkaConsumer
import json
from datetime import datetime
import time
import sys

# Kafka broker configuration
bootstrap_servers = 'localhost:9092'

# MySQL database configuration
mysql_config = {
    'host': 'localhost',
    'user': 'root',
    'password': 'tacoBell',
    'database': 'DBT_638_664_696_697'
}

# Initialize MySQL connection
conn = mysql.connector.connect(**mysql_config)
cursor = conn.cursor()

def create_tables():
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS youtube_videos (
        title TEXT, 
        channel_title TEXT, 
        like_count INTEGER, 
        view_count INTEGER, 
        dislike_count INTEGER, 
        comment_count INTEGER)
    ''')
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS youtube_comments (
        comment_id VARCHAR(255), 
        comment TEXT, 
        replies_count INTEGER,
        like_count INTEGER, 
        channel_title TEXT, 
        published_at DATETIME)
    ''')
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS youtube_searches (
        search_id VARCHAR(255), 
        title TEXT, 
        channel_title TEXT, 
        published_at DATETIME)
    ''')
    conn.commit()

def insert_video_data(video_data):
    cursor.execute('''
    INSERT INTO youtube_videos (title, channel_title, like_count, view_count, dislike_count, comment_count)
    VALUES (%s, %s, %s, %s, %s, %s)
    ''', (video_data['title'], video_data['channel_title'], video_data['like_count'],
          video_data['view_count'], video_data['dislike_count'], video_data.get('comment_count', 0)))
    conn.commit()

def insert_comments_data(comments_data):
    for comment_data in comments_data:
        published_at = datetime.strptime(comment_data['published_at'], '%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m-%d %H:%M:%S')
        cursor.execute('''
        INSERT INTO youtube_comments (comment_id, comment, replies_count, like_count, channel_title, published_at)
        VALUES (%s, %s, %s, %s, %s, %s)
        ''', (comment_data['comment_id'], comment_data['comment'], comment_data['replies_count'],
              comment_data['like_count'], comment_data['channel_title'], published_at))
        conn.commit()

def insert_search_data(search_data):
    published_at = datetime.strptime(search_data['published_at'], '%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m-%d %H:%M:%S')
    cursor.execute('''
    INSERT INTO youtube_searches (search_id, title, channel_title, published_at)
    VALUES (%s, %s, %s, %s)
    ''', (search_data['search_id'], search_data['title'], search_data['channel_title'], published_at))
    conn.commit()
    
def find_shorts_searches():
    """
    Find searches with #shorts from MySQL database.
    """
    query = """
    SELECT title,channel_title FROM youtube_searches
    WHERE title LIKE '%#shorts%'
    """
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    except Error as e:
        print("Error while fetching #shorts searches from MySQL", e)

def periodic_report():
    cursor.execute("SELECT * FROM youtube_videos ORDER BY like_count DESC LIMIT 1")
    most_liked_video = cursor.fetchone()
    cursor.execute("SELECT COUNT(*) FROM youtube_comments WHERE comment LIKE '%nice%'")
    nice_comment_count = cursor.fetchone()[0]
    shorts_searches = find_shorts_searches()

    with open('sql_results.txt', 'a') as f:
        f.write(f"Most liked video: {most_liked_video}\n")
        f.write(f"Number of nice comments: {nice_comment_count}\n")
        f.write("Shorts searches:\n")
        for search in shorts_searches:
            f.write(f"Title: {search[0]}, Channel: {search[1]}\n")
    print("Report updated.")

def consume_data():
    consumer = KafkaConsumer(
        'youtube_videos', 'youtube_comments', 'youtube_searches',
        group_id='my_consumer_group',
        bootstrap_servers=bootstrap_servers,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    start_time = time.time()
    for message in consumer:
        if message.topic == 'youtube_videos':
            insert_video_data(message.value)
        elif message.topic == 'youtube_comments':
            insert_comments_data(message.value)
        elif message.topic == 'youtube_searches':
            insert_search_data(message.value)
        # Check if 1 minute has passed
        if time.time() - start_time > 60:
            periodic_report()
            start_time = time.time()  # Reset timer

if __name__ == '__main__':
    create_tables()
    while True:
        consume_data()

