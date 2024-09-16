import json
import requests
from utils.api_utils import get_topics, get_topic, download_feedbacks
from utils.crawler_utils import construct_pg_payload, convert_topic_meta

PG_HANDLER_ROOT = "http://localhost:8081"
CACHE_DIR = "/home/abstract/demo-deliberate/crawler/.cache"
HEADERS = {'Content-Type': 'application/json'}
DATASOURCE_URL = "https://ec.europa.eu/info/law/better-regulation/doris-api/publications"

def trigger(topics : list[int]) -> None:
    """
    Triggers the process of crawling and processing topics. The function performs the following steps:
    
    1. Iterates through a list of topics.
    2. For each topic:
        a. Downloads the metadata for the topic using the `get_topic` function.
        b. Converts the metadata to a specific format using the `convert_topic_meta` function.
        c. Sends the topic metadata to an API endpoint (`/api/AddTopic`) to add the topic to the PostgreSQL database.
        d. Downloads the feedback associated with the topic using `download_feedbacks`.
        e. Constructs a payload for the feedback and sends it to another API endpoint (`/api/AddFeedback`) to add the feedback to the database.
    3. Handles any HTTP errors that occur during the API requests and logs relevant information.
    
    Args:
        topics (list): A list of topic IDs to process.
        
    Raises:
        requests.exceptions.RequestException: If any HTTP error occurs during the API requests, the error is logged and the process continues.
        
    Returns:
        None
    """
    # https://ec.europa.eu/info/law/better-regulation/doris-api/publications/23751964
    print ("Starting crawling.")
    for topic in topics:
        ## download specific topic:
        print (f"Injecting topic {topic}")
        meta = get_topic(topic_id=topic, datasource_url=DATASOURCE_URL)
        topic_pg = convert_topic_meta(meta=meta)
        print(json.dumps(topic_pg))

        try:
            topic_meta = requests.post(f'{PG_HANDLER_ROOT}/api/AddTopic', json=topic_pg, headers=HEADERS)
            topic_meta.raise_for_status()  # Raise an exception for HTTP errors
            print(f"Status Code: {topic_meta.status_code}")
            response_json = topic_meta.json()
            print(f"Response: {response_json}")
            
            # Extract createdID and assign it to a new variable
            topic_created_id = response_json.get('createdID')
            print(f"Created ID: {topic_created_id}")
            
        except requests.exceptions.RequestException as e:
            print(f"An error occurred: {e}")
            if e.response is not None:
                print(f"Status Code: {e.response.status_code}")
                print(f"Response: {e.response.text}")
        # ## retrieve topic id from pg
        feedback_list = download_feedbacks(topic_id=topic, cache_dir=CACHE_DIR)
        for feedback in feedback_list:
            feedback_pg = construct_pg_payload(payload=feedback, topic_id=topic_created_id)
            print(json.dumps(feedback_pg))
            try:
                feedback_meta = requests.post(f'{PG_HANDLER_ROOT}/api/AddFeedback', json=feedback_pg, headers=HEADERS)
                feedback_meta.raise_for_status()  # Raise an exception for HTTP errors
                print(f"Status Code: {feedback_meta.status_code}")
                response_json = feedback_meta.json()
                print(f"Response: {response_json}")
                # Extract createdID and assign it to a new variable
            except requests.exceptions.RequestException as e:
                print(f"An error occurred: {e}")
                if e.response is not None:
                    print(f"Status Code: {e.response.status_code}")
                    print(f"Response: {e.response.text}")
                    exit(0)

topics = get_topics([])
trigger(topics)
