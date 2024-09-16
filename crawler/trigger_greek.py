import json
import pandas as pd
import requests
from utils.crawler_utils import construct_pg_payload, convert_topic_meta

PG_HANDLER_ROOT = "http://localhost:8081"
ORIGINAL_DATE_FROMAT = '%Y/%m/%d %H:%M:%S'
PG_DATE_FROMAT = '%Y-%m-%dT%H:%M:%SZ'
DATASOURCE_URL = "https://ec.europa.eu/info/law/better-regulation/doris-api/publications"
HEADERS = {'Content-Type': 'application/json'}

def trigger(
        csv_path:str=None,
        json_path:str=None
        ) -> None:
    """
    Processes flight data from a CSV file, uploads topic metadata from a JSON file, 
    and posts feedback related to the topic to a specified server.

    Parameters:
    ----------
    csv_path : str
        The path to the CSV file containing feedback data.
    
    json_path : str
        The path to the JSON file containing topic metadata.
    
    Returns:
    -------
    None
        Prints the response statuses and IDs of created topics and feedback.
        Handles any HTTP request exceptions and logs error details.

    Workflow:
    --------
    1. Reads the CSV file to get feedback data.
    2. Loads the topic metadata from the specified JSON file.
    3. Sends the topic metadata to the server and retrieves the created topic ID.
    4. Iterates over feedback from the CSV, posting each one to the feedback API.
    5. Logs statuses and handles errors at each step.
    
    Raises:
    -------
    requests.exceptions.RequestException
        Raised if there's an issue with the POST requests.
    """
    
    # Read the CSV file
    df = pd.read_csv(csv_path)

    # Load the topic metadata from JSON
    with open(json_path, "r", encoding="utf-8") as topic_f:
        meta = json.load(topic_f)

    try:
        # Convert and send the topic metadata to the server
        topic_pg = convert_topic_meta(meta=meta)
        with open("data/sample.json", "w", encoding="utf-8") as save_f:
            json.dump(topic_pg, save_f)
        print(json.dumps(topic_pg))
        
        # Send a POST request to add the topic
        topic_meta = requests.post(f'{PG_HANDLER_ROOT}/api/AddTopic', json=topic_pg, headers=HEADERS)
        topic_meta.raise_for_status()  # Raise an exception for HTTP errors
        print(f"Status Code: {topic_meta.status_code}")
        
        response_json = topic_meta.json()
        print(f"Response: {response_json}")
        
        # Extract createdID for the topic
        topic_created_id = response_json.get('createdID')
        print(f"Created ID: {topic_created_id}")
        
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")
        if e.response is not None:
            print(f"Status Code: {e.response.status_code}")
            print(f"Response: {e.response.text}")
            exit(0)  # Exit if there's an error in processing the topic

    # Process feedback from the CSV
    feedback_list = df["text"].to_list()
    for feedback in feedback_list:
        feedback_pg = construct_pg_payload(payload=feedback, topic_id=topic_created_id)
        print(json.dumps(feedback_pg))
        try:
            # Send a POST request to add feedback
            feedback_meta = requests.post(f'{PG_HANDLER_ROOT}/api/AddFeedback', json=feedback_pg, headers=HEADERS)
            feedback_meta.raise_for_status()  # Raise an exception for HTTP errors
            print(f"Status Code: {feedback_meta.status_code}")
            
            response_json = feedback_meta.json()
            print(f"Response: {response_json}")
            
            # Extract createdID for feedback
            feedback_created_id = response_json.get('createdID')
            print(f"Successfully produced feedback: {feedback_created_id} on topic: {topic_created_id}")
        except requests.exceptions.RequestException as e:
            print(f"An error occurred: {e}")
            if e.response is not None:
                print(f"Status Code: {e.response.status_code}")
                print(f"Response: {e.response.text}")
                exit(0)  # Exit if there's an error in processing feedback


if __name__ == "__main__":
    csv_path = ""
    json_path = ""

    trigger(
        csv_path,
        json_path
    )



