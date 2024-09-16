import random
from datetime import datetime, timedelta

def generate_random_date_2024() -> str:
    """
    Generates a random date and time within the year 2024, including milliseconds precision.
    
    The function calculates the time range between January 1, 2024, and December 31, 2024, 
    then selects a random date and time within this range. The result is returned as a string 
    formatted in ISO 8601 format with milliseconds (YYYY-MM-DDTHH:MM:SS.sssZ).

    Args:
        None

    Returns:
        str: A random date and time in ISO 8601 format (e.g., '2024-05-15T12:34:56.789Z').
    """
    # Define the start and end dates for the year 2024
    start_date = datetime(2024, 1, 1)
    end_date = datetime(2024, 12, 31, 23, 59, 59, 999999)
    
    # Calculate the difference between the start and end dates
    time_between_dates = end_date - start_date
    days_between_dates = time_between_dates.days
    
    # Generate a random number of days and seconds to add to the start date
    random_number_of_days = random.randrange(days_between_dates)
    random_number_of_seconds = random.randrange(86400)  # Number of seconds in a day
    
    # Generate a random date
    random_date = start_date + timedelta(days=random_number_of_days, seconds=random_number_of_seconds)
    
    # Format the random date in the desired format with milliseconds
    random_date_str = random_date.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
    
    return random_date_str

def construct_pg_payload(payload : dict=None, topic_id: int=None):
    """
    Constructs a payload for sending feedback data to a PostgreSQL (PG) database.
    
    The function takes a raw feedback payload and a topic ID, then generates additional fields 
    such as a random creation date, creator ID, and other required fields for the feedback submission.
    The response is structured to include these values and can be used in subsequent API calls.

    Args:
        payload (dict, optional): A dictionary containing the feedback data, expected to have at least a `text` field.
        topic_id (int, optional): The unique identifier for the topic to which the feedback is related.

    Returns:
        dict: A dictionary representing the structured payload, including:
            - `createdAt`: A randomly generated date within 2024.
            - `creatorId`: A random integer between 1 and 9.
            - `feedbackText`: The text of the feedback.
            - `isComment`: A boolean indicating whether the feedback is a comment (set to False).
            - `topicId`: The ID of the topic associated with the feedback.
            - `stanceId`: A fixed integer representing a stance (set to 4).
    """
    response = {}
    response["createdAt"] = generate_random_date_2024()
    response["creatorId"] = random.randint(1, 9)
    response["feedbackText"] = payload["text"]
    response["isComment"] = False
    response["topicId"] = topic_id
    response["stanceId"] = 4
    return response

def convert_topic_meta(meta : dict=None):
    """
    Converts topic metadata into a structured payload for database submission.
    
    The function processes the provided metadata by parsing and formatting dates, 
    generating random creator IDs, and structuring the fields needed for submission to a PostgreSQL (PG) database.
    
    Args:
        meta (dict, optional): A dictionary containing metadata about the topic, expected to have at least:
            - `createdDate`: The original creation date in a specific format.
            - `modifiedDate`: The last modified date in a specific format.
            - `shortTitle`: A short title for the topic.
            - `title`: The full title of the topic.

    Returns:
        dict: A dictionary representing the structured payload, including:
            - `title`: The short title of the topic.
            - `topicText`: The full title of the topic.
            - `summary`: An empty summary field (can be filled later).
            - `creatorId`: A randomly generated creator ID between 2 and 9.
            - `createdAt`: The formatted creation date of the topic.
            - `url`: The data source URL for the topic.
            - `lastUpdated`: The formatted last updated date of the topic.
            - `statusId`: A fixed status identifier (set to 1).
    """
    ORIGINAL_DATE_FORMAT = '%Y/%m/%d %H:%M:%S'
    PG_DATE_FORMAT = '%Y-%m-%dT%H:%M:%SZ'
    DATASOURCE_URL = "https://ec.europa.eu/info/law/better-regulation/doris-api/publications"
    
    date_created = datetime.strptime(meta['createdDate'], ORIGINAL_DATE_FORMAT)
    date_created_formatted = date_created.strftime(PG_DATE_FORMAT)
    last_updated_date = datetime.strptime(meta['modifiedDate'], ORIGINAL_DATE_FORMAT)
    last_updated_formatted = last_updated_date.strftime(PG_DATE_FORMAT)

    payload = {}
    payload["title"] = meta["shortTitle"]
    payload["topicText"] = meta["title"]
    payload["summary"] = ""
    payload["creatorId"] = random.randint(2, 9)
    payload["createdAt"] = date_created_formatted
    payload["url"] = f'{DATASOURCE_URL}'
    payload["lastUpdated"] = last_updated_formatted
    payload["statusId"] = 1
    return payload