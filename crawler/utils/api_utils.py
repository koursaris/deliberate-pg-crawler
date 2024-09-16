import time
import requests
import itertools
import pandas as pd

def retry(func, retries=20):
    def retry_wrapper(*args, **kwargs):
        attempts = 0
        while attempts < retries:
            try:
                return func(*args, **kwargs)
            
            except Exception as e:
                print(e)
                time.sleep(120)
                attempts += 1
    
    return retry_wrapper

@retry
def get_json(url : str) -> dict["str", any] | list[None]:
    """
    Get a Europa page JSON file

    Parameters:
    ------------
    url : (str) -> string containing the exact link

    Returns:
    ------------
    json_data : (dict["str", any]) -> JSON containing
    page metadata OR an empty list if the request fails
    """
    response = requests.get(url)
    response.raise_for_status()
    json_data = response.json()
    return json_data


@retry
def get_topic(topic_id=None, datasource_url=None) -> dict:
    """
    Get all topic IDs from every page of the EUROPA API.

    Returns:
    ----------
    total_ids : (list[str]) -> list of topic IDs
    """

    # parse all europa pages, get topic ids

    try:
        print(f'Crawling topic with id : {topic_id}' )
        topic_url = f'{datasource_url}/{topic_id}'

        page_json = get_json(topic_url)
        # print (page_json)
    
    except TypeError as e:
        print(e)

    return page_json

@retry
def get_topics() -> list[str]:
    """
    Get all topic IDs from every page of the EUROPA API.

    Returns:
    ----------
    total_ids : (list[str]) -> list of topic IDs
    """
    print("Collecting Topics...\n")
    i = 0
    flag = False
    total_ids = []

    # parse all europa pages, get topic ids
    while i<5:
        try:
            print(f"Crawling through page {i}...")
            topic_url = f'https://ec.europa.eu/info/law/better-regulation/doris-api/publications?page={i}&size=100&sort=modifiedDate,desc'

            page_json = get_json(topic_url)
            page_ids = [item["id"] for item in page_json["content"]]
            total_ids.append(page_ids)

            # flag = True # change it when the api is fixed
            flag = page_json["last"]

            i+=1

        except TypeError as e:
            print(e)

    total_ids = list(itertools.chain(*total_ids))
    return total_ids

def download_feedbacks(topic_id=None, cache_dir=None) -> dict:
    """
    Download feedbacks pertaining to certain topic
    and store them in a JSON format.

    Parameters:
    ------------
    topic_id : (int) -> ID number of the relevant topic
    """
    # get all feedbacks for the topic id
    i = 0

    
    flag = False
    total_feeds = []
    
    while not flag:
        try:
            page_url = f"https://ec.europa.eu/info/law/better-regulation/doris-api/feedback?publicationId={topic_id}&page={i}"
            page_json = get_json(page_url)
            page_feeds = [item["feedback"] if item["language"] == "EN" else item["englishfeedback"] for item in page_json["content"]]
            page_feeds = [item for item in page_feeds if item is not None]
            total_feeds.append(page_feeds)

            try:
                flag = page_json["last"]
            
            except KeyError:
                flag = True
        
        except Exception:
            flag = True

        i += 1
        
    total_feeds_flat = list(itertools.chain(*total_feeds))
    payload =None
    if len(total_feeds_flat) >= 1:
        df = pd.DataFrame(
            {
                "index" : [i for i in range(len(total_feeds_flat))],
                "topic_id" : [topic_id] * len(total_feeds_flat), 
                "text" : total_feeds_flat
            }
            )
        df.to_json(f"{cache_dir}/{topic_id}.json", orient="records")
        payload = df.to_dict(orient="records")
    
    print(f"Dataset {topic_id} Length: {len(total_feeds_flat)}")
    return payload
    