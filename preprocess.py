import pandas as pd
import datetime
import emoji

#### This function will open the json output from Twitter scraping.####
# This returns ONLY a dataframe of all tweets and time columns as `TimeStamp` objects

def tweet_open(file):
    df = pd.read_json(file)
    try:
        df['time'] = pd.to_datetime(df['time'])
    except:
        pass
    try:
        df['timestamp'] = pd.to_datetime(df['timestamp'])
    except:
        pass
    return df



#### This function will open the json output from IG scraping. ####
# This returns ONLY a dataframe of comments on one post. Work needs to be done
# to preprocess other columns

def insta_comment(file):
    df = pd.DataFrame(dict(pd.read_json(file)['comments'][0])['data'])

    # Create a list of usernames
    
    return pd.DataFrame(dict(pd.read_json(file)['comments'][0])['data'])



#### This function encodes empojis to unicode representation ####

def extract_emojis(str):
    return ''.join(c for c in str if c in emoji.UNICODE_EMOJI)
