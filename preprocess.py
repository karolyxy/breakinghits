import pandas as pd
import datetime
from datetime import datetime
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
# This returns a datafram of posts, comments, user ids, post ids, and text (with emojis) #

def insta_open(file):
    df = pd.read_json(file) # Open json output

    # We will keep neccesary columns
    df = df[['__typename',
             'comments',
             'edge_media_preview_like',
             'edge_media_to_caption',
             'edge_media_to_comment',
             'id',
             'owner',
             'tags',
             'taken_at_timestamp',
             'username'
            ]]

    # Rename columns
    cols = {'__typename':'post_type',
            'comments':'comments_text',
            'edge_media_preview_like':'post_likes',
            'edge_media_to_caption':'post_text',
            'edge_media_to_comment':'comments(int)',
            'id':'post_id',
            'owner':'post_user_id',
            'tags':'post_tags',
            'taken_at_timestamp':'post_timestamp',
            'username':'post_username'}
    df = df.rename(columns=cols)

    # Clean post_type
    df['post_type'].replace('Graph', '', regex=True, inplace=True)

    # Clean post_likes
    df['post_likes'] = [c.get('count') for c in df['post_likes']]

    # Clean post_text
    df = df.drop(20).reset_index(drop=True) # This post has no caption text
                                            # and it is the only one.
    df['post_text'] = df['post_text'].apply(lambda x: x['edges'][0]).apply(
                                            lambda x: x['node']).apply(
                                            lambda x: x['text'])

    # Clean comments(int)
    df['comments(int)'] = [c.get('count') for c in df['comments(int)']]

    # Convert post_timestamp to datetime
    post_timestamps = df['post_timestamp']
    post_timestamps = post_timestamps.apply(datetime.fromtimestamp)
    df['post_timestamp'] = post_timestamps

    # Clean post_user_id
    df['post_user_id'] = [c.get('id') for c in df['post_user_id']]

    # Clean post_tags - if a post has no tags, we will impute with 'None'
    tags = df['post_tags'].copy()
    tags[tags.str.len() == 0] = 'None'
    df['post_tags'] = tags

    ### We need to open up all the dicts that represent the comments per post

    # Create a series of just comments
    comments = df['comments_text'].copy()

    # Create an empty data frame that will contain all posts' comments
    comments_df = pd.DataFrame([])

    # Each element of the comments series is itself a dictionary with one key, 'data', whose values are the comments
    # We also create a column that houses original post_id to join on later
    for i in range(len(comments)):
        temp_df = pd.DataFrame(comments.iloc[i]['data'])
        temp_df['count'] = len(temp_df)
        temp_df['post_id'] = df.iloc[i]['post_id']
        comments_df = comments_df.append(temp_df)

    # Rename columns
    comment_cols = ['comment_timestamp', 'comment_id', 'comment_user_id', 'comment_text', 'count', 'post_id']
    comments_df.columns = comment_cols

    # Handling timestamp of comments
    comments_timestamps = comments_df['comment_timestamp']
    comments_timestamps = comments_timestamps.apply(datetime.fromtimestamp)
    comments_df['comment_timestamp'] = comments_timestamps

    # Clean comment_user_id
    comments_df['comment_user_id'] = comments_df['comment_user_id'].apply(lambda x: x['id'])

    # Finally, join both df and comments_df by setting index on df as post_id
    # Join comments_df with df using post_id as 'on' key.

    df = df.set_index('post_id')
    IG_full = comments_df.join(df, on='post_id')
    IG_full.drop('comments_text', axis=1, inplace=True)
    IG_full.drop('count', axis=1, inplace=True)

    return IG_full



#### This function encodes empojis to unicode representation ####

def extract_emojis(str):
    return ''.join(c for c in str if c in emoji.UNICODE_EMOJI)
