import os
import re
import pandas as pd

from googleapiclient.discovery import build

def get_channel_info(youtube, id_channel):
    
    """
    Function for getting channel information: id_channel, channel_name, 
    subscriber_count, total_video, total_views
    """
    
    request = youtube.channels().list(
        part='snippet, statistics',
        id=id_channel
    )
    response_channel = request.execute()
    
    id_channel = response_channel['items'][0]['id']
    channel_name = response_channel['items'][0]['snippet']['title']
    subscriber_count = response_channel['items'][0]['statistics']['subscriberCount']
    total_video = response_channel['items'][0]['statistics']['videoCount']
    total_views = response_channel['items'][0]['statistics']['viewCount']
    
    channel_info = [id_channel, channel_name, subscriber_count, total_video, total_views]
    return channel_info

def get_video_list(youtube, id_channel, withInfo=False):
    
    """
    Function for getting list of videos and statistics of video in channel
    """
    
    request = youtube.search().list(
        part='snippet',
        channelId=id_channel
    )
    response_list = request.execute()
    
    result = []
    
    for item in response_list['items']:
        id_video = item['id']['videoId']
        title_video = item['snippet']['title']
        published_at = item['snippet']['publishedAt']
        
        if withInfo:
            request = youtube.videos().list(
                part='statistics',
                id=id_video
            )
            response_video = request.execute()
            
            view_count = response_video['items'][0]['statistics']['viewCount']
            like_count = response_video['items'][0]['statistics']['likeCount']
            comment_count = response_video['items'][0]['statistics']['commentCount']
            
            buff = [id_video, title_video, published_at, view_count, like_count, comment_count]
        else:
            buff = [id_video, title_video, published_at]
            
        result.append(buff)
    
    return result

def get_video_comments(youtube, id_video, withReply=False):
    
    """
    Function for getting list of comments in video. There is option
    for scrap with reply comment or not.
    """
    
    request = youtube.commentThreads().list(
        part='snippet, replies',
        videoId=id_video,
        maxResults=100
    )
    response_comments = request.execute()
    
    comment_list = []   
    
    while True:
        for item in response_comments['items']:
            id_comment = item['snippet']['topLevelComment']['id']
            display_name = item['snippet']['topLevelComment']['snippet']['authorDisplayName']
            text_comment = re.sub('\s+',' ', item['snippet']['topLevelComment']['snippet']['textOriginal'])
            published_at = item['snippet']['topLevelComment']['snippet']['publishedAt']
            like_count = item['snippet']['topLevelComment']['snippet']['likeCount']
            total_reply = item['snippet']['totalReplyCount']
            
            if withReply:
                reply_list = []
                
                if (total_reply > 0) and ('replies' in item):   
                    for reply in item['replies']['comments']:
                        parent_id = reply['snippet']['parentId']
                        name_reply = reply['snippet']['textOriginal']
                        text_reply = re.sub('\s+',' ',reply['snippet']['authorDisplayName'])
                        
                        reply_list.append([parent_id, name_reply, text_reply])
                        
                buff = [id_comment, display_name, text_comment, published_at, like_count, total_reply, reply_list]
            
            else:
                buff = [id_comment, display_name, text_comment, published_at, like_count, total_reply]
            
            comment_list.append(buff)
        
        if 'nextPageToken' in response_comments:
            request = youtube.commentThreads().list(
                part='snippet, replies',
                videoId=id_video,
                maxResults=100,
                pageToken=response_comments['nextPageToken']
            )

            response_comments = request.execute()

        else:
            break
    
    return comment_list

def merge_list(list_a, list_b):
    
    """
    Merge two list
    """
    
    result = []
    
    for i in range(len(list_b)):
        result.append(list_a + list_b[i])
        
    return result

if __name__ == '__main__':
    
    API_SERVICE_NAME = 'youtube'
    API_VERSION = 'v3'
    API_KEY_DEV = 'YOUR API HERE'
    
    ID_CHANNEL = 'UCEHjExuQgRMCFYXpBtcEO2Q' # Example Maudy Ayunda
    
    OUTPUT_PATH = '..\\RESULT\\result.csv'
    
    COLUMN_NAMES = ['id_channel', 'channel_name', 'subscriber', 'total_videos', 'total_views',
                    'id_video', 'title', 'published_at', 'view_count', 'like_count', 'comment_count',
                    'id_comment', 'display_name', 'text_comment', 'published_at', 'like_count', 'total_reply', 'replies']

    youtube = build(API_SERVICE_NAME, API_VERSION, developerKey=API_KEY_DEV)
    
    channel_info = get_channel_info(youtube=youtube, id_channel=ID_CHANNEL)
    list_video = get_video_list(youtube=youtube, id_channel=ID_CHANNEL, withInfo=True)
    
    list_video = merge_list(channel_info, list_video)
    
    for i in range(len(list_video)):
        
        print(f'{list_video[i][1]} : {list_video[i][6]} ({list_video[i][10]} Comments)')

        list_comment = get_video_comments(youtube=youtube, id_video=list_video[i][5], withReply=True)
        buff = merge_list(list_video[i], list_comment)
        result_df = pd.DataFrame(buff, columns=COLUMN_NAMES)
        
        if not os.path.isfile(OUTPUT_PATH):
            result_df.to_csv(OUTPUT_PATH, sep='|', mode='a')
        else:
            result_df.to_csv(OUTPUT_PATH, header=False, sep='|', mode='a')