import googleapiclient.discovery
import pymongo
import psycopg2
import streamlit as st
import pandas as pd
import re

# Api key connection
api_service_name = "youtube"
api_version = "v3"
api_key = "please_update_your_apikey"
youtube = googleapiclient.discovery.build(
    api_service_name, api_version, developerKey=api_key)

# MongoDB connection
client = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = client["Your_db_name"]
mycol = mydb["your_Channels_info"]

# PostgreSQL connection
postgres_conn = psycopg2.connect(
    database="your_db_name",
    user="user",
    password="password",
    host="localhost",
    port="5432"
)
postgres_cursor = postgres_conn.cursor()

#Channel Id entry validation
def validate_input(text):
    # Check if input is not empty
    if not text:
        st.warning("Input cannot be empty.")
        return False
    if len(text) != 24:  # Check for 24 characters
        st.warning("please enter valid channel ID")
        return False
        # Check if input matches the pattern: 'UC' followed by alphanumeric, hyphens, or underscores
    if not re.match(r'^UC[\w\-]+$', text):
        st.warning("Input should start with 'UC' and contain only alphanumeric, hyphens, or underscores.")
        return False
        # If all conditions pass, input is valid
    return True

def main():
    channel_id_sql = ''#for sql insertion

    with st.sidebar:# streamlit side bar
        st.title(":red[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
        st.markdown("#### :blue[Domain] : Social Media")
        st.markdown("#### :blue[Technologies used] : Python, MongoDB, API Integration, PostgreSQL, Streamlit")
        st.markdown( "#### :blue[Overview] : Retrieving the Youtube channels data from the Google API, storing it in a MongoDB as data lake, migrating and transforming data into a SQL database, then querying the data and displaying it in the Streamlit app.")

    channel_id = st.text_input('Enter the Channel Id:')
    mn_button_clicked = st.button('Collect and Store')
    if mn_button_clicked:
        if validate_input(channel_id):
            ch_ids=[]
            for ch_data in mycol.find({},{"Channel_information":1}):
                ch_ids.append(ch_data["Channel_information"]["Channel_Id"])
            if channel_id in ch_ids: # checking if the entered id is already stored in mongodb
                st.success("channel already exists in mongodb")
            elif len(ch_ids)>=10: # also a max of 10 channels details only allowed
                st.success("only 10 channel ids info are permitted")
            else:#if all validations are satisfied
                # getting or scrapping data from youtube for the entered channel id
                channel_dict = channel_details(channel_id)
                if channel_dict is not None and bool(channel_dict):# Dictionary is not None and is not empty
                    video_ids = video_details(channel_id)
                    video_dict_list = video_dict_details(video_ids)
                    comments_dict_list = comments_details(video_ids)
                    # channel dictionary creation
                    final_channel_dict = {"Channel_information": channel_dict,
                                          "Video_details": video_dict_list,
                                          "Comments_details": comments_dict_list}
                    # insert into mongo db
                    mn_insert_success = mongodb_insert(final_channel_dict)
                    st.success(mn_insert_success)
                else:
                    st.success('invalid channel id')

    # migrating data to sql
    # Create a dropdown with channel names options from mongodb
    ch_names = []
    for each_doc in mycol.find({}, {"Channel_information": 1}):
        ch_names.append(each_doc["Channel_information"]['Channel_Name'])
    selected_option = st.selectbox('Channels for sql migration', ch_names, index=None, placeholder='select a channel :')
    for each_doc in mycol.find({}, {"Channel_information": 1}):
        if selected_option == each_doc["Channel_information"]['Channel_Name']:
            channel_id_sql = each_doc["Channel_information"]["Channel_Id"]
    sql_button_clicked = st.button('Migrate to SQL')
    if sql_button_clicked:
        #insert into postgres tables
        msg1, msg2 =ch_sql_insert(channel_id_sql)
        msg3, msg4 =vid_sql_insert(channel_id_sql)
        msg5, msg6 =com_sql_insert(channel_id_sql)
        st.success(msg2)
        st.success(msg4)
        st.success(msg6)

    # sql queries dropdown
    queries_list=('1. What are the names of all the videos and their corresponding channels?',
    '2. Which channels have the most number of videos, and how many videos do they have?',
    '3. What are the top 10 most viewed videos and their respective channels?',
    '4. How many comments were made on each video, and what are their corresponding video names?',
    '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
    '6. What is the total number of likes for each video, and what are their corresponding video names?',
    '7. What is the total number of views for each channel, and what are their corresponding channel names?',
    '8. What are the names of all the channels that have published videos in the year 2022?',
    '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
    '10 Which videos have the highest number of comments, and what are their corresponding channel names?',
    )

    sql_queries=st.selectbox('SQL queries',queries_list,index=None,placeholder="choose your query:",)
    if sql_queries=='1. What are the names of all the videos and their corresponding channels?':
        query1="select video_name,channel_name from videos"
        postgres_cursor.execute(query1)
        postgres_conn.commit()
        t1=postgres_cursor.fetchall()
        df1=pd.DataFrame(t1,columns=['Videos_title','Channels'])
        st.write(df1)

    elif sql_queries == '2. Which channels have the most number of videos, and how many videos do they have?':
        query2 = "select channel_name,total_videos from channels order by total_videos desc"
        postgres_cursor.execute(query2)
        postgres_conn.commit()
        t2 = postgres_cursor.fetchall()
        df2 = pd.DataFrame(t2, columns=['Channel_name', 'video_count'])
        st.write(df2)

    elif sql_queries == '3. What are the top 10 most viewed videos and their respective channels?':
        query3 = '''select view_count,video_name,channel_name from videos where view_count is 
                 not null order by view_count desc limit 10'''
        postgres_cursor.execute(query3)
        postgres_conn.commit()
        t3 = postgres_cursor.fetchall()
        df3 = pd.DataFrame(t3, columns=['views', 'Video_name','Channel_name'])
        st.write(df3)

    elif sql_queries == '4. How many comments were made on each video, and what are their corresponding video names?':
        query4 = '''select comment_count,video_name from videos'''
        postgres_cursor.execute(query4)
        postgres_conn.commit()
        t4 = postgres_cursor.fetchall()
        df4 = pd.DataFrame(t4, columns=['comment_count', 'Video_name'])
        st.write(df4)

    elif sql_queries == '5. Which videos have the highest number of likes, and what are their corresponding channel names?':
        query5 = '''select video_name,channel_name,like_count from videos where
                    like_count is not null order by like_count desc'''
        postgres_cursor.execute(query5)
        postgres_conn.commit()
        t5 = postgres_cursor.fetchall()
        df5 = pd.DataFrame(t5, columns=['Video_name','channel_name','likes'])
        st.write(df5)

    elif sql_queries == '6. What is the total number of likes for each video, and what are their corresponding video names?':
        query6 = '''select video_name,like_count from videos where
                       like_count is not null'''
        postgres_cursor.execute(query6)
        postgres_conn.commit()
        t6 = postgres_cursor.fetchall()
        df6 = pd.DataFrame(t6, columns=['Video_name','likes'])
        st.write(df6)

    elif sql_queries == '7. What is the total number of views for each channel, and what are their corresponding channel names?':
        query7 = '''select channel_name,views from channels'''
        postgres_cursor.execute(query7)
        postgres_conn.commit()
        t7 = postgres_cursor.fetchall()
        df7 = pd.DataFrame(t7, columns=['channel_name','views'])
        st.write(df7)

    elif sql_queries == '8. What are the names of all the channels that have published videos in the year 2022?':
        query8 = '''select video_name,channel_name,published_at
            from videos where extract(year from published_at)=2022'''
        postgres_cursor.execute(query8)
        postgres_conn.commit()
        t8 = postgres_cursor.fetchall()
        df8 = pd.DataFrame(t8, columns=['video_name','channel_name','published_time'])
        st.write(df8)

    elif sql_queries == '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':

        query9 ='''select channel_name,avg(extract(epoch from duration))/60 from videos group by channel_name'''
        postgres_cursor.execute(query9)
        postgres_conn.commit()
        t9 = postgres_cursor.fetchall()
        df9 = pd.DataFrame(t9, columns=['channel_name', 'avg_duration(mins)'])
        st.write(df9)

    elif sql_queries == '10 Which videos have the highest number of comments, and what are their corresponding channel names?':
        query10 = '''select video_name,comment_count,channel_name
               from videos where comment_count is not null order by comment_count desc'''
        postgres_cursor.execute(query10)
        postgres_conn.commit()
        t10 = postgres_cursor.fetchall()
        df10 = pd.DataFrame(t10, columns=['video_name', 'comment_count', 'channel_name'])
        st.write(df10)

#inserting data into MongoDB and creating datalake
def mongodb_insert(final_channel_dict):
    mycol.insert_one(final_channel_dict)
    mn_insert_success='inserted into mongo db successfully'
    return mn_insert_success

#getting all channel details for the particular channel id
def channel_details(channel_id):
    channel_req = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_id
    )
    channel_res = channel_req.execute()
    items = channel_res.get('items', [])

    if items:
        channel_dict = {'Channel_Id': channel_res["items"][0]['id'],
                        'Channel_Name': channel_res["items"][0]['snippet']['title'],
                        'Subscription_Count': channel_res["items"][0]['statistics']['subscriberCount'],
                        'Total_videos':channel_res["items"][0]['statistics']['videoCount'],
                        'Channel_Views': channel_res["items"][0]['statistics']['viewCount'],
                        'Channel_Description': channel_res["items"][0]['snippet']['description'],
                        'Playlist_id': channel_res["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]
                        }
        return channel_dict

#getting all video ids for the particular channel by passing the playlist id
def video_details(channel_id):
    channel_req = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_id
    )
    channel_res = channel_req.execute()
    ch_playlist_id = channel_res["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]
    next_page_token = None
    while True:
        video_req = youtube.playlistItems().list(
            part='snippet',
            playlistId=ch_playlist_id,
            maxResults=50,
            pageToken=next_page_token
        )
        video_res = video_req.execute()
        video_ids = []
        for i in range(len(video_res['items'])):
            video_ids.append(video_res['items'][i]['snippet']['resourceId']['videoId'])

        next_page_token = video_res.get('nextPageToken')
        if next_page_token is None:
            break
    return video_ids

#getting all video details for all the video ids
def video_dict_details(video_ids):
    video_dict_list = []

    for each_video_id in video_ids:
        video_response = youtube.videos().list(
            part='snippet,statistics,contentDetails',
            id=each_video_id
        ).execute()
        for each_item in video_response['items']:
            video_dict = {'Video_Id': each_item['id'],
                          'Channel_Id': each_item['snippet']['channelId'],
                          'Channel_Name': each_item['snippet']['channelTitle'],
                          'Video_Name': each_item['snippet']['title'],
                          'Video_Description': each_item['snippet']['description'],
                          "Tags": each_item['snippet'].get('tags'),
                          "Published_At": each_item['snippet']['publishedAt'],
                          "View_Count": each_item['statistics'].get('viewCount'),
                          "Like_Count": each_item['statistics'].get('likeCount'),
                          "Favorite_Count": each_item['statistics']['favoriteCount'],
                          "Comment_Count": each_item['statistics'].get('commentCount'),
                          "Duration": each_item['contentDetails']['duration'],
                          "Thumbnail": each_item['snippet']['thumbnails']['default']['url'],
                          "Caption_Status": each_item['contentDetails']['caption']
                          }
            video_dict_list.append(video_dict)
    return video_dict_list


def comments_details(video_ids):
    comment_list = []
    try:
        for each_video_id in video_ids:
            comments_response = youtube.commentThreads().list(
                part="snippet",
                videoId=each_video_id,
                maxResults=50,
                # pageToken=nextPageToken
            ).execute()

            for each_item in comments_response['items']:
                comment_dict = {'Comment_Id': each_item["snippet"]["topLevelComment"]['id'],
                                'video_id': each_item["snippet"]["topLevelComment"]['snippet']['videoId'],
                                'Comment_text': each_item["snippet"]["topLevelComment"]["snippet"]["textDisplay"],
                                'Comment_Author': each_item["snippet"]["topLevelComment"]["snippet"][
                                    'authorDisplayName'],
                                'Comment_Published_at': each_item["snippet"]["topLevelComment"]["snippet"][
                                    'publishedAt'],
                                }
                comment_list.append(comment_dict)
        return comment_list
    except:
        pass
        return comment_list

#Channel Table creation if not exists and insertion into postgresSQL
def ch_sql_insert(channel_id):
    msg_ch_tab,msg_ch='',''
    try:
        create_query = '''create table if not exists
        channels(channel_id varchar(80) primary key,
        Channel_name varchar(100),
         subscribers bigint ,
         total_videos int,
         Views bigint,
         Channel_Description text,
         Playlist_id varchar(80))'''

        postgres_cursor.execute(create_query)
        postgres_conn.commit()
    except:
        msg_ch_tab='channels table already created'

    # Retrieve channels data from MongoDB and insert into PostgreSQL
    for each_doc in mycol.find({},{"Channel_information": 1}):
        if each_doc["Channel_information"]['Channel_Id']==channel_id:
            # Extract data from MongoDB document and format it for SQL
            sql_ch_data = (each_doc["Channel_information"]['Channel_Id'], each_doc["Channel_information"]['Channel_Name'],
                            each_doc["Channel_information"]['Subscription_Count'], each_doc["Channel_information"]['Total_videos'],
                           each_doc["Channel_information"]['Channel_Views'],
                            each_doc["Channel_information"]['Channel_Description'], each_doc["Channel_information"]['Playlist_id'])

        # Define your SQL query
            sql_ch_query = '''INSERT INTO channels(channel_id, Channel_name,
                                        subscribers,Views,total_videos ,Channel_Description ,Playlist_id)
                            VALUES (%s,%s,%s,%s,%s,%s,%s)'''
            try:
                # Execute the SQL query
                postgres_cursor.execute(sql_ch_query, sql_ch_data)
                msg_ch = "Channels are successfully inserted"
                postgres_conn.commit()
            except:
               msg_ch="Channels are already inserted"
    return msg_ch_tab,msg_ch

# For videos table creation and insertion
def vid_sql_insert(channel_id):
    msg_vid_tab, msg_vid = '', ''
    try:
        create_vid_tab_query = '''create table if not exists videos
            (channel_id varchar(80),Channel_name varchar(100),
            Video_Id varchar(30) primary key,
            Video_Name varchar(100), Video_Description text,Tags text,
            Published_At timestamp,View_Count bigint,
            Like_Count bigint,Favorite_Count int,
            Comment_Count int,Duration interval,
            Thumbnail varchar(200), Caption_Status varchar(50)
        )'''
        postgres_cursor.execute(create_vid_tab_query)
        postgres_conn.commit()
    except:
        msg_vid_tab = 'videos table already created'

        # Retrieve videos data from MongoDB and insert into PostgreSQL
    for each_doc in mycol.find({}, {"Video_details": 1, "Channel_information": 1}):
        if each_doc["Channel_information"]['Channel_Id'] == channel_id:
            for vid_count in range(len(each_doc["Video_details"])):
                sql_vi_data = (each_doc["Video_details"][vid_count]['Channel_Id'],
                               each_doc["Video_details"][vid_count]['Channel_Name'],
                               each_doc["Video_details"][vid_count]['Video_Id'],
                               each_doc["Video_details"][vid_count]['Video_Name'],
                               each_doc["Video_details"][vid_count]['Video_Description'],
                               each_doc["Video_details"][vid_count]['Tags'],
                               each_doc["Video_details"][vid_count]['Published_At'],
                               each_doc["Video_details"][vid_count]['View_Count'],
                               each_doc["Video_details"][vid_count]['Like_Count'],
                               each_doc["Video_details"][vid_count]['Favorite_Count'],
                               each_doc["Video_details"][vid_count]['Comment_Count'],
                               each_doc["Video_details"][vid_count]['Duration'],
                               each_doc["Video_details"][vid_count]['Thumbnail'],
                               each_doc["Video_details"][vid_count]['Caption_Status'],)

                # Define your SQL query
                sql_vi_query = '''INSERT INTO videos(channel_id ,Channel_name,Video_Id ,
                    Video_Name, Video_Description,Tags,
                    Published_At ,View_Count ,
                    Like_Count ,Favorite_Count ,
                    Comment_Count ,Duration ,
                    Thumbnail , Caption_Status
                                )
                                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
                try:
                    # Execute the SQL query
                    postgres_cursor.execute(sql_vi_query, sql_vi_data)
                    postgres_conn.commit()
                    msg_vid = "videos are successfully inserted"
                except:
                    msg_vid = "videos are already inserted"
    return msg_vid_tab, msg_vid


# For comments table creation and insertion
def com_sql_insert(channel_id):
    msg_com_tab, msg_com = '', ''
    try:
        create_com_tab_query = '''create table if not exists comments
            (Comment_Id varchar(80) primary key,
            Comment_text text,
            Video_Id varchar(50),
            Comment_Author varchar(150),
            Comment_Published_at timestamp
            )'''
        postgres_cursor.execute(create_com_tab_query)
        postgres_conn.commit()
    except:
        msg_com_tab = 'comments table already created'

        # Retrieve comments data from MongoDB and insert into PostgreSQL
    for each_doc in mycol.find({}, {"Channel_information": 1, "Comments_details": 1}):
        if each_doc["Channel_information"]['Channel_Id'] == channel_id:
            for com_count in range(len(each_doc["Comments_details"])):
                sql_com_data = (each_doc["Comments_details"][com_count]['Comment_Id'],
                                each_doc["Comments_details"][com_count]['Comment_text'],
                                each_doc["Comments_details"][com_count]['video_id'],
                                each_doc["Comments_details"][com_count]['Comment_Author'],
                                each_doc["Comments_details"][com_count]['Comment_Published_at']
                                )

                # Define your SQL query
                sql_com_query = '''INSERT INTO comments(Comment_Id,
                                    Comment_text ,
                                    Video_Id ,
                                    Comment_Author,
                                    Comment_Published_at
                                    )
                                VALUES (%s,%s,%s,%s,%s)'''
                try:
                    # Execute the SQL query
                    postgres_cursor.execute(sql_com_query, sql_com_data)
                    postgres_conn.commit()
                    msg_com = "comments are successfully inserted"
                except:
                    msg_com = "comments are already inserted"
    return msg_com_tab, msg_com

main()

