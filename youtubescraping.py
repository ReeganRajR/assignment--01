from googleapiclient.discovery import build
import pymongo
import psycopg2
import pandas as pd
import streamlit as st

#Api key establishment.

def Api_connect():
    Api_Id="AIzaSyDtE3WrMB_c6I0Cmksnpvqv5omKAmwQvZI"
    api_service_name="youtube"
    api_version="v3"

    youtube=build(api_service_name,api_version,developerKey=Api_Id)

    return youtube

youtube=Api_connect()


#get chnanel information using the modular functions.

def get_channel_info(channel_id):
    request=youtube.channels().list(
                    part="snippet,ContentDetails,statistics",
                    id=channel_id
    )
    response=request.execute()
    for i in response['items']:
        data=dict(Channel_Name=i['snippet']['title'],
                Channel_Id=i['id'],
                Subscribers=i['statistics']['subscriberCount'],
                Views=i["statistics"]["viewCount"],
                Total_videos=i["statistics"]['videoCount'],
                Channel_Description=i["snippet"]['description'],
                playlist_Id=i['contentDetails']["relatedPlaylists"]["uploads"])
    return data

#obtaining videos information using modular functions.
def get_video_ids(channel_id):
    video_ids=[]

    response=youtube.channels().list(id=channel_id,
                                    part='contentDetails').execute()
    Playlist_Id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_token=None

    while True:
        response1=youtube.playlistItems().list(
                                            part='snippet',
                                            playlistId=Playlist_Id,
                                            maxResults=50,
                                            pageToken=next_page_token).execute()
        for i in range(len(response1['items'])):
            video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token=response1.get('nextPageToken')


        if next_page_token is None:
            break

    return video_ids

#obtain video informations using modular functions.
def get_video_info(video_ids):

    video_data=[]

    for video_id in video_ids:
        request=youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=video_id
        )
        response=request.execute()

        for item in response["items"]:
            data=dict(Channel_Name=item['snippet']['channelTitle'],
                        Channel_Id=item['snippet']['channelId'],
                        Video_Id=item['id'],
                        Title=item['snippet']['title'],
                        Tags=item['snippet'].get('tags'),
                        Thumbnail=item['snippet']['thumbnails']['default']['url'],
                        Description=item['snippet'].get('description'),
                        Published_Date=item['snippet']['publishedAt'],
                        Duration=item['contentDetails']['duration'],
                        Views=item['statistics'].get('viewCount'),
                        Likes=item['statistics'].get('likeCount'),
                        Comments=item['statistics'].get('commentCount'),
                        Favorite_count=item['statistics']['favoriteCount'],
                        Definition=item['contentDetails']['definition'],
                        Caption_Status=item['contentDetails']['caption']
                    )
            video_data.append(data)

    return video_data

#obtaining comment information using modular funtions.
def get_comment_info(video_Id_s):
        
    Comment_data=[]

    try:
        for video_id in video_Id_s:
            request=youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=100
            )
            response=request.execute()

            for item in response['items']:
                data=dict(
                        Comment_Id=item['snippet']['topLevelComment']['id'],
                        VideoID=item['snippet']['topLevelComment']['snippet']['videoId'],
                        Comment_text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                        Comment_Aurthor=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        Comment_published_date=item['snippet']['topLevelComment']['snippet']['publishedAt'])
                Comment_data.append(data)

    except:
        pass
    return Comment_data   

#obtaing playlist details..

def get_Playlist_Details(channel_id):
    next_page_token=None
    Playlist_data=[]
    while True:
        request=youtube.playlists().list(part="snippet,contentDetails",
                                        channelId=channel_id,
                                        maxResults=50,pageToken=next_page_token)
        response=request.execute()

        for item in response['items']:
            data=dict(PlaylistId=item['id'],
                        Title=item['snippet']['title'],
                        channel_Id=item['snippet']['channelId'],
                        Channel_Name=item['snippet']['channelTitle'],
                        Publish_Date=item['snippet']['publishedAt'],
                        Video_count=item['contentDetails']['itemCount'])
            Playlist_data.append(data)

        next_page_token=response.get('nextPageToken')
        if next_page_token is None:
            break
    return Playlist_data


#file trasfer to mongo

client=pymongo.MongoClient("mongodb+srv://reeganraj2906:reeganraj@cluster0.24ddllh.mongodb.net/")
db=client["youtube_data"]


def channel_details(channel_id):
    ch_details=get_channel_info(channel_id)
    ch_Playlist=get_Playlist_Details(channel_id)
    ch_video_ids=get_video_ids(channel_id)
    ch_video_info=get_video_info(ch_video_ids)
    ch_comment_info=get_comment_info(ch_video_ids)

    coll1=db["channel_details"]
    coll1.insert_one({"channel_information":ch_details,"playlist_information":ch_Playlist,"video_details":ch_video_info,"comment_information":ch_comment_info})

    return "Uploaded completed"


#table creation and connections to Sql
def channels_table():
    mydb=psycopg2.connect(host="localhost",user="postgres",password="1234",database="youtube_data",port="5432")
    cursor=mydb.cursor()
    drop_query="drop table if exists channels"
    cursor.execute(drop_query)
    mydb.commit()
    try:
        create_query="create table if not exists channels(Channel_Name varchar(100),Channel_Id varchar(80) primary key,Subscribers bigint,Views bigint,Total_videos int,Channel_Description text,playlist_Id varchar(80))"
        cursor.execute(create_query)
        mydb.commit()
    except:
        print("Failed")

    ch_list=[]
    db=client["youtube_data"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    df=pd.DataFrame(ch_list)

    for index,row in df.iterrows():
        insert_query="insert into channels(Channel_Name,Channel_Id,Subscribers,Views,Total_videos,Channel_Description,playlist_Id)values(%s,%s,%s,%s,%s,%s,%s)"
        values=(row['Channel_Name'],row['Channel_Id'],row['Subscribers'],row['Views'],row['Total_videos'],row['Channel_Description'],row['playlist_Id'])
        cursor.execute(insert_query,values)
        mydb.commit()


#to create playlist table:-
def playlist_table():
    mydb=psycopg2.connect(host="localhost",user="postgres",password="1234",database="youtube_data",port="5432")
    cursor=mydb.cursor()
    drop_query="drop table if exists playlists"
    cursor.execute(drop_query)
    mydb.commit()
    create_query="create table if not exists playlists(playlist_Id varchar(100) primary key,Title varchar(80),channel_Id varchar(100),Channel_Name varchar(100),Publish_Date timestamp,Video_count int)"
    cursor.execute(create_query)
    mydb.commit()

    Pl_list=[]
    db=client["youtube_data"]
    coll1=db["channel_details"]
    for Pl_data in coll1.find({},{"_id":0,"playlist_information":1}):
        for i in range(len(Pl_data["playlist_information"])):
            Pl_list.append(Pl_data["playlist_information"][i])
    df1=pd.DataFrame(Pl_list)

    for index,row in df1.iterrows():
            insert_query="insert into playlists(playlist_Id,Title,channel_Id,Channel_Name,Publish_Date,Video_count)values(%s,%s,%s,%s,%s,%s)"
            values=(row['PlaylistId'],row['Title'],row['channel_Id'],row['Channel_Name'],row['Publish_Date'],row['Video_count'])
            cursor.execute(insert_query,values)
            mydb.commit()

#Video_tables
def Video_tables():
        mydb=psycopg2.connect(host="localhost",user="postgres",password="1234",database="youtube_data",port="5432")
        cursor=mydb.cursor()
        drop_query="drop table if exists videos"
        cursor.execute(drop_query)
        mydb.commit()
        create_query='''create table if not exists videos(Channel_Name varchar(100),Channel_Id varchar(100),Video_Id varchar(150) primary key,
                                                        Title varchar(150),
                                                        Tags text,
                                                        Thumbnail varchar(200),
                                                        Description text,
                                                        Published_Date timestamp,
                                                        Duration interval,
                                                        Views bigint,
                                                        Likes bigint,
                                                        Comments int,
                                                        Favorite_count int,
                                                        Definition varchar(20),
                                                        Caption_Status varchar(50))'''
        cursor.execute(create_query)
        mydb.commit()


        Vd_list=[]
        db=client["youtube_data"]
        coll1=db["channel_details"]
        for Vd_data in coll1.find({},{"_id":0,"video_details":1}):
                for i in range(len(Vd_data["video_details"])):
                        Vd_list.append(Vd_data["video_details"][i])
        df2=pd.DataFrame(Vd_list)

        for index,row in df2.iterrows():
                insert_query='''insert into videos(Channel_Name,
                                                        Channel_Id,
                                                        Video_Id,
                                                        Title,
                                                        Tags,
                                                        Thumbnail,
                                                        Description,
                                                        Published_Date,
                                                        Duration,
                                                        Views,
                                                        Likes,
                                                        Comments,
                                                        Favorite_count,
                                                        Definition,
                                                        Caption_Status
                                                        )
                                                        values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
                values=(row['Channel_Name'],row['Channel_Id'],row['Video_Id'],row['Title'],row['Tags'],row['Thumbnail'],
                        row['Description'],row['Published_Date'],row['Duration'],row['Views'],row['Likes'],row['Comments'],row['Favorite_count'],
                        row['Definition'],row['Caption_Status'])
                cursor.execute(insert_query,values)
                mydb.commit()



#Comments Table
def Comments_table():
    mydb=psycopg2.connect(host="localhost",user="postgres",password="1234",database="youtube_data",port="5432")
    cursor=mydb.cursor()
    drop_query="drop table if exists comments"
    cursor.execute(drop_query)
    mydb.commit()
    create_query='''create table if not exists comments(Comment_Id varchar(100) primary key,Video_Id varchar(50),Comment_Text text,
                                                    Comment_Author varchar(150),
                                                    Comment_Published timestamp)'''
    cursor.execute(create_query)
    mydb.commit()

    Comment_list=[]
    db=client["youtube_data"]
    coll1=db["channel_details"]
    for Comment_data in coll1.find({},{"_id":0,"comment_information":1}):
            for i in range(len(Comment_data["comment_information"])):
                    Comment_list.append(Comment_data["comment_information"][i])
    df3=pd.DataFrame(Comment_list)

    for index,row in df3.iterrows():
        insert_query="insert into comments(Comment_Id,Video_Id,Comment_Text,Comment_Author,Comment_Published)values(%s,%s,%s,%s,%s)"
        values=(row['Comment_Id'],row['VideoID'],row['Comment_text'],row['Comment_Aurthor'],row['Comment_published_date'])
        cursor.execute(insert_query,values)
        mydb.commit()

#creation of common table 

def tables():
    channels_table()
    playlist_table()
    Video_tables()
    Comments_table()
    
    return "Action was sucessfull"

#streamlit

def show_channels_tables():
    ch_list=[]
    db=client["youtube_data"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    df=st.dataframe(ch_list)

    return df


def show_Playlists_tables():
    Pl_list=[]
    db=client["youtube_data"]
    coll1=db["channel_details"]
    for Pl_data in coll1.find({},{"_id":0,"playlist_information":1}):
        for i in range(len(Pl_data["playlist_information"])):
            Pl_list.append(Pl_data["playlist_information"][i])
    df1=st.dataframe(Pl_list)

    return df1

def show_video_tables():
    Vd_list=[]
    db=client["youtube_data"]
    coll1=db["channel_details"]
    for Vd_data in coll1.find({},{"_id":0,"video_details":1}):
            for i in range(len(Vd_data["video_details"])):
                    Vd_list.append(Vd_data["video_details"][i])
    df2=st.dataframe(Vd_list)

    return df2

def show_Comments_tables():
    Comment_list=[]
    db=client["youtube_data"]
    coll1=db["channel_details"]
    for Comment_data in coll1.find({},{"_id":0,"comment_information":1}):
            for i in range(len(Comment_data["comment_information"])):
                    Comment_list.append(Comment_data["comment_information"][i])
    df3=st.dataframe(Comment_list)

    return df3


#streamlit connection

with st.sidebar:
    st.title(":purple[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
    st.header("Skill Obtained !!")
    st.caption(">> Python programing")
    st.caption(">> DATA Collection")
    st.caption(">> Mongodb")
    st.caption(">> Intergration of API")
    st.caption(">> File Collection Using SQL")
    st.caption(">> Interface Creation")
channel_id=st.text_input("Channel Id")

if st.button("Proceed"):
    ch_ids=[]
    db=client["youtube_data"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_ids.append(ch_data["channel_information"]['Channel_Id'])

    if channel_id in ch_ids:
        st.success("Channel Details of the given channel id Exists")

    else:
        insert=channel_details(channel_id)
        st.success(insert)

if st.button("Move to SQL"):
    Table=tables()
    st.success(Table)

show_table=st.radio("PICK THE DATA TO VIEW",("CHANNELS","VIDEOS","COMMENTS","PLAYLISTS"))
if show_table=="CHANNELS":
    show_channels_tables()
elif show_table=="PLAYLISTS":
    show_Playlists_tables()
elif show_table=="VIDEOS":
    show_video_tables()
elif show_table=="COMMENTS":
    show_Comments_tables()

#SQL
mydb=psycopg2.connect(host="localhost",user="postgres",password="1234",database="youtube_data",port="5432")
cursor=mydb.cursor()
Question=st.selectbox("Select your questions",("1.What are the names of all the videos and their corresponding channels?",
                                               "2.Which channels have the most number of videos, and how many videos dothey have?",
                                               "3.What are the top 10 most viewed videos and their respective channels?",
                                               "4.How many comments were made on each video, and what are theircorresponding video names?",
                                               "5.Which videos have the highest number of likes, and what are their corresponding channel names?",
                                               "6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
                                               "7.What is the total number of views for each channel, and what are their corresponding channel names?",
                                               "8.What are the names of all the channels that have published videos in the year2022?",
                                               "9.What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                                               "10.Which videos have the highest number of comments, and what are their corresponding channel names?"))


if Question =="1.What are the names of all the videos and their corresponding channels?":
    Question1='''Select title as videos,channel_name as channelname from videos'''
    cursor.execute(Question1)
    mydb.commit()
    t1=cursor.fetchall()
    df=pd.DataFrame(t1,columns=["videos title","channel name"])
    st.write(df)

elif Question =="2.Which channels have the most number of videos, and how many videos dothey have?":
    Question2='''Select channel_name as Channelname,total_videos as no_videos from channels
                order by total_videos desc'''
    cursor.execute(Question2)
    mydb.commit()
    t2=cursor.fetchall()
    df1=pd.DataFrame(t2,columns=["channel name","No of video"])
    st.write(df1)

elif Question =="3.What are the top 10 most viewed videos and their respective channels?":
    Question3='''Select views as views,channel_name as channelname,title as videotitle from videos where views is not null order by views desc limit 10'''
    cursor.execute(Question3)
    mydb.commit()
    t3=cursor.fetchall()
    df2=pd.DataFrame(t3,columns=["views","channel Name","video Title"])
    st.write(df2)

elif Question =="4.How many comments were made on each video, and what are theircorresponding video names?":
    Question4='''Select comments as no_comments,title as videotitle from videos where comments is not null '''
    cursor.execute(Question4)
    mydb.commit()
    t4=cursor.fetchall()
    df3=pd.DataFrame(t4,columns=["no_comments","videotitle"])
    st.write(df3)

elif Question =="5.Which videos have the highest number of likes, and what are their corresponding channel names?":
    Question5='''Select title as videotitle,channel_name as channelname,likes as likecount from videos where likes is not null order by likes desc'''
    cursor.execute(Question5)
    mydb.commit()
    t5=cursor.fetchall()
    df4=pd.DataFrame(t5,columns=["videotitle","channelname","likecount"])
    st.write(df4)

elif Question =="6.What is the total number of likes for each video, and what are their corresponding video names?":
    Question6='''Select likes as likecount,title as videotitle from videos'''
    cursor.execute(Question6)
    mydb.commit()
    t6=cursor.fetchall()
    df5=pd.DataFrame(t6,columns=["likecount","videotitle"])
    st.write(df5)

elif Question =="7.What is the total number of views for each channel, and what are their corresponding channel names?":
    Question7='''select channel_name as channelname,views as totalviews from channels order by views desc'''
    cursor.execute(Question7)
    mydb.commit()
    t7=cursor.fetchall()
    df6=pd.DataFrame(t7,columns=["likecount","videotitle"])
    st.write(df6)

elif Question =="8.What are the names of all the channels that have published videos in the year2022?":
    Question8='''select title as video_title,published_date as videorelease ,channel_name as channelname from videos 
                where extract(year from published_date)=2022'''
    cursor.execute(Question8)
    mydb.commit()
    t8=cursor.fetchall()
    df7=pd.DataFrame(t8,columns=["video_title","videorelease","channelname"])
    st.write(df7)

elif Question =="9.What is the average duration of all videos in each channel, and what are their corresponding channel names?":
    Question9='''select channel_name as channelname,AVG(duration) as averageduration from videos group by channel_name'''
    cursor.execute(Question9)
    mydb.commit()
    t9=cursor.fetchall()
    df8=pd.DataFrame(t9,columns=["channelname","averageduration"])

    T9=[]
    for index,row in df8.iterrows():
        channel_title=row["channelname"]
        average_duration=row["averageduration"]
        average_duration_str=str(average_duration)
        T9.append(dict(channeltitle=channel_title,average_duration=average_duration_str))

    df9=pd.DataFrame(T9)
    st.write(df9)

elif Question =="10.Which videos have the highest number of comments, and what are their corresponding channel names?":
    Question10='''select title as videotitle,channel_name as channelname,comments as comments from videos where comments is not null order by comments desc'''
    cursor.execute(Question10)
    mydb.commit()
    t10=cursor.fetchall()
    df10=pd.DataFrame(t10,columns=["videotitle","channelname","comments"])
    st.write(df10)