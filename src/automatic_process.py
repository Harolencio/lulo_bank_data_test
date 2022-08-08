import json
import requests
import pandas as pd
import sweetviz as sv
import pandas_profiling


def extract_data(url = "http://api.tvmaze.com/schedule/web?date=2020-12-"):
    data = []

    for i in range(1,32):
        url_api =  url + str(i).zfill(2)
        result = requests.get(url_api)
        dato = json.loads(result.text)
        with open("./json/2020-12-" + str(i) +'.json', 'w') as json_file:
            json.dump(dato, json_file)
        data.extend(dato)
    return data

def dataframe_episodes(data):

    episodes = []

    for episode in data:

        show_id = episode["_embedded"]["show"]["id"]
        
        episode_id = episode["id"]
        episode_name = episode["name"]        
        episode_url = episode["url"]
        episode_season = episode["season"]
        episode_number = episode["number"]
        episode_type = episode["type"]
        episode_airdate = episode["airdate"]
        episode_airtime = episode["airtime"]
        episode_airstamp = episode["airstamp"]
        episode_runtime = episode["runtime"]
        episode_rating = episode["rating"]["average"]
        

        episode = {
            'show_id': show_id,
            'episode_id': episode_id,
            'name': episode_name,
            'url': episode_url,
            'season': episode_season,
            'number': episode_number,
            'type': episode_type,
            'airdate': episode_airdate,
            'airtime': episode_airtime,
            'airstamp': episode_airstamp,
            'runtime': episode_runtime,
            'rating': episode_rating,


        }



        episodes.append(episode)

    df_episodes = pd.DataFrame(episodes).reset_index(drop=True)
    return df_episodes

def dataframe_shows(data):

    shows = []

    for episode in data:

        show_id = episode["_embedded"]["show"]["id"]
        show_name = episode["_embedded"]["show"]["name"]
        show_url = episode["_embedded"]["show"]["url"]
        show_name = episode["_embedded"]["show"]["name"]
        show_type = episode["_embedded"]["show"]["type"]
        show_language = episode["_embedded"]["show"]["language"]
        show_genres = episode["_embedded"]["show"]["genres"]
        show_status = episode["_embedded"]["show"]["status"]
        show_runtime = episode["_embedded"]["show"]["runtime"]
        show_averageRuntime = episode["_embedded"]["show"]["averageRuntime"]
        show_premiered = episode["_embedded"]["show"]["premiered"]
        show_ended = episode["_embedded"]["show"]["ended"]
        show_officialSite = episode["_embedded"]["show"]["officialSite"]
        show_schedule_time = episode["_embedded"]["show"]["schedule"]["time"]
        show_schedule_days = episode["_embedded"]["show"]["schedule"]["days"]
        show_rating = episode["_embedded"]["show"]["rating"]["average"]
        show_weight = episode["_embedded"]["show"]["weight"]
        show_network = episode["_embedded"]["show"]["network"]
        try:
            show_webChannel_id = episode["_embedded"]["show"]["webChannel"]["id"]
        except:
            show_webChannel_id = []
        show_externals = episode["_embedded"]["show"]["externals"]
        try:
            show_image = episode["_embedded"]["show"]["image"]["original"]
        except:
            show_image = []
        show_summary = episode["_embedded"]["show"]["summary"]


        show = {
            'show_id': show_id,
            'name': show_name,
            "url": show_url,
            "type": show_type,
            "language": show_language,
            "genres": show_genres,
            "status": show_status,
            "runtime": show_runtime,
            "averageRuntime": show_averageRuntime,
            "premiered":show_premiered,
            "ended": show_ended,
            "officialSite": show_officialSite,
            "schedule_time": show_schedule_time,
            "schedule_days": show_schedule_days,
            "rating": show_rating,
            "weight": show_weight,
            "network": show_network,
            "webchannnel_id": show_webChannel_id,
            "externals": show_externals,
            "image": show_image,
            "summary": show_summary
        }

        shows.append(show)


    df_shows = pd.DataFrame(shows).drop_duplicates("show_id").reset_index(drop=True)
    return df_shows


def dataframe_webchannel(data):

    web_channels = []

    for episode in data:
        try:
            show_webChannel_id = episode["_embedded"]["show"]["webChannel"]["id"]
            show_webChannel_name = episode["_embedded"]["show"]["webChannel"]["name"]
            show_webChannel_country_name = episode["_embedded"]["show"]["webChannel"]["country"]["name"]
            show_webChannel_country_code = episode["_embedded"]["show"]["webChannel"]["country"]["code"]
            show_webChannel_country_timezone = episode["_embedded"]["show"]["webChannel"]["country"]["timezone"]
            show_webChannel_officialSite = episode["_embedded"]["show"]["webChannel"]["officialSite"]
        except:
            pass
        web_channel = {
            'id': show_webChannel_id,
            'name': show_webChannel_name,
            'country': show_webChannel_country_name,
            'code': show_webChannel_country_code,
            'timezone': show_webChannel_country_timezone,
            'officialSite': show_webChannel_officialSite,
        }

        web_channels.append(web_channel)

    df_webchannel = pd.DataFrame(web_channels).drop_duplicates("id").reset_index(drop=True)
    return df_webchannel


def profile_sv(df):
    analysis = sv.analyze(df)
    return analysis

def profile_shows(df):
    df = df[["show_id", "name", "url", "type", "language", "status", "runtime", "averageRuntime", "premiered"]]
    profile_shows= pandas_profiling.ProfileReport(df)
    profile_shows.to_file("df_shows.html")
    return df

def drop_missing(df):
    thresh = len(df) * 0.6
    df.dropna(axis=1, thresh=thresh, inplace=True)
    return df

def to_date(df):
    df["airstamp"] = df['airstamp'].str[:10]
    df["airstamp"] = df["airstamp"].apply(pd.to_datetime)
    df["airdate"] = df['airdate'].apply(pd.to_datetime)
    return df

def format_shows(df):
    df["premiered"] = df["premiered"].apply(pd.to_datetime)
    df['summary'] = df_shows['summary'].str.replace(r'<[^<]+?>', '')
    return df

def to_category(df):
    cols = df.select_dtypes(include='object').columns
    for col in cols:
        ratio = len(df[col].value_counts()) / len(df)
        if ratio < 0.05:
            df[col] = df[col].astype('category')
    return df

if __name__ == '__main__':
    datos = extract_data()
    #Dataframe Episodes
    dataframe_episodes = dataframe_episodes(datos)
    dataframe_episodes = drop_missing(dataframe_episodes)
    dataframe_episodes = to_date(dataframe_episodes)
    dataframe_episodes = to_category(dataframe_episodes)
    dataframe_episodes.to_csv('dataframe_episodes.csv', index=False) 
    profile_episodes = profile_sv(dataframe_episodes)
    profile_episodes.show_html('dataframe_episodes.html')

    #Dataframe Shows
    df_shows = dataframe_shows(datos)
    df_shows = drop_missing(df_shows)
    df_shows = format_shows(df_shows)
    df_shows = to_category(df_shows)
    df_shows.to_csv('df_shows.csv', index=False) 
    #profile_shows = profile_shows(df_shows)

    #Dataframe webchannel
    df_webchannel = dataframe_webchannel(datos)
    df_webchannel = drop_missing(df_webchannel)
    df_webchannel = to_category(df_webchannel)
    df_webchannel.to_csv('df_webchannel.csv', index=False)
    profile_webchannel = profile_sv(df_webchannel)
    profile_webchannel.show_html('df_webchannel.html')