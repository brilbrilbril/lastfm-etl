from collections import defaultdict
import io
import time

from airflow.sdk import DAG, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.bash import BashOperator
import requests
from dotenv import load_dotenv
import os
from datetime import datetime
from enum import Enum
import csv

load_dotenv()

class DataName(Enum):
    TopTracks = ("user.gettoptracks", "toptracks", "track")
    TopArtists = ("user.gettopartists", "topartists", "artist")
    TopAlbums = ("user.gettopalbums", "topalbums", "album")
    # TopTags = ("user.gettoptags", "toptags", "tag")

    def __init__(self, method, root_key, list_key):
        self.method = method
        self.root_key = root_key
        self.list_key = list_key

    def transform(self, item: dict) -> dict:
        if self == DataName.TopTracks:
            return {
                'title': item.get('name', ''),
                'artist': item.get('artist', {}).get('name', ''),
                'duration': item.get('duration', ''),
                'play_count': item.get('playcount', ''),
            }
        elif self == DataName.TopArtists:
            return {
                'name': item.get('name', ''),
                'play_count': item.get('playcount', ''),
                'rank': item.get('@attr', {}).get('rank', ''),
            }
        elif self == DataName.TopAlbums:
            return {
                'artist_name': item.get('artist', {}).get('name', ''),
                'album_name': item.get('name', ''),
                'play_count': item.get('playcount', ''),
                'rank': item.get('@attr', {}).get('rank', ''),
            }
        # elif self == DataName.TopTags:
        #     return {
        #         'name': item.get('name', ''),
        #         'count': item.get('count', ''),
        #         'url': item.get('url', ''),
        #     }
    
# FIELD_COLUMNS = {
#     'toptags':    ['name', 'count', 'url'],
#     'toptracks':  ['title', 'artist', 'duration', 'play_count'],
#     'topartists': ['name', 'play_count', 'rank'],
#     'topalbums':  ['artist_name', 'album_name', 'play_count', 'rank'],
# }    

def transform_data(data, data_name: DataName):
    items = data.get(data_name.root_key, {}).get(data_name.list_key, [])
    cleaned = defaultdict(list)
    for item in items:
        for field, value in data_name.transform(item=item).items():
            cleaned[field].append(value)
    return dict(cleaned)
                
        
with DAG(
    dag_id="ingest_data_v43",
    description="ingest data from lastfm",
    start_date=datetime(2026, 3, 17),
    schedule="@daily",
    tags=["example"],
    catchup=False
) as dag:
    
    create_toptracks_stg_table = SQLExecuteQueryOperator(
        task_id="create_toptracks_stg_table",
        conn_id="pg_conn",
        sql='''
        DROP TABLE IF EXISTS toptracks_temp;
        
        CREATE TABLE IF NOT EXISTS toptracks_temp (
            "title" VARCHAR(255),
            "artist" VARCHAR(255),
            "duration" INTEGER,
            "play_count" INTEGER
        );
        '''
    )
    
    create_topartists_stg_table = SQLExecuteQueryOperator(
        task_id="create_topartists_stg_table",
        conn_id="pg_conn",
        sql='''
        DROP TABLE IF EXISTS topartists_temp;
        
        CREATE TABLE IF NOT EXISTS topartists_temp (
            "name" TEXT,
            "play_count" INTEGER,
            "rank" INTEGER
        );
        '''
    )
    
    create_topalbums_stg_table = SQLExecuteQueryOperator(
        task_id="create_topalbums_stg_table",
        conn_id="pg_conn",
        sql='''
        DROP TABLE IF EXISTS topalbums_temp;
        
        CREATE TABLE IF NOT EXISTS topalbums_temp (
            "artist_name" TEXT,
            "album_name" TEXT,
            "play_count" INTEGER,
            "rank" INTEGER
        );
        '''
    )
    
    create_artisttags_stg_table = SQLExecuteQueryOperator(
        task_id="create_artisttags_stg_table",
        conn_id="pg_conn",
        sql='''
        DROP TABLE IF EXISTS artisttags_temp;
        
        CREATE TABLE IF NOT EXISTS artisttags_temp (
            "artist_name" VARCHAR(255),
            "tag_name" VARCHAR(255),
            "count" INTEGER
        );
        '''
    )
    
    create_toptracks_table = SQLExecuteQueryOperator(
        task_id="create_toptracks_table",
        conn_id="pg_conn",
        sql='''
        CREATE TABLE IF NOT EXISTS toptracks (
            "title" VARCHAR(255),
            "artist" VARCHAR(255),
            "duration" INTEGER,
            "play_count" INTEGER
        );
        '''
    )
    
    create_topartists_table = SQLExecuteQueryOperator(
        task_id="create_topartists_table",
        conn_id="pg_conn",
        sql='''
        CREATE TABLE IF NOT EXISTS topartists (
            "name" TEXT,
            "play_count" INTEGER,
            "rank" INTEGER
        );
        '''
    )
    
    create_topalbums_table = SQLExecuteQueryOperator(
        task_id="create_topalbums_table",
        conn_id="pg_conn",
        sql='''
        CREATE TABLE IF NOT EXISTS topalbums (
            "artist_name" TEXT,
            "album_name" TEXT,
            "play_count" INTEGER,
            "rank" INTEGER
        );
        '''
    )
    
    create_artisttags_table = SQLExecuteQueryOperator(
        task_id="create_artisttags_table",
        conn_id="pg_conn",
        sql='''
        CREATE TABLE IF NOT EXISTS artisttags (
            "artist_name" VARCHAR(255),
            "tag_name" VARCHAR(255),
            "count" INTEGER
        );
        '''
    )
    
    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command="cd /opt/dbt/lastfm_pipeline && dbt build --profiles-dir /opt/dbt/lastfm_pipeline",
    )
    
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/dbt/lastfm_pipeline && dbt test --profiles-dir /opt/dbt/lastfm_pipeline"
    )
    
    @task
    def extract_and_transform_task():
        BASE_URL = "http://ws.audioscrobbler.com/2.0/"
        extracted_data = defaultdict(dict)
        for name in DataName:
            request = {
                "method": "GET",
                "url": BASE_URL,
                "params": {
                    "method": name.method,
                    "user": os.getenv("LASTFM_ACCOUNT_NAME"),
                    "api_key": os.getenv("LASTFM_API_KEY"),
                    "format": "json",
                    "limit": 1000
                }
            }
            response = requests.request(**request).json()
            print(f"raw response for {name.root_key}: {response}")
            cleaned_response = transform_data(response, name)
            print(f"cleaned response for {name.root_key}: {cleaned_response}")
            extracted_data[name.root_key] = cleaned_response 
        return extracted_data
    
    @task
    def enriched_artist_tags(data: dict):
        BASE_URL = "http://ws.audioscrobbler.com/2.0/"
        artist_names = data["topartists"]["name"]
        enriched_data = defaultdict(list)
        
        for artist_name in artist_names:
            print(f"Processing {artist_name}...")
            request = {
                "method": "GET",
                "url": BASE_URL,
                "params": {
                    "method": "artist.gettoptags",
                    "artist": artist_name,
                    "api_key": os.getenv("LASTFM_API_KEY"),
                    "format": "json",
                    "limit": 10 
                }
            }
            
            response_tags = requests.request(**request).json()
            tags = response_tags.get("toptags", {}).get("tag", [])
            for tag in tags:
                enriched_data['artist_name'].append(artist_name)
                enriched_data['tag_name'].append(tag.get('name', ''))
                enriched_data['count'].append(tag.get('count', ''))
                
            time.sleep(0.02)
        
        return {'artisttags': dict(enriched_data)}
                
    
    @task
    def insert_to_stg(data: dict):
        postgres_hook = PostgresHook(postgres_conn_id="pg_conn")
        conn = postgres_hook.get_conn()
        cur = conn.cursor()
        
        print(f"Data: {data}")
        processed = []
        for field, value in data.items():
            COLUMNS = list(value.keys())
            if not COLUMNS:
                print(f"Skipping {field}: no data")
                continue
            buffer = io.StringIO()
            writer = csv.writer(buffer)
            
            for row in zip(*[value[col] for col in COLUMNS]):
                writer.writerow(row)
            
            buffer.seek(0)
            
            cur.copy_expert(
            f"COPY {field}_temp ({', '.join(COLUMNS)}) FROM STDIN WITH CSV",
            buffer
        )
            processed.append(field)
            
        conn.commit()
        return processed
        
    @task
    def merge_data(processed_fields: list):
        hook = PostgresHook(postgres_conn_id="pg_conn")
        conn = hook.get_conn()
        cur = conn.cursor()
        
        for field in processed_fields:
            cur.execute(f"TRUNCATE {field}")
            cur.execute(f"INSERT INTO {field} SELECT * FROM {field}_temp")
            
        conn.commit()
        cur.close()
        conn.close()
    
    ET_task = extract_and_transform_task()
    enriched_task = enriched_artist_tags(ET_task)
    insert_task = insert_to_stg.override(task_id="insert_extracted_data_to_stg")(ET_task)
    insert_enriched_task = insert_to_stg.override(task_id="insert_enriched_data_to_stg")(enriched_task)
    merge_task = merge_data.override(task_id="merge_extracted_data")(insert_task)
    merge_enriched_task = merge_data.override(task_id="merge_enriched_data")(insert_enriched_task)

    [
        create_topalbums_stg_table,
        create_toptracks_stg_table,
        create_artisttags_stg_table,
        create_topartists_stg_table,
        create_toptracks_table,
        create_topartists_table,
        create_topalbums_table,
        create_artisttags_table
    ] >> insert_task >> insert_enriched_task >> merge_task >> merge_enriched_task >> dbt_build >> dbt_test
# extract_data()
    
