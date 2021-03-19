from pydantic import BaseModel,validator,PrivateAttr,Field
from typing import List,Type,Union,Dict,Any
from influxdb_client import InfluxDBClient, Point, WriteOptions
from influxdb_client.client.write_api import SYNCHRONOUS
import os 
import requests
from urllib.parse import urljoin
from .bucket import Bucket
from datetime import datetime
import pandas as pd

class InfluxWriteOptions(BaseModel):
    batch_size:int=1000
    flush_interval:int=1000
    jitter_interval:int=0
    retry_interval:int=5000
    max_retries:int=3
    max_retry_delay:int=180000
    exponential_base:int=5

class DB(BaseModel):
    org:str=Field(default_factory=lambda:os.environ.get("INFLUX_ORG"))
    token:str=Field(default_factory=lambda:os.environ.get("INFLUX_TOKEN"))
    url:str=Field(default_factory=lambda:os.environ.get("INFLUX_URL"))
    write_options:InfluxWriteOptions=InfluxWriteOptions()
    _client:Any=PrivateAttr()
    _write_client:Any=PrivateAttr()

    def __init__(self,**data):
        super().__init__(**data)
        connection_kwargs = {
            "url":self.url,
            "token":self.token,
            "org":self.org
        }
        for name,val in connection_kwargs.items():
            if val is None:
                raise ValueError(f'{name} value cannot be NoneType. Specify it when instantiating DB or set INFLUX_{name.upper()} environment variable')
        try:
            self._client = InfluxDBClient(
                url=self.url,
                token=self.token,
                org=self.org
            )
            self._write_client = self._client.write_api(
                write_options=WriteOptions(**self.write_options.dict())
                )

        except:
            raise Exception(f'Could not create database client.')
    
    @property
    def buckets_api(self):
        return self._client.buckets_api()
    @property
    def query_api(self):
        return self._client.query_api()

    def list_buckets(self):
        buckets_result = self.buckets_api.find_buckets()
        output = []
        for b in buckets_result.buckets:
            bucket_dict = b.to_dict()
            bucket_dict["db"]=self
            bucket = Bucket(**bucket_dict)
            output.append(bucket)
        return  output
    def get_bucket_by_name(self,id:str):
        bucket_result = self.buckets_api.find_bucket_by_name(id)
        if bucket_result:
            bucket_dict = bucket_result.to_dict()
            bucket_dict["db"]=self
            bucket = Bucket(**bucket_dict)
            return bucket
        return None
    # def write_points(self,points):
    #     self._client.write_points(points)
    def get_data(self,bucket_name,fields:List[str]=None,start=None,stop=None,tags:Dict[str,str]=None,average_over:str=None,as_dataframe:bool=False):
        if isinstance(start,datetime):
            start = int(datetime.timestamp(start))
        if isinstance(stop,datetime):
            stop = int(datetime.timestamp(stop))
        if start and stop:
            query_str = f'from(bucket:"{bucket_name}") |> range(start: {start}, stop: {stop})'
        elif start and not stop:
            query_str = f'from(bucket:"{bucket_name}") |> range(start: {start})'
        elif not start and stop:
            query_str = f'from(bucket:"{bucket_name}") |> range(stop: {stop})'
        else:
            query_str = f'from(bucket:"{bucket_name}")'
        if isinstance(fields,list) and len(fields)>0:
            if len(fields)==1:
                query_str += f' |> filter(fn: (r) => r["_field"] == "{fields[0]}")'
            else:
                field_filter_list = [f'r["_field"] == "{f}"' for f in fields]
                field_filter = " or ".join(field_filter_list)
                query_str += f' |> filter(fn: (r) => {field_filter})'
        if tags is not None and len(tags)>0:
            tag_filter_list = [f'r["{tag_name}"] == "{tag_value}"' for tag_name,tag_value in tags.items()]
            tag_filter = " and ".join(tag_filter_list)
            query_str += f' |> filter(fn: (r) => {tag_filter})'

        if average_over:
            query_str += f' |> aggregateWindow(every: {average_over}, fn: mean, createEmpty: false)'
        result =  self.query_api.query(query_str)
        records_dict = {}
        times = []
        for table in result:
            print(table)
            for record in table.records:
                value = record.get_value()
                field = record.get_field()
                time = record.get_time()
                times.append(time)
                if field in records_dict:
                    records_dict[field].append(value)
                else:
                    records_dict[field]=[value]
        records_dict['time'] = times

                # print(time,field,value)
                # if time in records_dict:
                #     records_dict[time][field]=value
                # else:
                #     records_dict[time]={field:value}
        # if len(records_dict)>0:
        # records = [{**{"time":time},**value} for time,value in records_dict.items()]
        # if as_dataframe:
        # print([(j,len(i)) for j,i in records_dict.items()])
        # records = pd.DataFrame.from_dict(records_dict).set_index("time")
        return records_dict
        
