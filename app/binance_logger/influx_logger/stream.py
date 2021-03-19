from pydantic import BaseModel,validator,PrivateAttr,Field
from typing import List,Type,Union,Dict,Any
from influxdb_client import InfluxDBClient, Point, WriteOptions
from influxdb_client.client.write_api import SYNCHRONOUS
import os 
from time import sleep,time_ns
import logging
from .db import DB

class Stream(BaseModel):
    name:str
    influx_bucket:str
    tags:Dict[str,str]
    db:DB=Field(default_factory=lambda:DB())
    value_spec:Dict[str,Type[Union[str,int,float,bool]]]
    def __init__(self,**data):
        super().__init__(**data)
        bucket = self.db._client.buckets_api().find_bucket_by_name(self.influx_bucket)
        if bucket is None:
            raise ValueError(f'Bucket {self.influx_bucket} does not exist in database')
    @validator('value_spec')
    def validate_value_spec(cls,v):
        allowed_types = [int,str,float,bool]
        for name,data_type in v.items():
            if data_type not in [int,str,float,bool]:
                raise ValueError(f"The type for {name} must be one of {allowed_types}")
        if len(v)==0:
            raise ValueError(f"value spec can't be an empty dictionary")
        return v

    def log_data_point(self,data,time=None):
        for name,val in data.items():
            if name not in self.value_spec:
                raise ValueError(f'{name} is not part of this stream')
            if not isinstance(val,self.value_spec[name]):
                raise ValueError(f'value for {name} must be {self.value_spec[name]} but got {type(val)}')
        if time is None:
            time = time_ns()
        point = {
            "measurement":self.name,
            "tags":self.tags,
            "fields":data,
            "time":time
        }
        self.db._write_client.write(
            self.influx_bucket,
            self.db.org,
            point
        )
    def register_logging_function(self,function,delay_interval=None,max_retries=0):
        errors = []
        while True:
            if len(errors) <= max_retries:
                try:
                    log_data = function()
                    self.log_data_point(log_data)
                    if delay_interval:
                        sleep(delay_interval)
                except Exception as e:
                    logging.exception("")
                    errors.append(str(e))
            else:
                raise Exception(f'Logging function failed more than {max_retries} times. Errors: {set(errors)}')



