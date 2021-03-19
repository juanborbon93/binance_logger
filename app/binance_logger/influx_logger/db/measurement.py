from pydantic import BaseModel
from typing import Any,List
from datetime import datetime,timedelta

class TimeRange(BaseModel):
    start:datetime
    stop:datetime

    @property
    def duration(self):
        return self.stop-self.start

class Measurement(BaseModel):
    name:str
    bucket:Any
    
    def get_data(self,start=0,stop=None,average_over:str=None,fields:List[str]=None,as_dataframe:bool=False):
        results = self.bucket.db.get_data(
            bucket_name=self.bucket.name,
            start=start,
            stop=stop,
            fields=fields,
            average_over=average_over,
            as_dataframe=as_dataframe
            )
        return results

    @property
    def time_range(self):
        start,stop = None,None
        results = self.bucket.db.query_api.query(f'from(bucket: "{self.bucket.name}")\n  |> range(start:0)\n  |> filter(fn: (r) => r["_measurement"] == "{self.name}")\n  |> first(column:"_time")')
        for t in results:
            for r in t.records:
                if start is None:
                    start = r.get_time()
        results = self.bucket.db.query_api.query(f'from(bucket: "{self.bucket.name}")\n  |> range(start:0)\n  |> filter(fn: (r) => r["_measurement"] == "{self.name}")\n  |> last(column:"_time")')
        for t in results:
            for r in t.records:
                if stop is None:
                    stop = r.get_time()
        return TimeRange(start=start,stop=stop)