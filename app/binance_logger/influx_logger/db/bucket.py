from pydantic import BaseModel
from typing import List,Dict,Any
from datetime import datetime
from .measurement import Measurement

class Bucket(BaseModel):
    id:str
    org_id:str
    type:str
    description:str=None
    name:str
    retention_rules:List[Dict]
    created_at:datetime
    updated_at:datetime
    links:Dict[str,str]
    labels:List[str]
    rp:Any
    db:Any=None
    
    @property
    def measurements(self):
        result = self.db.query_api.query(f'import "influxdata/influxdb/schema"\nschema.measurements(bucket: "{self.name}")')
        measurement_list = []
        for t in result:
            for r in t.records:
                measurement_list.append(Measurement(name=r.get_value(),bucket=self))
        return measurement_list

    