import requests
from datetime import datetime
from logging import Logger
from typing import List


class DeliverySystemReader:
    def __init__(self, log: Logger, nickname: str, cohort: str, api_key: str, base_url: str) -> None:
        self.headers = {
                "X-Nickname" : nickname,
                "X-Cohort" : cohort,
                "X-API-KEY" : api_key
        }
        self.base_url = base_url
        
    def get_couriers(self, sort_field='id', sort_direction='asc', limit=None, offset=None) -> List:
        params = {
            "sort_field" : sort_field, 
            "sort_direction" : sort_direction,
            "limit" : limit,
            "offset" : offset
            }

        response = requests.get(
            url = self.base_url + '/couriers', 
            params = params, 
            headers = self.headers)

        objs = response.json()
        return objs
    
    def get_restaurants(self, sort_field='id', sort_direction='asc', limit=None, offset=None) -> List:
        params = {
            "sort_field" : sort_field, 
            "sort_direction" : sort_direction,
            "limit" : limit,
            "offset" : offset
            }

        response = requests.get(
            url = self.base_url + '/restaurants', 
            params = params, 
            headers = self.headers)
        
        objs = response.json()
        return objs


    def get_deliveries(self, sort_field='delivery_ts', sort_direction='asc', limit=None, offset=None, from_ts=datetime(1900,1,1), to_ts=None) -> List:
        params = {
            "sort_field" : sort_field, 
            "sort_direction" : sort_direction,
            "from": from_ts,
            "to": to_ts,
            "limit" : limit,
            "offset" : offset
            }

        response = requests.get(
            url = self.base_url + '/deliveries', 
            params = params, 
            headers = self.headers)
        
        objs = response.json()
        return objs
        