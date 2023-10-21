import io
import sys
sys.path.append('/Users/jamesgardner/Desktop/TD-ELT/TD-Source/Utils')
from Initialize import Secrets 
from Initialize import Utils
import requests 
import json 
import pandas as pd
from datetime import datetime

class Klavio(Utils):

    def __init__(self,app_type):
        super().__init__(app_type)

    def get_result(self,url):

        headers = {
                "accept": "application/json",
                "revision": "2023-06-15",
                "Authorization": "Klaviyo-API-Key {}".format(self.klavio_api_key)

                }
        response = requests.get(url, headers=headers)
        raw  = response.text
        data = json.loads(raw)
        print(data)




    def events(self,url):
        
        
        headers = {
                "accept": "application/json",
                "revision": "2023-06-15",
                "Authorization": "Klaviyo-API-Key {}".format(self.klavio_api_key)

                }
        response = requests.get(url, headers=headers)
        raw  = response.text
        data = json.loads(raw)
        df = pd.DataFrame(data['data'])

        self.putS3('turbo-debt-dev','klavio/events/klavio_events_{}.csv'.format(str(datetime.now())),df)
        
        if data['links']['next'] != 'None':
            print(data['links']['next'])
            self.events(data['links']['next'])


    def profiles(self,url):
        headers = {
                "accept": "application/json",
                "revision": "2023-06-15",
                "Authorization": "Klaviyo-API-Key {}".format(self.klavio_api_key)}

        response = requests.get(url, headers=headers)
        raw  = response.text
        data = json.loads(raw)
       
        df = pd.DataFrame(data['data'])
        print(df)

        self.putS3('turbo-debt-dev','klavio/profiles/klavio_profiles_{}.csv'.format(str(datetime.now())),df)
        
        if data['links']['next'] != 'None':
            print(data['links']['next'])
            self.profiles(data['links']['next'])

        
        

if __name__ == '__main__':

    

    # instantiate klavio class
    k = Klavio('klavio')
    url = "https://a.klaviyo.com/api/events/?filter=greater-than(datetime%2C2023-08-02T00%3A00%3A00Z)"
    #url= "https://a.klaviyo.com/api/profiles/?page[size]=100"
    k.events(url)


   





   

