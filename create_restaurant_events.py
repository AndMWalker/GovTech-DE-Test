import json
import requests
import pandas as pd
from datetime import datetime

class RestaurantEventProcessor:
    def __init__(self):
        self.columns = [
            "Event Id",
            "Restaurant Id",
            "Restaurant Name",
            "Photo URL",
            "Event Title",
            "Event Start Date",
            "Event End Date"
        ]
        
        self.start_april = datetime(2019, 4, 1)  # April 1 2019
        self.end_april = datetime(2019, 4, 30)   # April 30 2019
        self.restaurants_full = pd.DataFrame(columns=self.columns)

    def fetch_restaurant_data(self, url): #fetch restaurant data from url
        response = requests.get(url)
        if response.status_code == 200:
            return response.json()

    def process_data(self, data):
        for restaurant_lst in data:
            res = restaurant_lst['restaurants']
            for res_info in res:
                if 'zomato_events' not in res_info['restaurant']:
                    continue

                res_id = res_info['restaurant']['R'].get('res_id')
                res_name = res_info['restaurant'].get('name')
                for e in res_info['restaurant']['zomato_events']:
                    start_date_str = e['event'].get('start_date')
                    end_date_str = e['event'].get('end_date')
                    start_date = datetime.strptime(start_date_str, '%Y-%m-%d') #converting start date and end date to datetime format
                    end_date = datetime.strptime(end_date_str, '%Y-%m-%d')

                    if ((start_date <= self.end_april) and (end_date >= self.start_april)): #Assumption that as long as the event has a single day that is within April 2019, we add to the list
                        photo_url = e['event']['photos'][0]['photo'].get('url') if e['event']['photos'] else 'NA' #making NA if no event photos as instructed
                        new_row = {
                            "Event Id": e['event'].get('event_id'),
                            "Restaurant Id": res_id,
                            "Restaurant Name": res_name,
                            "Photo URL": photo_url,
                            "Event Title": e['event'].get('title'),
                            "Event Start Date": start_date_str,
                            "Event End Date": end_date_str
                        }
                        self.restaurants_full = self.restaurants_full.append(new_row, ignore_index=True)

    def convert_data_types(self): #These data types make the most sense to me
        data_types = {
            "Event Id": int,
            "Restaurant Id": int,
            "Restaurant Name": str,
            "Photo URL": str,
            "Event Title": str,
            "Event Start Date": str,
            "Event End Date": str
        }
        self.restaurants_full = self.restaurants_full.astype(data_types)

    def run(self, url):
        data = self.fetch_restaurant_data(url)
        if data:
            self.process_data(data)
            
    def save_to_csv(self, file_name):
        self.restaurants_full.to_csv(file_name, index=False)

def main():
    url = "https://raw.githubusercontent.com/Papagoat/brain-assessment/main/restaurant_data.json"
    processor = RestaurantEventProcessor()
    processor.run(url)
    processor.convert_data_types()
    processor.save_to_csv("restaurant_events.csv")
    
if __name__ == "__main__":
    main()