import json
import requests
import pandas as pd

class RestaurantDataProcessor:
    def __init__(self):
        self.columns = [
            "User Aggregate Rating",
            "User Rating Text"
        ]
        self.Ratings = pd.DataFrame(columns=self.columns)

    def fetch_restaurant_data(self, url): #fetch restaurant data from url
        response = requests.get(url)

        if response.status_code == 200:
            data = response.json()
            self.process_data(data)

    def process_data(self, data):
        for restaurant_lst in data:
            res = restaurant_lst['restaurants']
            for res_info in res:
                new_row = {
                    "User Aggregate Rating": res_info['restaurant']['user_rating'].get('aggregate_rating'),
                    "User Rating Text": res_info['restaurant']['user_rating'].get('rating_text') #only these 2 columns needed
                }      
                self.Ratings = self.Ratings.append(new_row, ignore_index=True)

    def convert_data_types(self): 
        data_types = {
            "User Aggregate Rating": float,
            "User Rating Text": str
        }
        self.Ratings = self.Ratings.astype(data_types)
        
    def partition(self, df):
        excellent_rating = df[df['User Rating Text'] == 'Excellent']['User Aggregate Rating'] #splitting the dataframe into the appropriate rating texts
        vg_rating = df[df['User Rating Text'] == 'Very Good']['User Aggregate Rating']
        g_rating = df[df['User Rating Text'] == 'Good']['User Aggregate Rating']
        ave_rating = df[df['User Rating Text'] == 'Average']['User Aggregate Rating']
        poor_rating = df[df['User Rating Text'] == 'Poor']['User Aggregate Rating']
        print('From the data:')
        print(f" Excellent ratings are between {max(excellent_rating)} and {min(excellent_rating)}")
        print(f" Very Good ratings are between {max(vg_rating)} and {min(vg_rating)}")
        print(f" Good ratings are between {max(g_rating)} and {min(g_rating)}")
        print(f" Average ratings are between {max(ave_rating)} and {min(ave_rating)}")
        print(f" Poor ratings are between {max(poor_rating)} and {min(poor_rating)}")
        print("")
        print('Based on this, my assumption is that:')
        print(f" Excellent ratings are between {5.0} and {min(excellent_rating)}") #max of excellent should be the max rating which i assume is 5.0
        print(f" Very Good ratings are between {max(vg_rating)} and {min(vg_rating)}")
        print(f" Good ratings are between {max(g_rating)} and {min(g_rating)}")
        print(f" Average ratings are between {max(ave_rating)} and {min(ave_rating)}")
        print(f" Poor ratings are between {2.4} and {0}") #logically, the pattern of the above ratings tells me that poor rating max is likely 2.4 as average rating starts at 2.5
    
def main():
    url = "https://raw.githubusercontent.com/Papagoat/brain-assessment/main/restaurant_data.json"
    processor = RestaurantDataProcessor()
    processor.fetch_restaurant_data(url)
    processor.convert_data_types()
    ratings_df = processor.Ratings
    processor.partition(ratings_df)

if __name__ == "__main__":
    main()
    