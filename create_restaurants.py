import json
import requests
import pandas as pd

class RestaurantDataProcessor:
    def __init__(self):
        self.columns = [
            "Restaurant Id",
            "Restaurant Name",
            "Country",
            "City",
            "User Rating Votes",
            "User Aggregate Rating (in float)",
            "Cuisines"
        ]
        self.restaurants_full = pd.DataFrame(columns=self.columns)
        self.country_dict = self.load_country_codes()

    def load_country_codes(self):
        cc = pd.read_excel('Country-Code.xlsx')
        return dict(zip(cc['Country Code'], cc['Country']))

    def fetch_restaurant_data(self, url):
        response = requests.get(url)

        if response.status_code == 200:
            data = response.json()
            self.process_data(data)

    def process_data(self, data):
        for restaurant_lst in data:
            res = restaurant_lst['restaurants']
            for res_info in res:
                country_id = int(res_info['restaurant']['location'].get('country_id'))
                country = self.country_dict.get(country_id, 'NA')
                new_row = {
                    "Restaurant Id": int(res_info['restaurant']['R'].get('res_id')),
                    "Restaurant Name": res_info['restaurant'].get('name'),
                    "Country": country,
                    "City": res_info['restaurant']['location'].get('city'),
                    "User Rating Votes": int(res_info['restaurant']['user_rating'].get('votes')),
                    "User Aggregate Rating (in float)": float(res_info['restaurant']['user_rating'].get('aggregate_rating')),
                    "Cuisines": res_info['restaurant'].get('cuisines')
                }
                self.restaurants_full = self.restaurants_full.append(new_row, ignore_index=True)

    def convert_data_types(self):
        data_types = {
            "Restaurant Id": int,
            "Restaurant Name": str,
            "Country": str,
            "City": str,
            "User Rating Votes": int,
            "User Aggregate Rating (in float)": float,
            "Cuisines": str
        }
        self.restaurants_full = self.restaurants_full.astype(data_types)

    def save_to_csv(self, file_name):
        self.restaurants_full.to_csv(file_name, index=False)
    
def main():
    url = "https://raw.githubusercontent.com/Papagoat/brain-assessment/main/restaurant_data.json"
    processor = RestaurantDataProcessor()
    processor.fetch_restaurant_data(url)
    processor.convert_data_types()
    processor.save_to_csv("restaurants.csv")

if __name__ == "__main__":
    main()