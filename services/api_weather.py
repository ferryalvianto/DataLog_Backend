import requests
from models.model import WeatherForecast
import pandas as pd


def get_weather():
    city_name = "Vancouver,CA"
    api_key = '26e3ab58c982deb6571267759cd689e1'
    url = f"http://api.openweathermap.org/data/2.5/forecast?q={city_name}&appid={api_key}"

    response = requests.get(url).json()["list"]

    # getting only one weather info per day
    # response_list = []

    # counter_start = 0
    # counter_end = 6

    # for x in range(0, 5):
    #     temp = 0
    #     temp_array = []
    #     temp_max = response[counter_start]["main"]["temp_max"] - 273.15
    #     temp_min = response[counter_start]["main"]["temp_min"] - 273.15
    #     dt = ""
    #     response_raw = response[0]["main"]

    #     for x in range(counter_start, counter_end):
    #         response_raw = response[x]["main"]
    #         # converting temperature from kelvin to celsius
    #         min = response_raw["temp_min"] - 273.15
    #         max = response_raw["temp_max"] - 273.15
    #         temp_array.append(response_raw["temp"] - 273.15)
    #         if min < temp_min:
    #             temp_min = min

    #         if max > temp_max:
    #             temp_max = max

    #         dt = response[x]["dt_txt"][0:10]

    #     temp_sum = 0
    #     for x in temp_array:
    #         temp_sum += x
    #     temp = temp_sum/8
    #     response_raw["temp_avg"] = int(round(temp, 0))
    #     response_raw["temp_min"] = int(round(temp_min, 0))
    #     response_raw["temp_max"] = int(round(temp_max, 0))
    #     response_raw["dt_txt"] = dt

    #     response_list.append(WeatherForecast(**response_raw))

    #     counter_start += 7
    #     counter_end += 7

    response_list = []

    for x in range(len(response)):
        response_raw = response[x]["main"]
        # response_raw_final = ""
        # response_raw_final["temp"] = response[x]["main"]["temp"]
        response_raw["dt_txt"] = response[x]["dt_txt"][0:10]
        response_raw["temp_avg"] = response[x]["main"]["temp"]
        response_raw["temp_min"] = response[x]["main"]["temp"]
        response_raw["temp_max"] = response[x]["main"]["temp"]

        response_list.append(response_raw)

    response_df = pd.DataFrame(response_list)

    max_df = response_df.groupby("dt_txt").max('temp_max').temp_max.to_frame()
    max_df = pd.DataFrame(max_df)

    min_df = response_df.groupby("dt_txt").min('temp_min').temp_min.to_frame()
    min_df = pd.DataFrame(min_df)

    avg_df = response_df.groupby("dt_txt").mean('temp').temp.to_frame()
    avg_df = pd.DataFrame(avg_df)

    final_temp_df = pd.concat([min_df, avg_df, max_df], axis=1)
    final_temp_df.rename(columns={"temp": "temp_avg"}, inplace=True)
    final_temp_df["dt_txt"] = final_temp_df.index

    return final_temp_df.to_dict("records")
