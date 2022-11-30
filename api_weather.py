import requests
from models.model import WeatherForecast

def get_weather():
    city_name = "Vancouver,CA"
    api_key = '26e3ab58c982deb6571267759cd689e1'
    url = f"http://api.openweathermap.org/data/2.5/forecast?q={city_name}&appid={api_key}"

    response = requests.get(url).json()["list"]

    # getting only one weather info per day
    response_list = []

    counter_start = 0
    counter_end = 6

    for x in range(0, 5):
        temp = 0
        temp_array = []
        temp_max = response[counter_start]["main"]["temp_max"] - 273.15
        temp_min = response[counter_start]["main"]["temp_min"] - 273.15
        dt = ""
        response_raw = response[0]["main"]

        for x in range(counter_start, counter_end):
            response_raw = response[x]["main"]
            # converting temperature from kelvin to celsius
            min = response_raw["temp_min"] - 273.15
            max = response_raw["temp_max"] - 273.15
            temp_array.append(response_raw["temp"] - 273.15)
            if min < temp_min:
                temp_min = min

            if max > temp_max:
                temp_max = max

            dt = response[x]["dt_txt"][0:10]

        temp_sum = 0
        for x in temp_array:
            temp_sum += x
        temp = temp_sum/8
        response_raw["temp_avg"] = int(round(temp, 0))
        response_raw["temp_min"] = int(round(temp_min, 0))
        response_raw["temp_max"] = int(round(temp_max, 0))
        response_raw["dt_txt"] = dt


        response_list.append(WeatherForecast(**response_raw))

        counter_start += 7
        counter_end += 7

    return response_list
