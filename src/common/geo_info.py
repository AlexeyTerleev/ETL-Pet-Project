import typing
import requests
from haversine import haversine


class Client:

    API_KEY = "2ea5f3ce-e677-4bb0-8834-12ee29730cb8"
    API_URL = "https://geocode-maps.yandex.ru/1.x/"
    PARAMS = {"format": "json", "apikey": API_KEY}

    @classmethod
    def request(cls, address: str) -> dict:

        response = requests.get(
            cls.API_URL, params=dict(geocode=address, **cls.PARAMS)
        )

        if response.status_code != 200:
            print("Non-200 response from yandex geocoder")
            raise Exception

        return response.json()["response"]

    @classmethod
    def coordinates(cls, address: str) -> typing.Tuple[float, float]:

        data = cls.request(address)["GeoObjectCollection"]["featureMember"]

        if not data:
            print('"{}" not found'.format(address))
            raise Exception

        coordinates = data[0]["GeoObject"]["Point"]["pos"]
        return tuple([float(x) for x in coordinates.split(" ")][::-1])

    @classmethod
    def distance_to_center(cls,  coordinates: typing.Tuple[float, float]) -> float:
        center = (53.902627, 27.561216)
        return round(haversine(coordinates, center), 3)

