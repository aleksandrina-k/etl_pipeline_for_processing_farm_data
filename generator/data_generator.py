import random
import string
from randomtimestamp import randomtimestamp


def generate_farm_data(n: int) -> list:
    farms_data = []
    for _ in range(n):
        current_farm_data = [value for key, value in vars(Farm()).items()]
        farms_data.append(current_farm_data)

    return farms_data


class Farm:
    ALL_CHARACTERS = string.ascii_uppercase + string.digits
    COUNTRY_CITY_MAPPING = {
        "Germany": "Berlin",
        "Romania": "Constanta",
        "Bulgaria": "Sofia",
        "The Netherlands": "Delft",
        "Portugal": "Evora",
        "Spain": "Madrid",
        "Slovenia": "Piran",
        "Poland": "Krakow",
        "Greece": "Thessaloniki",
        "Sweden": "Umea",
    }

    def __init__(self):
        self.farm_license = self.generate_farm_license()
        # self.system_number = self.generate_system_number()
        self.country = self.generate_country()
        self.city = self.generate_city()
        self.creation_timestamp = self.generate_creation_timestamp()

    def generate_farm_license(self):
        new_licence_parts = [
            "".join(random.choices(self.ALL_CHARACTERS, k=5)) for _ in range(3)
        ]
        return "-".join(new_licence_parts)

    @staticmethod
    def generate_system_number():
        return int(random.randint(80, 85))

    def generate_country(self):
        return random.choice(list(self.COUNTRY_CITY_MAPPING.keys()))

    def generate_city(self):
        return self.COUNTRY_CITY_MAPPING.get(self.country)

    @staticmethod
    def generate_creation_timestamp():
        return randomtimestamp(start_year=2018, end_year=2022)


if __name__ == "__main__":
    print(generate_farm_data(10))
