import os

from utils.weather import DarkSky

class WeatherConfiguration:
    API_KEY = os.environ.get('DARKSKY_API_KEY')
    
    darkSky = None

    @classmethod
    def getDarkSky(cls):
        if (cls.darkSky is None):
            cls.darkSky = DarkSky(cls.API_KEY)
        return cls.darkSky