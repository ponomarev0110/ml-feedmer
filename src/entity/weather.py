from dataclasses import dataclass
from datetime import date

@dataclass
class Weather:
    formaladdr : str
    weatherDate : date
    latitude : float
    longitude : float
    apparentTemperatureHigh : float
    apparentTemperatureLow : float
    temperatureHigh : float
    temperatureLow : float
    cloudcover : float
    humidity : float
    precipIntensity : float
    precipProbability : float
    precipType : str
    pressure : float
    windSpeed : float
    windBearing : float
    moonPhase : float