import logging

from factory.repository_factory import RepositoryFactory
from exception.uninitialized import FailedInitialization
from config.weather import WeatherConfiguration
from service.model.catboost import CatboostModelService
from service.user import UserService
from service.weather import WeatherService
from service.predict import PredictionService

class ServiceFactory:
    instance = None

    def __init__(self, repositoryFactory : RepositoryFactory) -> None:
        self.repositoryFactory = repositoryFactory
        self.userService = None
        self.weatherService = None
        self.catboostModelService = None
        self.predictionService = None

    def getCatboostModelService(self):
        if (self.catboostModelService is None):
            try:
                self.catboostModelService = CatboostModelService(
                    self.repositoryFactory.getModelRepository()
                )
            except Exception as exc:
                logging.warning("Failed to initialize CatboostModelService")
                raise FailedInitialization("CatboostModelService") from exc
        return self.catboostModelService

    
    def getPredictionService(self):
        if (self.predictionService is None):
            try:
                self.predictionService = PredictionService(
                    self.getCatboostModelService(),
                    self.getUserService()
                )
            except Exception as exc:
                logging.warning("Failed to initialize CatboostModelService")
                raise FailedInitialization("CatboostModelService") from exc
        return self.predictionService
    
    def getUserService(self):
        if (self.userService is None):
            try:
                self.userService = UserService(
                    self.repositoryFactory.getExternalUserRepository(),
                    self.repositoryFactory.getInternalUserRepository(),
                    self.repositoryFactory.getUserStatisticsRepository(),
                    self.repositoryFactory.getUserHistoryRepository()
                )
            except Exception as exc:
                logging.warning("Failed to initialize UserService")
                raise FailedInitialization("UserService") from exc
        return self.userService

    def getWeatherService(self):
        if (self.weatherService is None):
            try:
                self.weatherService = WeatherService(
                    self.repositoryFactory.getWeatherRepository(),
                    WeatherConfiguration.getDarkSky()
                )
            except Exception as exc:
                logging.warning("Failed to initialize WeatherService")
                raise FailedInitialization("WeatherService") from exc
        return self.weatherService

    @classmethod
    def getInstance(cls):
        if (cls.instance is None):
            cls.instance = ServiceFactory(RepositoryFactory.getInstance())
        return cls.instance