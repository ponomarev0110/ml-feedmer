from sqlalchemy.engine import Engine

import logging

from exception.uninitialized import FailedInitialization
from repository.internal.user_history import UserHistoryRepository
from repository.internal.user_statistics import UserStatisticsRepository
from repository.internal.user import UserRepository as InternalUserRepository
from repository.external.user import UserRepository as ExternalUserRepository
from repository.internal.weather import WeatherRepository
from repository.internal.data import ModelDataRepository
from config.engine import InternalDatabaseConfiguration, ExternalDatabaseConfiguration

class RepositoryFactory:
    instance = None

    def __init__(self, internalEngine : Engine, externalEngine : Engine) -> None:
        self.internalEngine = internalEngine
        self.externalEngine = externalEngine
        self.userStatisticsRepository = None
        self.userHistoryRepository = None
        self.internalUserRepository = None
        self.externalUserRepository = None
        self.weatherRepository = None
        self.modelDataRepository = None

    def getUserStatisticsRepository(self) -> UserStatisticsRepository:
        if (self.userStatisticsRepository is None):
            try:
                self.userStatisticsRepository = UserStatisticsRepository(self.internalEngine)
            except Exception as exc:
                logging.warning("Failled to initialize UserStatistics Repository")
                raise FailedInitialization("UserStatisticsRepository") from exc
        return self.userStatisticsRepository

    def getUserHistoryRepository(self) -> UserHistoryRepository:
        if (self.userHistoryRepository is None):
            try:
                self.userHistoryRepository = UserHistoryRepository(self.internalEngine)
            except Exception as exc:
                logging.warning("Failled to initialize UserStatistics Repository")
                raise FailedInitialization("UserStatisticsRepository") from exc
        return self.userHistoryRepository

    def getInternalUserRepository(self) -> InternalUserRepository:
        if (self.internalUserRepository is None):
            try:
                self.internalUserRepository = InternalUserRepository(self.internalEngine)
            except Exception as exc:
                logging.warning("Failled to initialize User Repository")
                raise FailedInitialization("UserRepository") from exc
        return self.internalUserRepository

    def getExternalUserRepository(self) -> ExternalUserRepository:
        if (self.externalUserRepository is None):
            try:
                self.externalUserRepository = ExternalUserRepository(self.externalEngine)
            except Exception as exc:
                logging.warning("Failled to initialize User Repository")
                raise FailedInitialization("UserRepository") from exc
        return self.externalUserRepository

    def getWeatherRepository(self) -> WeatherRepository:
        if (self.weatherRepository is None):
            try:
                self.weatherRepository = WeatherRepository(self.internalEngine)
            except Exception as exc:
                logging.warning("Failled to initialize Weather Repository")
                raise FailedInitialization("WeatherRepository") from exc
        return self.weatherRepository
    
    def getModelRepository(self) -> ModelDataRepository:
        if (self.modelDataRepository is None):
            try:
                self.modelDataRepository = ModelDataRepository(self.internalEngine)
            except Exception as exc:
                logging.warning("Failled to initialize Weather Repository")
                raise FailedInitialization("WeatherRepository") from exc
        return self.modelDataRepository

    @classmethod
    def getInstance(cls):
        if (cls.instance is None):
            try:
                cls.instance = RepositoryFactory(InternalDatabaseConfiguration.getEngine(), ExternalDatabaseConfiguration.getEngine())
            except Exception as exc:
                logging.warning("Failled to initialize Weather Repository")
                raise FailedInitialization("WeatherRepository") from exc
        return cls.instance

