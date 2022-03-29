from datetime import datetime
import logging

from factory.service_factory import ServiceFactory
from service.user import UserService

if __name__ == "__main__":
    logging.basicConfig(format='%(asctime)s:%(levelname)s:%(message)s', encoding='utf-8', level=logging.DEBUG)
    serviceFactory = ServiceFactory.getInstance()
    userService = serviceFactory.getUserService()
    logging.info("Updating user data")
    userService.updateUsers()
    logging.info("Building History")
    userService.buildUserHistory()
    logging.info("Updating statistics")
    userService.updateStatistics()
    weatherService = serviceFactory.getWeatherService()
    logging.info("Updating Weather")
    weatherService.update_weather()
    logging.info("Updating Building Types")
    userService.saveBuildingTypes()
    catboostService = serviceFactory.getCatboostModelService()
    logging.info("Training Model")
    catboostService.train()
    predictionService = serviceFactory.getPredictionService()
    predictionService.predict_all(datetime.now())

    
