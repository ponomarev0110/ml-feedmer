from datetime import datetime
import time
import logging

from factory.service_factory import ServiceFactory

#TODO add Django endpoint
class CalculateController:

    #TODO endpoint(/calculate)
    def calculate_endpoint(self):
        logging.basicConfig(format='%(asctime)s:%(levelname)s:%(message)s', encoding='utf-8', level=logging.DEBUG)
        date = datetime.today()
        logging.info(f"Current date: {date}")
        start_time = time.time()
        serviceFactory = ServiceFactory.getInstance()
        userService = serviceFactory.getUserService()
        logging.info("Updating user data")
        userService.updateUsers()
        logging.info("Building History")
        userService.buildUserHistory(date = date)
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
        predictionService.predict_all(date)
        logging.info(f"Finished work in {time.time() - start_time} seconds")
