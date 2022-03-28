import logging
import datetime

from apscheduler.schedulers.blocking import BlockingScheduler

from factory.service_factory import ServiceFactory

sched = BlockingScheduler()

@sched.scheduled_job('cron', day_of_week='mon-fri', hour=4)
def scheduled_job():
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
    weatherService.update_weather(limit = 5)
    logging.info("Updating Building Types")
    userService.saveBuildingTypes()
    catboostService = serviceFactory.getCatboostModelService()
    logging.info("Training Model")
    catboostService.train()
    predictionService = serviceFactory.getPredictionService()
    logging.info(predictionService.predict_user_date(390, datetime.fromisoformat("2021-10-01")))


sched.start()