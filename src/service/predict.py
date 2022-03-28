from service.model.imodel import IModelService
from service.user import UserService


class PredictionService:
    def __init__(self, modelService : IModelService, userSerivce: UserService) -> None:
        self.modelService = modelService
        self.userService = userSerivce

    def predict(self, userid, date):
        prediction = self.modelService.predict_user_date(userid, date)
        self.userService.savePrediction(userid, date, prediction)
        return prediction

    def predict_all(self, date):
        predictions = self.modelService.predict_date(date)
        self.userService.savePrediction(predictions)
        return predictions


    