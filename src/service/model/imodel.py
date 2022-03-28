import abc

class IModelService(abc.ABC):

    @abc.abstractclassmethod
    def predict_user_date(self, userid, date):
        pass

    @abc.abstractclassmethod
    def predict_date(self, date):
        pass