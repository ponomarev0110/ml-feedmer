import abc

class IModelService(abc.ABC):

    @abc.abstractclassmethod
    def predict_user_date(self, userid, date):
        pass