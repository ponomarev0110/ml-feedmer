import os

from catboost import CatBoostClassifier

def load_model(path, userid):
    model = CatBoostClassifier()
    model.load_model(os.path.join(path, str(userid)))
    return model