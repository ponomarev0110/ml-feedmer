import traceback
import pandas as pd
import numpy as np

from catboost import Pool, CatBoostClassifier, cv 

from sklearn import exceptions, preprocessing
from sklearn.decomposition import PCA

from sklearn.linear_model import LogisticRegression
from sklearn.metrics import classification_report
from sklearn.model_selection import train_test_split
from sklearn.model_selection import GridSearchCV

import os
import time
from datetime import datetime as dt
import random 
import gc
import logging

from repository.internal.data import ModelDataRepository
from service.model.imodel import IModelService
from utils.time import getDayofWeek, getOnlyTime
from utils.load_model import load_model
from config.constants import Constants

class CatboostModelService(IModelService):
    NAME = "Catboost"
    LOOK_BACK = 90
    cat_features = [
        'payway',
        'preciptype',
        'dayofweek',
        'building_type',
        'offer_sent'
        ]

    def __init__(
        self,
        dataRepository: ModelDataRepository
        ) -> None:
        self.dataRepository = dataRepository

    def clear_unactive(self, data, ltou):
        for i in data['userid'].unique():
            persona = data[data['userid'] == i]
            persona = persona.iloc[::-1]
            persona = persona.reset_index(drop=True)
            flag = False
            for event in persona['hasOrdered'][persona.index < ltou]:
                if float(event) == float(1) or bool(event) == True:
                    flag = True
            if flag == False:
                index = data[data['userid']==i].index
                data = data.drop(index = index)
        return data

    def clear_with_one_cat(self, new_orders):
        for id in new_orders['userid'].unique():
            k = 0
            for i in new_orders[new_orders['userid'] == id]['hasOrdered']:
                if i == False:
                    k = k + 1
            if k == len(new_orders[new_orders['userid'] == id]):
                index = new_orders[new_orders['userid']==id].index
                new_orders = new_orders.drop(index = index)
        return new_orders

    def prepareData(self, orders):
        orders['strdate'] = pd.to_datetime(orders['strdate'], infer_datetime_format=True, format = "%Y-%m-%d", errors='coerce')
        orders.sort_values(['strdate', 'userid'])
        
        orders['dayofweek'] = orders['strdate'].apply(dt.weekday)
        logging.debug(orders['strdate'].apply(dt.weekday))
        orders = orders.dropna()
        logging.debug(orders.dtypes)
        orders = orders[orders['dayofweek'] < 5]

        orders = orders.reset_index(drop=True)

        orders['building_type'] = orders['building_type'].astype("category") 
        orders['hasOrdered'] = orders['hasOrdered'].astype("category")
        orders['payway'] = orders['payway'].astype("category")
        orders['dayofweek'] = orders['dayofweek'].astype("category")
        orders['preciptype'] = orders['preciptype'].astype("category")
        orders['offer_sent'] = orders['offer_sent'].astype("category")

        orders = orders.reset_index(drop=True)

        cat_features = self.cat_features
        cat_var = cat_features.copy()
        cat_var.append('hasOrdered')

        object_var = ['statistics',
                    'period',		
                    'apparenttemperaturehigh', 
                    'apparenttemperaturelow', 
                    'temperaturelow', 
                    'temperaturehigh', 
                    'cloudcover', 
                    'humidity', 
                    'precipintensity', 
                    'precipprobability', 
                    'pressure', 
                    'windspeed', 
                    'windbearing', 
                    'moonphase',
                    'price_60',
                    'count_60',
                    'price_14',
                    'count_14',
                    'price_7',
                    'count_7']
        for i in object_var:
            orders[i] = orders[i].astype('float')

        orders = orders.drop(['formaladdr', 'latitude', 'longitude'],axis=1).dropna()
        to_drop = [
          #  'pressure', 
           'preciptype', 
           'offer_sent',
          #  'windbearing', 
          #  'humidity',
          #  'cloudcover', 
          #  'precipintensity', 
          #  'precipprobability',
           'building_type',
          # 'moonphase',
          # 'price_60',
          # 'count_60',
          # 'price_14',
          # 'count_14',
          # 'price_7',
          # 'count_7',      
          #  'payway',
          #  'statistics',
          #  'period',
          #  'apparenttemperaturehigh',
          #  'apparenttemperaturelow',
          #  'temperaturelow',
          #  'temperaturehigh',
          #  'windspeed',
          #  'dayofweek'
            ]
        orders = orders.drop(to_drop,axis=1).dropna()
        for var in to_drop:
            try:
                object_var.remove(var)
            except:
                pass
            try:
                cat_var.remove(var)
            except:
                pass
            try:
                cat_features.remove(var)
            except:
                pass
            try:
                cats = cats.drop(var,axis=1).dropna()
            except:
                pass
        orders = orders.dropna()
        orders = orders.reset_index(drop=True)
        logging.info(orders.head())
        return orders


    def train(self):
        data = self.dataRepository.getData()
        data = self.prepareData(data)
        train = self.clear_unactive(data, self.LOOK_BACK)
        train = self.clear_with_one_cat(train).dropna().reset_index(drop=True)
        for id in train['userid'].unique():
            if not train[train['userid']==id].empty:
                user = train[train['userid']==id]
                x = user.drop(['userid','strdate','hasOrdered'], axis=1)
                y = user['hasOrdered']
                train_data = Pool(data=x,
                                label=y.astype('string'),
                                cat_features=self.cat_features)
                model = None
                model = CatBoostClassifier(iterations=30,
                                    learning_rate=0.1,
                                    depth=2,
                                    custom_metric=['Logloss',
                                                    'AUC:hints=skip_train~false'])
                try:
                    model.fit(train_data, verbose=False)
                    path = os.path.join(Constants.MODEL_PATH, self.NAME, str(id))
                    model.save_model(path)
                except Exception as exc:
                    logging.info(f"Пропущен клиент № {id}")
                    logging.debug(exc)

    def predict_user_date(self, userid, date):
        try:
            model = load_model(os.path.join(Constants.MODEL_PATH, self.NAME), userid)
            data = self.dataRepository.getDataForUserFor(userid, date)
            orders = self.prepareData(data)
            x = orders.drop(['userid','strdate','hasOrdered'], axis=1)
            y = orders['hasOrdered']
            pred = model.predict(x)
            logging.debug(pred, y)
            return pred
        except Exception as exc:
            logging.info(f"Couldn't find a model for {userid}")
            logging.debug(exc)
            logging.debug(traceback.format_exc())
            
    def predict_date(self, date):
            data = self.dataRepository.getDataForUsersFor(date)
            orders = self.prepareData(data)
            result = []
            for id in orders['userid'].unique():        
                try:
                    model = load_model(os.path.join(Constants.MODEL_PATH, self.NAME), id)
                    user = orders[orders['userid']==id]
                    x = user.drop(['userid','strdate','hasOrdered'], axis=1)
                    y = user['hasOrdered']
                    pred = model.predict(x)
                    logging.debug(pred, y)
                    result.append({
                        'userid' : id, 
                        'date' : date.strftime("%d.%m.%Y"), 
                        'prediction' : pred
                    })
                except Exception as exc:
                    logging.info(f"Couldn't find a model for {id}")
                    logging.debug(exc)
                    logging.debug(traceback.format_exc())
                    result.append({
                        'userid' : id, 
                        'date' : date.strftime("%d.%m.%Y"), 
                        'prediction' : None
                    })