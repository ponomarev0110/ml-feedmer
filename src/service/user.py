import datetime as dt
import logging
import traceback
import asyncio
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
import numpy as np

import osmnx as ox
from config.executor import ThreadPoolConfig

from repository.internal.user import UserRepository as ExternalUserRepository
from repository.external.user import UserRepository as InternalUserRepository
from repository.internal.user_statistics import UserStatisticsRepository
from repository.internal.user_history import UserHistoryRepository
from utils.background import background

class UserService:
    def __init__(
        self, 
        externalUserRepository : ExternalUserRepository, 
        internalUserRepository : InternalUserRepository, 
        userStatisticsRepository : UserStatisticsRepository,
        userHistoryRepository : UserHistoryRepository
        ) -> None:
        self.externalUserRepository = externalUserRepository
        self.internalUserRepository = internalUserRepository
        self.userStatisticsRepository = userStatisticsRepository
        self.userHistoryRepository = userHistoryRepository

    def insert_user_orders(self, userid, startdate, enddate, orders_true):
        try:
            date_s = startdate
            values = []
            while date_s <= enddate:  
                hasOrdered = not orders_true.loc[(orders_true['userid'] == userid) & (orders_true['strdate'] == date_s)].empty
                price = None
                messages = None
                if hasOrdered:
                    price = orders_true.loc[(orders_true['userid'] == userid) & (orders_true['strdate'] == date_s), ['price']]
                    price = price.values[0][0]
                    messages = orders_true.loc[(orders_true['userid'] == userid) & (orders_true['strdate'] == date_s), ['messages']]
                    messages = int(messages.values[0][0])
                values.append({'userid' : userid, 'strdate' : date_s, 'hasOrdered': hasOrdered, 'price' : price, 'messages' : messages})
                date_s += dt.timedelta(days=1)
            self.userHistoryRepository.save_data(values)
        except Exception as exc:
            logging.warning(f'Failed building history for {userid}: {exc}')
            logging.debug(traceback.format_exc())
        else:
            logging.debug(f'Finished building history for id:{userid}')

    def buildUserHistory(self, date = dt.datetime.now()):
        orders = self.externalUserRepository.getOrderData()
        orders['strdate'] = pd.to_datetime(orders['strdate'], infer_datetime_format=True, format = "%d.%m.%Y", errors='coerce')
        orders_dates = orders.groupby("userid")
        orders_dates = orders_dates.strdate.agg(startdate = np.min, enddate = np.max)
        with ThreadPoolExecutor(max_workers=10) as executor:
            for userid, row in orders_dates.iterrows():
                future = executor.submit(self.insert_user_orders, userid, row['startdate'], max(row['enddate'], date), orders)
        logging.debug("Finished building user history")
        

    def updateUsers(self):
        users = self.externalUserRepository.getAll()
        self.internalUserRepository.saveAll(users)

    def updateStatistics(self):
        logging.debug("Recalculating statistics")
        self.userStatisticsRepository.recalculateStats()
        logging.debug("Calculating days since last order")
        self.userStatisticsRepository.days_since_last_order()

    def saveBuildingTypes(self):
        data = self.internalUserRepository.getBuildingCoords()
        logging.info(f'Inserting {len(data)} objects')
        types = []
        for row in data:
            addr = row['formaladdr'].split(' ')
            try:
                logging.info(f"???????????? ???????????????? {row['formaladdr']}, ({row['latitude']}, {row['longitude']})")
                buildings = ox.geometries.geometries_from_point((float(row['latitude']), float(row['longitude'])), tags = {'building': True} , dist=100)
                for i in buildings.iterrows():
                    building = i[1]
                    try:
                        if building['addr:housenumber'] == addr[-1]:
                            types.append({'formaladdr' : row['formaladdr'], 'building_type' : building['building']})
                            break
                    except Exception as exc:
                        logging.debug(exc)
                        pass
            except Exception as exc:
                logging.warning(exc)
                logging.warning(traceback.format_exc())
                break
        if types:
            self.internalUserRepository.saveBuildingTypes(types)

    def savePrediction(self, userid, date, prediction):
        self.externalUserRepository.savePrediction([
            {
                'userid' : userid, 
                'date' : date.strftime("%d.%m.%Y"), 
                'prediction' : float(prediction[1])
            }
        ])
    
    def savePredictions(self, predictions):
        if predictions:
            for i in predictions:
                i['userid'] = int(i['userid'])
                i['date'] = i['date'].strftime("%d.%m.%Y")
                if i['prediction'] is not None:
                    i['prediction'] = float(i['prediction'][1])
            self.externalUserRepository.savePrediction(predictions)