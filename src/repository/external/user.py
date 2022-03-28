from sqlalchemy.engine import Engine
from sqlalchemy import text
from entity.user import User
from typing import List

import pandas as pd


class UserRepository:
    def __init__(self, engine : Engine) -> None:
        self.engine = engine

    def rowmapper(self, row):
        if row is None:
            return None
        else:
            return {'userid' : row['userid'], 'payway' : row['payway'], 'formaladdr' : row['formaladdr']}

    def getAll(self):
        data = self.engine.execute('''
        SELECT users.userid, users.formaladdr, users.payway
        FROM public.users
        ''').fetchall()
        users = []
        for row in data:
            users.append(self.rowmapper(row))
        return users

    def getOrderData(self):
        data = self.engine.execute('''
        SELECT orders.userid, orders.strdate, prc.prcsum
        FROM public.orders
        LEFT JOIN
        (
            SELECT answ.userid, answ.strdate, SUM(prclst.price) as prcsum
            FROM public.answers as answ
            INNER JOIN public.pricelist as prclst 
            ON answ.text = prclst.name
            GROUP BY (answ.userid, answ.strdate)
        ) as prc 
        ON prc.userid = orders.userid AND prc.strdate = orders.strdate
        ''').fetchall()
        orders = pd.DataFrame(data = data, columns=['userid', 'strdate', 'price'])
        orders['strdate'] = pd.to_datetime(orders['strdate'], infer_datetime_format=True, format = "%d.%m.%Y", errors='coerce')
        return orders

    def savePrediction(self, values):
        self.engine.execute(
            text('''
            INSERT INTO predictions(userid, strdate, prediction)
            VALUES(:userid, :date, :prediction)
            '''),
            values
        )
    
    