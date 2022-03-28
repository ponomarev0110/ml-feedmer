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
        SELECT orders.userid, orders.strdate, prc.prcsum, COALESCE(received.messages, 0) as messages
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
        LEFT JOIN 
        (
            SELECT userid, strdate, count(text) as messages from sentmessages
            WHERE text LIKE 'Добрый день%%'
            GROUP BY userid, strdate
        ) as received 
        ON orders.userid = received.userid AND orders.strdate = received.strdate;
        ''').fetchall()
        orders = pd.DataFrame(data = data, columns=['userid', 'strdate', 'price', 'messages'])
        orders['messages'] = orders['messages'].astype(int)
        orders['strdate'] = pd.to_datetime(orders['strdate'], infer_datetime_format=True, format = "%d.%m.%Y", errors='coerce')
        return orders

    def savePrediction(self, values):
        self.engine.execute(
            text('''
            INSERT INTO public.predictions(userid, strdate, orderProbability)
            VALUES(:userid, :date, :prediction)
            ON CONFLICT (userid, strdate) 
            DO UPDATE SET orderProbability = EXCLUDED.orderProbability;
            '''),
            values
        )
    
    