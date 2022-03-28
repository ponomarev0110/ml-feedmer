import requests
import urllib.parse
import asyncio
import traceback
import json

from sqlalchemy.engine import Engine
from sqlalchemy.sql import text

import time
from datetime import datetime as dt

import pandas as pd
import osmnx as ox

class UserStatisticsRepository:
    def __init__(self, engine : Engine) -> None:
        self.engine = engine

    def recalculateStats(self):
        self.engine.execute('''
        INSERT INTO user_statistics 
        (
            SELECT 
            user_history.userid,
            user_history.strdate,
            AVG(user_history.price)
            OVER(PARTITION BY user_history.userid ORDER BY userid, user_history.strdate ROWS BETWEEN 60 PRECEDING AND 1 PRECEDING)
            AS average_order_price_60,

            COUNT(user_history.price)
            OVER(PARTITION BY user_history.userid ORDER BY userid, user_history.strdate ROWS BETWEEN 60 PRECEDING AND 1 PRECEDING)
            AS average_order_count_60,

            AVG(user_history.price)
            OVER(PARTITION BY user_history.userid ORDER BY userid, user_history.strdate ROWS BETWEEN 14 PRECEDING AND 1 PRECEDING)
            AS average_order_price_14,

            COUNT(user_history.price)
            OVER(PARTITION BY user_history.userid ORDER BY userid, user_history.strdate ROWS BETWEEN 14 PRECEDING AND 1 PRECEDING)
            AS average_order_count_14,

            AVG(user_history.price)
            OVER(PARTITION BY user_history.userid ORDER BY userid, user_history.strdate ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING)
            AS average_order_price_7,

            COUNT(user_history.price)
            OVER(PARTITION BY user_history.userid ORDER BY userid, user_history.strdate ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING)
            AS average_order_count_7,

            AVG(
                CASE 
                    WHEN user_history.hasOrdered then 1
                    ELSE 0
                END
                )
            OVER(
                PARTITION BY user_history.userid 
                ORDER BY user_history.strdate asc 
                RANGE BETWEEN UNBOUNDED PRECEDING AND '1 day' PRECEDING
            )	as order_frequency

            from user_history
            ORDER BY userid, user_history.strdate
        )
        ON CONFLICT (userid, strdate)
        DO UPDATE
        SET 
        average_order_price_60 = EXCLUDED.average_order_price_60,
        average_order_count_60 = EXCLUDED.average_order_count_60,
        average_order_price_14 = EXCLUDED.average_order_price_14,
        average_order_count_14 = EXCLUDED.average_order_count_14,
        average_order_price_7 = EXCLUDED.average_order_price_7,
        average_order_count_7 = EXCLUDED.average_order_count_7,
        order_frequency = EXCLUDED.order_frequency
    ''')

    def days_since_last_order(self):
        engine = self.engine
        data = engine.execute('''
        SELECT userid, strdate, hasOrdered
        FROM public.user_history
        ORDER BY (userid, strdate)
        ''').fetchall()
        data = pd.DataFrame(data = data, columns=['userid', 'strdate', 'hasOrdered'])
        data['strdate'] = pd.to_datetime(data['strdate'], infer_datetime_format=True, format = "%Y-%m-%d", errors='coerce')
        for i in data['userid'].unique():
            persona = data[data['userid'] == i]
            period = None
            query = text("UPDATE public.user_statistics SET days_since_last_order = :period WHERE userid = :userid and strdate = :strdate;")
            values = []
            for _, j in persona.iterrows():
                strdate = j['strdate']
                values.append({'period' : period, 'userid' : int(i), 'strdate' : strdate})
                if j['hasOrdered'] == True:
                    period = 0
                    period += 1
            engine.execute(query, values)