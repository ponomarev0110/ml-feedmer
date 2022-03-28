import logging
from sqlalchemy.engine import Engine
from sqlalchemy import text, event

from entity.user_history import UserHistory # pylint: disable=import-error

import pandas as pd
import numpy as np
import datetime as dt

class UserHistoryRepository:
    def __init__(self, engine : Engine) -> None:
        self.engine = engine

    def rowmapper(self, row):
        if row is None:
            return None
        else:
            return UserHistory(row['userid'], row['order_date'], row['hasordered'], row['final_price'])

    def save(self, entry : UserHistory):
        entry = self.engine.execute(text('''
        INSERT INTO public.user_history(userid, order_date, hasordered, final_price) VALUES
        (:userid, :date, :hasordered, :final_price)
        ON CONFLICT (userid, order_date) 
        DO UPDATE set hasOrdered = EXCLUDED.hasOrdered final_price = EXCLUDED.final_price;
        '''),
        userid = entry.userid,
        date = entry.date,
        hasordered = entry.hasOrdered,
        final_price = entry.finalPrice
        )

    def save_data(self, data):
        query = text("""
        INSERT INTO public.user_history(userid, strdate, hasOrdered, price, messages) 
        VALUES(:userid, :strdate, :hasOrdered, :price, :messages) 
        ON CONFLICT (userid, strdate) 
        DO UPDATE set hasOrdered = EXCLUDED.hasOrdered;""")
        self.engine.execute(query, data)

    def get(self, userid, date):
        entry = self.engine.execute('''
        SELECT userid, order_date, hasordered
	    FROM public.user_history
        Where userid = :userid AND date = :date;
        ''',
        userid = userid,
        date = date
        ).fetchone()
        return self.rowmapper(entry)
    
    def getUserEntriesIn(self, userid, start, end):
        data = self.engine.execute('''
        SELECT userid, order_date, hasordered
	    FROM public.user_history
        Where userid = :userid AND date BETWEEN :start AND :end;
        ''',
        userid = userid,
        start = start,
        end = end
        ).fetchall()
        return map(self.rowmapper, data)
    
    def getEntriesWithoutWeather(self):
        data = self.engine.execute('''
        SELECT DISTINCT users.formaladdr, user_history.strdate
        FROM public.user_history
        INNER JOIN public.users ON user_history.userid = users.userid
        LEFT JOIN public.weather ON weather.formaladdr = users.formaladdr AND weather.strdate = user_history.strdate
        WHERE weather.formaladdr IS NULL
        ''').fetchall()
        return map(self.rowmapper, data)
        