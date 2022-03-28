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
            return User(row['userid', row['payway'], row['formaladdr']])

    def get(self, userid):
        return None

    def getBuildingCoords(self):
        data = self.engine.execute('''
        SELECT DISTINCT users.formaladdr, weather.longitude, weather.latitude
        FROM public.users
        INNER JOIN public.weather ON weather.formaladdr = users.formaladdr
        LEFT JOIN building_types ON weather.formaladdr = building_types.formaladdr
        WHERE building_types.formaladdr is NULL
        ''').fetchall()
        return data

    def saveBuildingTypes(self, values):
        query = text("INSERT INTO public.building_types(formaladdr, building_type) VALUES(:formaladdr, :building_type) ON CONFLICT (formaladdr) DO NOTHING;")
        self.engine.execute(query, values)

    def save(self, user : User):
        self.engine.execute(text('''
        INSERT INTO public.users(userid, payway, formaladdr) VALUES
        (:userid, :payway, :formaladdr)
        ON CONFLICT (userid) 
        DO UPDATE set payway = EXCLUDED.payway, formaladdr = EXCLUDED.formaladdr;
        '''),
        userid = user.userid,
        date = user.payway,
        formaladdr = user.formaladdr
        )

    def saveAll(self, users):
        self.engine.execute(text('''
        INSERT INTO public.users(userid, payway, formaladdr) VALUES
        (:userid, :payway, :formaladdr)
        ON CONFLICT (userid) 
        DO UPDATE set payway = EXCLUDED.payway, formaladdr = EXCLUDED.formaladdr;
        '''),
        users
        )

    
    