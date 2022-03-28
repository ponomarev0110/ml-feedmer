from sqlalchemy.engine import Engine
from sqlalchemy import text
import pandas as pd


from entity.user_history import UserHistory # pylint: disable=import-error

class ModelDataRepository:
    def __init__(self, engine : Engine) -> None:
        self.engine = engine
    
    def map_to_df(self, data):
        return pd.DataFrame(data = data, columns=['userid', 
                                            'strdate', 
                                            'hasOrdered', 
                                            'formaladdr', 
                                            'building_type',
                                            'payway',
                                            'price_60',
                                            'count_60',
                                            'price_14',
                                            'count_14',
                                            'price_7',
                                            'count_7',
                                            'statistics',
                                            'period',		
                                            'offer_sent',																				                      
                                            'latitude', 
                                            'longitude', 
                                            'apparenttemperaturehigh', 
                                            'apparenttemperaturelow', 
                                            'temperaturelow', 
                                            'temperaturehigh', 
                                            'cloudcover', 
                                            'humidity', 
                                            'precipintensity', 
                                            'precipprobability', 
                                            'preciptype', 
                                            'pressure', 
                                            'windspeed', 
                                            'windbearing', 
                                            'moonphase'])

    def getData(self):
        data = self.engine.execute('''
        SELECT 
            user_history.userid, 
            user_history.strdate as user_history_date,
            user_history.hasOrdered,
            users.formaladdr, 
            COALESCE(building_types.building_type, 'yes') as building_type, 
            COALESCE(users.payway, 'Not Specified'),
            COALESCE(user_statistics.average_order_price_60, 0) as average_order_price_60,
            COALESCE(user_statistics.average_order_count_60, 0) as average_order_count_60, 
            COALESCE(user_statistics.average_order_price_14, 0) as average_order_price_14, 
            COALESCE(user_statistics.average_order_count_14, 0) as average_order_count_14, 
            COALESCE(user_statistics.average_order_price_7, 0) as average_order_price_7, 
            COALESCE(user_statistics.average_order_count_7, 0) as average_order_count_7,
            COALESCE(user_statistics.order_frequency, 0) as order_frequency,
            user_statistics.days_since_last_order as days_since_last_order,
            COALESCE((received.messages > 0), FALSE) as offer_sent,
            weather.latitude, 
            weather.longitude, 
            weather.apparenttemperaturehigh, 
            weather.apparenttemperaturelow, 
            weather.temperaturelow, 
            weather.temperaturehigh, 
            weather.cloudcover, 
            weather.humidity, 
            weather.precipintensity, 
            weather.precipprobability, 
            weather.preciptype, 
            weather.pressure, 
            weather.windspeed, 
            weather.windbearing, 
            weather.moonphase
        FROM public.user_history
        LEFT JOIN public.users as users
        ON user_history.userid = users.userid
        LEFT JOIN public.weather as weather
        ON weather.formaladdr = users.formaladdr AND weather.strdate = user_history.strdate
        LEFT JOIN public.orders 
        ON user_history.userid = orders.userid AND user_history.strdate = orders.strdate
        LEFT JOIN public.building_types 
        ON building_types.formaladdr = users.formaladdr
        LEFT JOIN public.user_statistics 
        ON user_history.userid = user_statistics.userid AND user_history.strdate = user_statistics.strdate
        LEFT JOIN 
        (
            SELECT userid, strdate, count(text) as messages from sentmessages
            WHERE text LIKE 'Добрый день%%'
            GROUP BY userid, strdate
        )as received 
        ON user_history.userid = received.userid AND user_history.strdate = received.strdate
        ORDER BY (user_history.userid, user_history.strdate);
        '''
        )
        
        return self.map_to_df(data)
    
    def getDataIn(self, start, end):
        data = self.engine.execute(text('''
        SELECT 
            user_history.userid, 
            user_history.strdate as user_history_date,
            user_history.hasOrdered,
            users.formaladdr, 
            COALESCE(building_types.building_type, 'yes') as building_type, 
            COALESCE(users.payway, 'Not Specified'),
            COALESCE(user_statistics.average_order_price_60, 0) as average_order_price_60,
            COALESCE(user_statistics.average_order_count_60, 0) as average_order_count_60, 
            COALESCE(user_statistics.average_order_price_14, 0) as average_order_price_14, 
            COALESCE(user_statistics.average_order_count_14, 0) as average_order_count_14, 
            COALESCE(user_statistics.average_order_price_7, 0) as average_order_price_7, 
            COALESCE(user_statistics.average_order_count_7, 0) as average_order_count_7,
            COALESCE(user_statistics.order_frequency, 0) as order_frequency,
            user_statistics.days_since_last_order as days_since_last_order,
            COALESCE((received.messages > 0), FALSE) as offer_sent,
            weather.latitude, 
            weather.longitude, 
            weather.apparenttemperaturehigh, 
            weather.apparenttemperaturelow, 
            weather.temperaturelow, 
            weather.temperaturehigh, 
            weather.cloudcover, 
            weather.humidity, 
            weather.precipintensity, 
            weather.precipprobability, 
            weather.preciptype, 
            weather.pressure, 
            weather.windspeed, 
            weather.windbearing, 
            weather.moonphase
        FROM public.user_history
        LEFT JOIN public.users as users
        ON user_history.userid = users.userid
        LEFT JOIN public.weather as weather
        ON weather.formaladdr = users.formaladdr AND weather.strdate = user_history.strdate
        LEFT JOIN public.orders 
        ON user_history.userid = orders.userid AND user_history.strdate = orders.strdate
        LEFT JOIN public.building_types 
        ON building_types.formaladdr = users.formaladdr
        LEFT JOIN public.user_statistics 
        ON user_history.userid = user_statistics.userid AND user_history.strdate = user_statistics.strdate
        LEFT JOIN 
        (
            SELECT userid, strdate, count(text) as messages from sentmessages
            WHERE text LIKE 'Добрый день%%'
            GROUP BY userid, strdate
        )as received 
        ON user_history.userid = received.userid AND user_history.strdate = received.strdate
        WHERE user_history.strdate BETWEEN :start AND :end
        ORDER BY (user_history.userid, user_history.strdate);
        '''),
        start = start,
        end = end
        )
        
        return self.map_to_df(data)

    def getDataForUser(self, userid):
        data = self.engine.execute(text('''
        SELECT 
            user_history.userid, 
            user_history.strdate as user_history_date,
            user_history.hasOrdered,
            users.formaladdr, 
            COALESCE(building_types.building_type, 'yes') as building_type, 
            COALESCE(users.payway, 'Not Specified'),
            COALESCE(user_statistics.average_order_price_60, 0) as average_order_price_60,
            COALESCE(user_statistics.average_order_count_60, 0) as average_order_count_60, 
            COALESCE(user_statistics.average_order_price_14, 0) as average_order_price_14, 
            COALESCE(user_statistics.average_order_count_14, 0) as average_order_count_14, 
            COALESCE(user_statistics.average_order_price_7, 0) as average_order_price_7, 
            COALESCE(user_statistics.average_order_count_7, 0) as average_order_count_7,
            COALESCE(user_statistics.order_frequency, 0) as order_frequency,
            user_statistics.days_since_last_order as days_since_last_order,
            COALESCE((received.messages > 0), FALSE) as offer_sent,
            weather.latitude, 
            weather.longitude, 
            weather.apparenttemperaturehigh, 
            weather.apparenttemperaturelow, 
            weather.temperaturelow, 
            weather.temperaturehigh, 
            weather.cloudcover, 
            weather.humidity, 
            weather.precipintensity, 
            weather.precipprobability, 
            weather.preciptype, 
            weather.pressure, 
            weather.windspeed, 
            weather.windbearing, 
            weather.moonphase
        FROM public.user_history
        LEFT JOIN public.users as users
        ON user_history.userid = users.userid
        LEFT JOIN public.weather as weather
        ON weather.formaladdr = users.formaladdr AND weather.strdate = user_history.strdate
        LEFT JOIN public.orders 
        ON user_history.userid = orders.userid AND user_history.strdate = orders.strdate
        LEFT JOIN public.building_types 
        ON building_types.formaladdr = users.formaladdr
        LEFT JOIN public.user_statistics 
        ON user_history.userid = user_statistics.userid AND user_history.strdate = user_statistics.strdate
        LEFT JOIN 
        (
            SELECT userid, strdate, count(text) as messages from sentmessages
            WHERE text LIKE 'Добрый день%%'
            GROUP BY userid, strdate
        )as received 
        ON user_history.userid = received.userid AND user_history.strdate = received.strdate
        WHERE user_history.userid = :userid
        ORDER BY (user_history.userid, user_history.strdate);
        '''),
        userid = userid
        )
        
        return self.map_to_df(data)

    def getDataForUserIn(self, userid, start, end):
        data = self.engine.execute(text('''
        SELECT 
            user_history.userid, 
            user_history.strdate as user_history_date,
            user_history.hasOrdered,
            users.formaladdr, 
            COALESCE(building_types.building_type, 'yes') as building_type, 
            COALESCE(users.payway, 'Not Specified'),
            COALESCE(user_statistics.average_order_price_60, 0) as average_order_price_60,
            COALESCE(user_statistics.average_order_count_60, 0) as average_order_count_60, 
            COALESCE(user_statistics.average_order_price_14, 0) as average_order_price_14, 
            COALESCE(user_statistics.average_order_count_14, 0) as average_order_count_14, 
            COALESCE(user_statistics.average_order_price_7, 0) as average_order_price_7, 
            COALESCE(user_statistics.average_order_count_7, 0) as average_order_count_7,
            COALESCE(user_statistics.order_frequency, 0) as order_frequency,
            user_statistics.days_since_last_order as days_since_last_order,
            COALESCE((received.messages > 0), FALSE) as offer_sent,
            weather.latitude, 
            weather.longitude, 
            weather.apparenttemperaturehigh, 
            weather.apparenttemperaturelow, 
            weather.temperaturelow, 
            weather.temperaturehigh, 
            weather.cloudcover, 
            weather.humidity, 
            weather.precipintensity, 
            weather.precipprobability, 
            weather.preciptype, 
            weather.pressure, 
            weather.windspeed, 
            weather.windbearing, 
            weather.moonphase
        FROM public.user_history
        LEFT JOIN public.users as users
        ON user_history.userid = users.userid
        LEFT JOIN public.weather as weather
        ON weather.formaladdr = users.formaladdr AND weather.strdate = user_history.strdate
        LEFT JOIN public.orders 
        ON user_history.userid = orders.userid AND user_history.strdate = orders.strdate
        LEFT JOIN public.building_types 
        ON building_types.formaladdr = users.formaladdr
        LEFT JOIN public.user_statistics 
        ON user_history.userid = user_statistics.userid AND user_history.strdate = user_statistics.strdate
        LEFT JOIN 
        (
            SELECT userid, strdate, count(text) as messages from sentmessages
            WHERE text LIKE 'Добрый день%%'
            GROUP BY userid, strdate
        )as received 
        ON user_history.userid = received.userid AND user_history.strdate = received.strdate
        WHERE user_history.userid = :userid
        AND user_history.strdate BETWEEN :start AND :end
        ORDER BY (user_history.userid, user_history.strdate);
        '''),
        start = start,
        end = end,
        userid = userid
        )

        return self.map_to_df(data)
    
    def getDataForUserFor(self, userid, date):
        data = self.engine.execute(text('''
        SELECT 
            user_history.userid, 
            user_history.strdate as user_history_date,
            user_history.hasOrdered,
            users.formaladdr, 
            COALESCE(building_types.building_type, 'yes') as building_type, 
            COALESCE(users.payway, 'Not Specified'),
            COALESCE(user_statistics.average_order_price_60, 0) as average_order_price_60,
            COALESCE(user_statistics.average_order_count_60, 0) as average_order_count_60, 
            COALESCE(user_statistics.average_order_price_14, 0) as average_order_price_14, 
            COALESCE(user_statistics.average_order_count_14, 0) as average_order_count_14, 
            COALESCE(user_statistics.average_order_price_7, 0) as average_order_price_7, 
            COALESCE(user_statistics.average_order_count_7, 0) as average_order_count_7,
            COALESCE(user_statistics.order_frequency, 0) as order_frequency,
            user_statistics.days_since_last_order as days_since_last_order,
            COALESCE((received.messages > 0), FALSE) as offer_sent,
            weather.latitude, 
            weather.longitude, 
            weather.apparenttemperaturehigh, 
            weather.apparenttemperaturelow, 
            weather.temperaturelow, 
            weather.temperaturehigh, 
            weather.cloudcover, 
            weather.humidity, 
            weather.precipintensity, 
            weather.precipprobability, 
            weather.preciptype, 
            weather.pressure, 
            weather.windspeed, 
            weather.windbearing, 
            weather.moonphase
        FROM public.user_history
        LEFT JOIN public.users as users
        ON user_history.userid = users.userid
        LEFT JOIN public.weather as weather
        ON weather.formaladdr = users.formaladdr AND weather.strdate = user_history.strdate
        LEFT JOIN public.orders 
        ON user_history.userid = orders.userid AND user_history.strdate = orders.strdate
        LEFT JOIN public.building_types 
        ON building_types.formaladdr = users.formaladdr
        LEFT JOIN public.user_statistics 
        ON user_history.userid = user_statistics.userid AND user_history.strdate = user_statistics.strdate
        LEFT JOIN 
        (
            SELECT userid, strdate, count(text) as messages from sentmessages
            WHERE text LIKE 'Добрый день%%'
            GROUP BY userid, strdate
        )as received 
        ON user_history.userid = received.userid AND user_history.strdate = received.strdate
        WHERE user_history.userid = :userid
        AND user_history.strdate = :date
        ORDER BY (user_history.userid, user_history.strdate);
        '''),
        date = date,
        userid = userid
        )

        return self.map_to_df(data)
    
    
    def getDataForUsersFor(self, date):
        data = self.engine.execute(text('''
        SELECT 
            user_history.userid, 
            user_history.strdate as user_history_date,
            user_history.hasOrdered,
            users.formaladdr, 
            COALESCE(building_types.building_type, 'yes') as building_type, 
            COALESCE(users.payway, 'Not Specified'),
            COALESCE(user_statistics.average_order_price_60, 0) as average_order_price_60,
            COALESCE(user_statistics.average_order_count_60, 0) as average_order_count_60, 
            COALESCE(user_statistics.average_order_price_14, 0) as average_order_price_14, 
            COALESCE(user_statistics.average_order_count_14, 0) as average_order_count_14, 
            COALESCE(user_statistics.average_order_price_7, 0) as average_order_price_7, 
            COALESCE(user_statistics.average_order_count_7, 0) as average_order_count_7,
            COALESCE(user_statistics.order_frequency, 0) as order_frequency,
            user_statistics.days_since_last_order as days_since_last_order,
            COALESCE((received.messages > 0), FALSE) as offer_sent,
            weather.latitude, 
            weather.longitude, 
            weather.apparenttemperaturehigh, 
            weather.apparenttemperaturelow, 
            weather.temperaturelow, 
            weather.temperaturehigh, 
            weather.cloudcover, 
            weather.humidity, 
            weather.precipintensity, 
            weather.precipprobability, 
            weather.preciptype, 
            weather.pressure, 
            weather.windspeed, 
            weather.windbearing, 
            weather.moonphase
        FROM public.user_history
        LEFT JOIN public.users as users
        ON user_history.userid = users.userid
        LEFT JOIN public.weather as weather
        ON weather.formaladdr = users.formaladdr AND weather.strdate = user_history.strdate
        LEFT JOIN public.orders 
        ON user_history.userid = orders.userid AND user_history.strdate = orders.strdate
        LEFT JOIN public.building_types 
        ON building_types.formaladdr = users.formaladdr
        LEFT JOIN public.user_statistics 
        ON user_history.userid = user_statistics.userid AND user_history.strdate = user_statistics.strdate
        LEFT JOIN 
        (
            SELECT userid, strdate, count(text) as messages from sentmessages
            WHERE text LIKE 'Добрый день%%'
            GROUP BY userid, strdate
        )as received 
        ON user_history.userid = received.userid AND user_history.strdate = received.strdate
        WHERE user_history.strdate = :date
        ORDER BY (user_history.userid, user_history.strdate);
        '''),
        date = date
        )

        return self.map_to_df(data)
      