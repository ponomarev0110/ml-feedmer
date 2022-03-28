from datetime import datetime
import logging
import re

p = re.compile("\A([0-1]?[0-9]|2[0-3]).[0-5][0-9]\Z")

def getDayofWeek(date):
    return date.weekday()

def getOnlyTime(time):
    if p.match(time):
        return time[0] + time[1] + ":" + time[3] + time[4]
    else:
        return None