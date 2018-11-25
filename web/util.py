import hashlib
import json

from sqlalchemy import engine

MYSQL_URL = 'wdias:wdias123@adapter-extension-mysql.default.svc.cluster.local/metadata'
DB_ENGINES = {}


def get_engine(db_name) -> engine:
    import sqlalchemy
    if db_name not in DB_ENGINES:
        DB_ENGINES[db_name] = sqlalchemy.create_engine('mysql+mysqlconnector://' + MYSQL_URL, pool_pre_ping=True, pool_size=4, pool_recycle=600)
    return DB_ENGINES[db_name]


def get_timeseries(moduleId, valueType, parameterId, locationId, timeseriesType, timeStepId, *args, **kwargs):
    timeseries = {
        'moduleId': moduleId,
        'valueType': valueType,
        'parameterId': parameterId,
        'locationId': locationId,
        'timeseriesType': timeseriesType,
        'timeStepId': timeStepId,
    }
    payload = json.dumps(timeseries, separators=(',', ':')).encode('ascii')
    timeseries['timeseriesId'] = hashlib.sha256(payload).hexdigest()
    return timeseries
