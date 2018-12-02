import requests
import json

from sqlalchemy import engine

METADATA_ADAPTER_URL = 'http://adapter-metadata.default.svc.cluster.local'
MYSQL_URL = 'wdias:wdias123@adapter-extension-mysql.default.svc.cluster.local/extension'
DB_ENGINES = {}


def get_engine(db_name) -> engine:
    import sqlalchemy
    if db_name not in DB_ENGINES:
        DB_ENGINES[db_name] = sqlalchemy.create_engine('mysql+mysqlconnector://' + MYSQL_URL, pool_pre_ping=True, pool_size=4, pool_recycle=600)
    return DB_ENGINES[db_name]


def create_timeseries(variables_data):
    variables = []
    variable_names = []
    s = requests.session()
    for v in variables_data:
        assert 'variableId' in v, f'Each variable should have a variableId'
        assert 'metadata' in v or 'metadataIds' in v or 'timeseriesId' in v, \
            f'Each variable should have a metadata or a metadataIds or a timeseriesId'
        t = None
        if 'metadata' in v:
            print('metadata:', v['metadata'])
            res = s.post(f'{METADATA_ADAPTER_URL}/timeseries', json=v['metadata'])
            print(res.status_code, res.text)
            assert res.status_code is 200, f'Unable to create timeseries for {v["variableId"]}'
            t = json.loads(res.text)
        elif 'metadataIds' in v:
            res = s.post(f'{METADATA_ADAPTER_URL}/timeseries', json=v['metadataIds'])
            assert res.status_code is 200, f'Unable to create timeseries for {v["variableId"]}'
            t = json.loads(res.text)
        if 'timeseriesId' in v:
            res = s.get(f'{METADATA_ADAPTER_URL}/timeseries/{v["timeseriesId"]}')
            assert res.status_code is 200, f'Unable to find timeseries {v["timeseriesId"]} for {v["variableId"]}'
            t = json.loads(res.text)

        variables.append({
            'variableId': v['variableId'],
            'timeseries': t
        })
        variable_names.append(v['variableId'])
    return variables, variable_names
