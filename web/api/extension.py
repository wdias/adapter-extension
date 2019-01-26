from flask import Blueprint, request, jsonify, json
from sqlalchemy import text as sql
from typing import List

from web import util
from web.api import trigger
from web.cache import Cache, TriggerScheduler

bp = Blueprint('timeseries', __name__)
ENGINE = util.get_engine('metadata')
CACHE = Cache()
SCHEDULER = TriggerScheduler()

""" Extension Structure:
{
    extensionId: "",
    extension: enum("Transformation", "Validation", "Interpolation"),
    function: "",
    variables: [
        {
            variableId: "",
            metadata: {},
        }, {
            variableId: "",
            metadataIds: {},
        }, {
            variableId: "",
            timeseriesId: {},
        }
    ],
    inputVariables: [],
    outputVariables: [],
    trigger: [
        {
            trigger_type: enum("OnChange", "OnTime"),
            trigger_on: []
        }
    ],
    options: {}
}
"""
@bp.route('/extension', methods=['POST'])
def extension_create():
    data = request.get_json()
    assert 'extensionId' in data, f'extensionId should be provided'
    print('POST extension:', data['extensionId'])
    assert 'variables' in data and isinstance(data['variables'], list), f'variables should be provided'
    data['variables'], variable_names = util.create_timeseries(data['variables'])
    if 'inputVariables' in data:
        for v in data['inputVariables']:
            assert v in variable_names, f'{v} is not defined in variables'
    if 'outputVariables' in data:
        for v in data['outputVariables']:
            assert v in variable_names, f'{v} is not defined in variables'
    data['data'] = dumps_data(data['variables'], data['inputVariables'], data['outputVariables'])
    if 'options' not in data:
        data['options'] = '{}'
    else:
        data['options'] = json.dumps(data['options'])
    assert 'trigger' in data and isinstance(data['trigger'], list), f'trigger list should be provided'

    with ENGINE.begin() as conn:
        trigger.extension_trigger_create(conn, data['extensionId'], data['trigger'])
        conn.execute(sql('''
            INSERT IGNORE INTO extensions (extensionId, extension, function, data, options)
            VALUES (:extensionId, :extension, :function, :data, :options)
        '''), **data)
        for t in data['trigger']:
            if t['trigger_type'] == 'OnChange':
                CACHE.hset_pipe_on_change_timeseries_extension_by_ids(t['trigger_on'], **data)
            elif t['trigger_type'] == 'OnTime':
                SCHEDULER.add_to_scheduler(t['trigger_on'], **data)
        del data['data']
        data['options'] = json.loads(data['options'])
        return jsonify(data)


@bp.route('/extension/<extension_id>', methods=['GET'])
def extension_get(extension_id):
    print('GET extension:', extension_id)
    extension = CACHE.get(extension_id)
    if extension is None:
        extension = ENGINE.execute(sql('''
            SELECT extensionId, extension, function, `data`, options
            FROM extensions WHERE extensionId=:extension_id
        '''), extension_id=extension_id).fetchone()
        assert extension, f'Extension does not exists: {extension_id}'
        extension = dict(extension)
        data = json.loads(extension['data'])
        extension['trigger'] = trigger.extension_trigger_get(ENGINE, extension_id)
        extension['variables'] = data['variables']
        extension['inputVariables'] = data['inputVariables']
        extension['outputVariables'] = data['outputVariables']
        extension['options'] = json.loads(extension['options'])
        del extension['data']
        CACHE.set(extension_id, extension)
    return jsonify(**extension)


@bp.route('/extension/trigger_type/OnChange', methods=['GET'])
def extension_get_trigger_on_change():
    timeseries_id = request.args.get('timeseriesId')
    assert timeseries_id, 'timeseriesId should provide as query'
    print('GET extension trigger_type: OnChange timeseries:', timeseries_id)
    extensions = CACHE.hgetall_on_change_extensions_by_timeseries(timeseries_id)
    if extensions is None:
        extension_ids = trigger.extension_get_trigger_on_change(ENGINE, timeseries_id)
        assert extension_ids, f'No extension found for trigger_type: OnChange, timeseries_id: {timeseries_id}'
        extension_id_str = [f"'{ext_id}'" for ext_id in extension_ids]
        q = f'SELECT extensionId, extension, function, `data`, options FROM extensions WHERE extensionId IN ({",".join(extension_id_str)})'
        extensions = ENGINE.execute(q).fetchall()
        extensions = [dict(ext) for ext in extensions]
        CACHE.hset_on_change_timeseries_extension(timeseries_id, extensions)
    for ext in extensions:
        ext['data'] = json.loads(ext['data'])
        # TODO: Change for same format for the consistency
        # extension['variables'],extension['inputVariables'],extension['outputVariables'] = loads_data(ext['data'])
        ext['options'] = json.loads(ext['options'])
    return jsonify(extensions)


@bp.route('/extension/trigger_type/OnTime', methods=['GET'])
def extension_get_trigger_on_time():
    print('GET extension trigger_type: OnTime ')
    triggers, extension_ids = trigger.extension_get_trigger_on_time(ENGINE)
    assert extension_ids, f'No extension found for trigger_type: OnTime'
    extension_id_str = [f"'{ext_id}'" for ext_id in extension_ids]
    q = f'SELECT extensionId, extension, function, `data`, options FROM extensions WHERE extensionId IN ({",".join(extension_id_str)})'
    extensions = ENGINE.execute(q).fetchall()
    extensions = [dict(ext) for ext in extensions]
    extension_map = {}
    for ext in extensions:
        ext['data'] = json.loads(ext['data'])
        # TODO: Change for same format for the consistency
        # extension['variables'],extension['inputVariables'],extension['outputVariables'] = loads_data(ext['data'])
        ext['options'] = json.loads(ext['options'])
        extension_map[ext['extensionId']] = ext
    # Replace extensionId values with extension data
    for t in triggers:
        t['extensions'] = [extension_map[ex_id] for ex_id in t['extensions'] if ex_id in extension_map]
    return jsonify(triggers)


@bp.route("/extension/<extension_id>", methods=['DELETE'])
def extension_delete(extension_id):
    extension = CACHE.get(extension_id)
    if extension is None:
        extension = {'trigger': trigger.extension_trigger_get(ENGINE, extension_id)}
    del_triggers = trigger.extension_trigger_delete(ENGINE, extension_id)
    del_extension = ENGINE.execute(sql('''
        DELETE FROM extensions
        WHERE extensionId=:extension_id
    '''), extension_id=extension_id)
    # Remove from two caches s.t. extension and OnChange. TODO: Need to merge into on caching structure
    for t in extension['trigger']:
        if t['trigger_type'] == 'OnChange':
            CACHE.hdel_pipe_on_change_extension(t['trigger_on'], [extension_id])
    CACHE.delete(extension_id)
    return jsonify(extension_id)


def dumps_data(variables: List[dict], input_variables: List[str], output_variables: List[str]):
    return json.dumps({
        'variables': variables,
        'inputVariables': input_variables,
        'outputVariables': output_variables
    })


def loads_data(data):
    data = json.loads(data)
    return data['variables'], data['inputVariables'], data['outputVariables']
