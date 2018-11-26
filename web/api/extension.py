from flask import Blueprint, request, jsonify, json
from sqlalchemy import text as sql

from web import util

bp = Blueprint('timeseries', __name__)
ENGINE = util.get_engine('metadata')

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
    trigger: {
        type: enum("onChange", "onTime"),
        on: []
    },
    options: {}
}
"""
@bp.route('/extension', methods=['POST'])
def extension_create():
    data = request.get_json()
    assert 'variables' in data and isinstance(data['variables'], list), f'variables should be provided'
    data['variables'], variable_names = util.create_timeseries(data['variables'])
    if 'inputVariables' in data:
        for v in data['inputVariables']:
            assert v in variable_names, f'{v} is not defined in variables'
    if 'outputVariables' in data:
        for v in data['outputVariables']:
            assert v in variable_names, f'{v} is not defined in variables'
    data['data'] = json.dumps({
        'variables': data['variables'],
        'inputVariables': data['inputVariables'],
        'outputVariables': data['outputVariables']
    })
    if 'options' not in data:
        data['options'] = '{}'
    else:
        data['options'] = json.dumps(data['options'])

    with ENGINE.begin() as conn:
        conn.execute(sql('''
            INSERT IGNORE INTO extensions (extensionId, extension, function, data, options)
            VALUES (:extensionId, :extension, :function, :data, :options)
        '''), **data)
        del data['data']
        data['options'] = json.loads(data['options'])
        return jsonify(data)


@bp.route('/extension/<extension_id>', methods=['GET'])
def extension_get(extension_id):
    print('GET extension:', extension_id)
    extension = ENGINE.execute(sql('''
        SELECT extensionId, extension, function, `data`, options
        FROM extensions WHERE extensionId=:extension_id
    '''), extension_id=extension_id).fetchone()
    assert extension, f'Extension does not exists: {extension_id}'
    extension = dict(extension)
    data = json.loads(extension['data'])
    extension['variables'] = data['variables']
    extension['inputVariables'] = data['inputVariables']
    extension['outputVariables'] = data['outputVariables']
    extension['options'] = json.loads(extension['options'])
    del extension['data']
    return jsonify(**extension)


@bp.route("/extension/<extension_id>", methods=['DELETE'])
def extension_delete(extension_id):
    extension = ENGINE.execute(sql('''
        DELETE FROM extensions
        WHERE extensionId=:extension_id
    '''), extension_id=extension_id)
    return jsonify(extension_id)
