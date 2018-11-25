from flask import Blueprint, request, jsonify
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
    with ENGINE.begin() as conn:
        return jsonify(**{"response": 123})


@bp.route('/extension/<extension_id>')
def extension_get(extension_id):
    return jsonify(**{'extension_id': extension_id})


@bp.route("/extension/<extension_id>", methods=['DELETE'])
def extension_delete(extension_id):
    extension = ENGINE.execute(sql('''
        DELETE FROM extension
        WHERE extensionId=:extension_id
    '''), extension_id=extension_id)
    return jsonify(extension_id)
