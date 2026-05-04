from flask import Blueprint, request
from flask import make_response, jsonify
from flask import current_app as app
import json
import os

dst_api = Blueprint('dst_api', __name__)

from app.auth.scripts import checkUserDataAPIKey
from .scripts import format_get_request
from .scripts import get_datasets_information
from .scripts import (checkParamsRequest_rawdata,
                      checkParamsRequest_climatology,
                      checkParamsRequest_analysis)
from .scripts import (download_rawdata,
                      download_climdata,
                      download_analysis)
from .scripts import (response_download_json,
                      response_download_error)

@dst_api.route('/dataset_info', defaults={'dataset': None, 'temporal_res': None, 'variable': None})
@dst_api.route('/dataset_info/<dataset>', defaults={'temporal_res': None, 'variable': None})
@dst_api.route('/dataset_info/<dataset>/<temporal_res>', defaults={'variable': None})
@dst_api.route('/dataset_info/<dataset>/<temporal_res>/<variable>')
def dataset_info(dataset, temporal_res, variable):
    pyobj = get_datasets_information()
    if dataset:
        dataset = dataset.upper()[0:3]
        pyobj = pyobj[dataset]
    if temporal_res:
        temporal_res = temporal_res.lower()
        pyobj = pyobj[temporal_res]
        if variable:
            variable = variable.lower()
            pyobj = pyobj[variable]
    # return response_download_json(pyobj, 'dataset-info')
    return response_download_json(pyobj)

@dst_api.route('/download_raw_data', methods=['GET', 'POST'])
def download_raw_data():
    if request.method == 'GET':
        params = format_get_request(request.args)
    else:
        params = request.get_json()

    check_user = checkUserDataAPIKey(params, request, 'rawdata')
    if check_user['status'] == -1:
        return response_download_error(check_user['message'], None, check_user['code'])

    check_params = checkParamsRequest_rawdata(params)
    if check_params['status'] == -1:
        return response_download_error(check_params['message'], None, 400)

    try:
        params = check_params['params']
        params['user'] = check_user['user']
        params['httpMethod'] = request.method

        # print('---------------- rawdata ----------------')
        # print(params)

        return download_rawdata(params)
    except Exception as e:
        return response_download_error(str(e), None, 500)

@dst_api.route('/download_climtology_data', methods=['GET', 'POST'])
def download_climtology_data():
    if request.method == 'GET':
        params = format_get_request(request.args)
    else:
        params = request.get_json()

    check_user = checkUserDataAPIKey(params, request, 'climatology')
    if check_user['status'] == -1:
        return response_download_error(check_user['message'], None, check_user['code'])

    check_params = checkParamsRequest_climatology(params)
    if check_params['status'] == -1:
        return response_download_error(check_params['message'], None, 400)

    try:
        params = check_params['params']
        params['user'] = check_user['user']
        params['httpMethod'] = request.method

        # print('---------------- climatology ----------------')
        # print(params)

        return download_climdata(params)
    except Exception as e:
        return response_download_error(str(e), None, 500)

@dst_api.route('/download_analysis_data', methods=['GET', 'POST'])
def download_analysis_data():
    if request.method == 'GET':
        params = format_get_request(request.args)
    else:
        params = request.get_json()

    check_user = checkUserDataAPIKey(params, request, 'analysis')
    if check_user['status'] == -1:
        return response_download_error(check_user['message'], None, check_user['code'])

    check_params = checkParamsRequest_analysis(params)
    if check_params['status'] == -1:
        return response_download_error(check_params['message'], None, 400)

    try:
        params = check_params['params']
        params['user'] = check_user['user']
        params['httpMethod'] = request.method

        # print('---------------- analysis ----------------')
        # print(params)

        return download_analysis(params)
    except Exception as e:
        return response_download_error(str(e), None, 500)
