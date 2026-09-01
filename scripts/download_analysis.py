import json
import re
import base64
import numpy as np
from .response import *
from .anomaly import get_anomaly_data
from .spei_compute import get_spi_data
from .util import (
    response_download_file,
    response_download_error
)
from .dates import (
    get_ncinfo_date,
    format_output_date
)
from .download_raw import _get_download_dataset

def download_analysis(params):
    if params['analysis'] == 'anomaly':
        return download_anomaly_data(params)
    elif params['analysis'] == 'spi':
        return download_spi_data(params)
    elif params['analysis'] == 'spei':
        return download_spei_data(params)
    else:
        out = {'status': -1, 'message': 'Unknown'}
        return json.dumps(out)

def download_anomaly_data(params):
    params = _analysis_extra_params(params)
    filename = _get_analysis_filename(params)
    anom = get_anomaly_data(params)
    if anom['status'] == -1:
        return response_download_error(
            anom['message'], filename, 422
        )

    if params['gridded']:
        return _response_analysis_grid(anom['data'], params)
    else:
        return _response_anomaly_points(anom['data'], params)

def download_spi_data(params):
    params = _analysis_extra_params(params)
    filename = _get_analysis_filename(params)
    spi = get_spi_data(params)
    if spi['status'] == -1:
        return response_download_error(
            spi['message'], filename, 422
        )

    if params['gridded']:
        return _response_analysis_grid(spi['data'], params)
    else:
        return _response_anomaly_points(spi['data'], params)

def _response_anomaly_points(data_points, params):
    period = format_output_date(params)
    filename = f"anomaly_{params['variable']}_{params['temporalRes']}_{period}"

    if params['outFormat_0'] == 'CSV-CDT-Format':
        out_data = response_anomaly_points_cdt(data_points)
        filename = f'{filename}.csv'
        mimetype = 'text/csv'
    elif params['outFormat_0'] == 'JSON-Format':
        out_data = json.dumps(data_points)
        filename = f'{filename}.json'
        mimetype = 'application/json'
    else:
        out_data = {'status': -1, 'message': 'Unknown output format'}
        return response_download_error(out_data['message'], filename, 422)

    params['httpMethod'] = params['httpMethod_0']

    if params['webApp']:
        return json.dumps({'status': 0, 'data': out_data,
                           'filename': filename, 'mimetype': mimetype})
    else:
        return response_download_file(out_data, filename, mimetype)

def _response_analysis_grid(out, params):
    filename = _get_analysis_filename(params)
    timeinfo = get_ncinfo_date(
        params['temporalRes'], params['Date']
    )

    if type(out) is list:
        ncinfo = _get_ncinfo_variable(out[0], params)
        out = [_format_analysis_grid(d) for d in out]
        out_data = response_data_zip(
            out, params['outFormat_0'], ncinfo, timeinfo, filename
        )
        resfile = f'{filename}.zip'
        mimetype = 'application/zip'
        bin_data = True
    else:
        ncinfo = _get_ncinfo_variable(out, params)
        if 'Name' in out:
            name = re.sub(r'[^a-zA-Z0-9]', '', out['Name'])
            out_file = f'{name}_{filename}'
        else:
            out_file = filename

        if params['outFormat_0'] == 'netCDF-Format':
            out = _format_analysis_grid(out)
            out_data = response_data_nc(out, ncinfo, timeinfo)
            resfile = f'{out_file}.nc'
            mimetype = 'application/netcdf'
            bin_data = True
        elif params['outFormat_0'] == 'JSON-Format':
            out_data = json.dumps(out)
            resfile = f'{out_file}.json'
            mimetype = 'application/json'
            bin_data = False
        elif params['outFormat_0'] == 'CSV-Column-Format':
            out = _format_analysis_grid(out)
            out_data = response_data_csv(out, ncinfo)
            resfile = f'{out_file}.csv'
            mimetype = 'text/csv'
            bin_data = False
        else:
            out_data = {
                'status': -1,
                'message': 'Unknown output format'
            }
            return response_download_error(
                out_data['message'], out_file, 422
            )

    params['httpMethod'] = params['httpMethod_0']

    if params['webApp']:
        if bin_data:
            out_data = out_data.getvalue()
            out_data = base64.b64encode(out_data).decode('utf-8')

        return json.dumps({
            'status': 0,
            'data': out_data,
            'filename': resfile,
            'mimetype': mimetype
        })
    else:
        return response_download_file(
            out_data, resfile, mimetype
        )

def _format_analysis_grid(out):
    tmp = {}
    if 'Name' in out:
        tmp['poly'] = out['Name']
    tmp['data'] = {}
    tmp['data']['lon'] = np.array(out['Longitude'])
    tmp['data']['lat'] = np.array(out['Latitude'])
    tmp['data']['data'] = np.ma.masked_array(
        data=out['Data'],
        mask=False,
        fill_value=out['Missing']
    )
    return tmp

def _get_ncinfo_variable(out, params):
    dataset = _get_download_dataset(params)
    dataset['name'] = out['VariableName']
    dataset['units'] = out['VariableUnits']
    dataset['missval'] = out['Missing']
    return dataset

def _get_analysis_filename(params):
    f1 = f"{params['analysis']}_{params['variable']}"
    if params['gridded']:
        f2 = f"{params['temporalRes']}_{params['Date']}"
    else:
        period = format_output_date(params)
        f2 = f"{params['temporalRes']}_{period}"
    return f'{f1}_{f2}'

def _analysis_extra_params(params):
    params['httpMethod_0'] = params['httpMethod']
    pars = {'webApp': True, 'finalOutput': False}
    params = pars | params
    params['httpMethod'] = 'POST'
    return params

######

def download_spei_data(params):
    out = {'status': -1, 'message': 'SPEI not implemented yet', 'filename': 'Unknown'}
    return json.dumps(out)