import json
import copy
import numpy as np
from .download_clim import download_climdata
from .download_zarrclim import extract_climdata
from .download_raw import download_rawdata
from app.scripts._cache import cache, hash_pamars_anom

def get_anomaly_data(params):
    params['webApp'] = True
    params['httpMethod_0'] = params['httpMethod']
    params['httpMethod'] = 'POST'
    params['finalOutput'] = False

    cache_key = hash_pamars_anom(params)
    cached_data = cache.get(cache_key)

    if cached_data is None:
        data = _anomaly_get_data(params)
        if data['status'] == -1: return data

        climato = data['climato']
        tseries = data['tseries']

        if params['gridded']:
            if type(tseries) is list:
                cached_data = []
                for j in range(len(tseries)):
                    cached_data += [_anomaly_gridded_data(tseries[j], climato[j], params)]
            else:
                cached_data = _anomaly_gridded_data(tseries, climato, params)
        else:
            cached_data = _anomaly_multipoints_data(tseries, climato, params)

        cache.set(cache_key, cached_data)

    return {'status': 0, 'data': cached_data}

def _compute_anomalies(ts_data, anom_type, data_units,
                      mean, stdev=None):
    anom_miss = -9999.
    if anom_type == 'difference':
        anom = ts_data - mean
        anom = np.nan_to_num(anom, nan=anom_miss)
        rnd = 2
        units = data_units
    elif anom_type == 'standardized':
        mask = stdev < 10e-5
        stdev = np.ma.masked_array(stdev, mask=mask)
        anom = (ts_data - mean)/stdev
        anom[mask] = 0.
        anom = anom.filled(fill_value=anom_miss)
        rnd = 4
        units = ''
    elif anom_type == 'percentage':
        mask = mean < 10e-5
        mean = np.ma.masked_array(mean, mask=mask)
        anom = 100 * (ts_data - mean)/mean
        anom[mask] = 0.
        anom = anom.filled(fill_value=anom_miss)
        rnd = 2
        units = '%'
    else:
        return None

    if len(ts_data.shape) == 1:
        anom = [round(x, rnd) for x in anom.tolist()]
    else:
        anom = [[round(y, rnd) for y in x] for x in anom.tolist()]

    return {'anomaly': anom, 'units': units, 'missval': anom_miss}

def _anomaly_format_params(params):
    start_year = 1991
    if 'startYear' in params:
        start_year = int(params['startYear'])
    params['startYear'] = start_year

    end_year = 2020
    if 'endYear' in params:
        end_year = int(params['endYear'])
    params['endYear'] = end_year

    min_year = 30
    if 'minYear' in params:
        min_year = int(params['minYear'])
    params['minYear'] = min_year

    seas_len = 3
    if params['temporalRes'] == 'seasonal':
        seas_len = int(params['seasLength'])
    params['seasLength'] = seas_len

    day_win = 0
    if params['temporalRes'] == 'daily':
        day_win = int(params['daysWindow'])
    params['daysWindow'] = day_win

    return params

def _anomaly_get_data(params):
    params = _anomaly_format_params(params)
    params_clim = ['dataset', 'temporalRes', 'variable', 
        'fullYear', 'climDate', 'seasLength', 'daysWindow',
        'climFunction', 'startYear', 'endYear', 'minYear',
        'geomExtract', 'pointsSource', 'pointsFile', 'pointsList',
        'padLon', 'padLat', 'minLon', 'maxLon', 'minLat', 'maxLat',
        'shpSource', 'shpFile', 'shpField', 'Poly', 'allPolygons',
        'geojsonSource', 'geojsonFile', 'geojsonData', 'geojsonField',
        'spatialAvg', 'outFormat', 'webApp', 'gridded',
        'user', 'httpMethod', 'finalOutput']
    pclim = {k: v for k, v in params.items() if k in params_clim}
    dailyClim = pclim['temporalRes'] == 'daily' and pclim['daysWindow'] != 0
    seasClim = pclim['temporalRes'] == 'seasonal' and pclim['seasLength'] != 3
    if dailyClim or seasClim:
        clim = download_climdata(pclim)
    else:
        clim = extract_climdata(pclim)
    clim = json.loads(clim)
    if clim['status'] == -1: return clim
    if type(clim['data']) is list:
        clim = [json.loads(x) for x in clim['data']]
    else:
        clim = json.loads(clim['data'])

    tseries = download_rawdata(params)
    tseries = json.loads(tseries)
    if tseries['status'] == -1: return tseries
    if type(tseries['data']) is list:
        tseries = [json.loads(x) for x in tseries['data']]
    else:
        tseries = json.loads(tseries['data'])

    clim = copy.deepcopy(clim)
    tseries = copy.deepcopy(tseries)

    return {'status': 0, 'climato': clim, 'tseries': tseries}

def _anomaly_multipoints_data(tseries, climato, params):
    clim_dates = climato['Dates']
    clim_data = climato['Data']
    ts_dates = tseries['Dates']
    ts_data = tseries['Data']

    if params['temporalRes'] == 'daily':
        clim_d = [d.split('_')[1] for d in clim_dates]
        clim_d = [''.join(d.split('-')) for d in clim_d]
        ts_d = [d[4:8] for d in ts_dates]
        ts_d = ['0228' if d == '0229' else d for d in ts_d]
        ic = [clim_d.index(d) for d in ts_d]
    elif params['temporalRes'] == 'dekadal':
        clim_d = [d.split('_')[1] for d in clim_dates]
        clim_d = [''.join(d.split('-')) for d in clim_d]
        ts_d = [d[4:7] for d in ts_dates]
        ic = [clim_d.index(d) for d in ts_d]
    elif params['temporalRes'] == 'monthly':
        clim_d = [d.split('_')[1] for d in clim_dates]
        ts_d = [d[4:6] for d in ts_dates]
        ic = [clim_d.index(d) for d in ts_d]
    elif params['temporalRes'] == 'seasonal':
        clim_d = [d.split('_')[1] for d in clim_dates]
        ts_d = [[x.split('-')[1] for x in d.split('_')] for d in ts_dates]
        ts_d = ['-'.join(d) for d in ts_d]
        ic = [clim_d.index(d) for d in ts_d]
    elif params['temporalRes'] == 'annual':
        ic = 0
    else:
        return None

    for j in range(len(ts_data)):
        ts = np.array(ts_data[j]['Values'])
        ts = np.where(ts == tseries['Missing'], np.nan, ts)

        mean = clim_data[j]['Values'][0]
        stdev = clim_data[j]['Values'][1]
        if params['temporalRes'] == 'annual':
            mean = np.repeat(mean[ic], len(ts))
            stdev = np.repeat(stdev[ic], len(ts))
        else:
            mean = np.array([mean[i] for i in ic])
            stdev = np.array([stdev[i] for i in ic])
        mean = np.where(mean == climato['Missing'], np.nan, mean)
        stdev = np.where(stdev == climato['Missing'], np.nan, stdev)
        anom = _compute_anomalies(ts, params['anomaly'],
                                  tseries['VariableUnits'],
                                  mean, stdev)
        if anom is None: return None
        ts_data[j]['Values'] = anom['anomaly']

    tseries['Data'] = ts_data
    tseries['Missing'] = anom['missval']
    tseries['VariableName'] = f"Anomaly {tseries['VariableName']}"
    tseries['VariableUnits'] = anom['units']

    return tseries

def _anomaly_gridded_data(tseries, climato, params):
    clim_dates = climato['Dates']
    clim_data = np.array(climato['Data'])
    clim_data = np.where(clim_data == climato['Missing'], np.nan, clim_data)
    ts_date = tseries['Date']
    ts_data = np.array(tseries['Data'])
    ts_data = np.where(ts_data == tseries['Missing'], np.nan, ts_data)

    if params['temporalRes'] == 'daily':
        clim_d = [d.split('_')[1] for d in clim_dates]
        ts_d = '-'.join(ts_date.split('-')[1:])
        if ts_d == '02-29':
            ts_d = '02-28'
        ic = clim_d.index(ts_d)
    elif params['temporalRes'] == 'dekadal':
        clim_d = [d.split('_')[1] for d in clim_dates]
        ts_d = '-'.join(ts_date.split('-')[1:])
        ic = clim_d.index(ts_d)
    elif params['temporalRes'] == 'monthly':
        clim_d = [d.split('_')[1] for d in clim_dates]
        ts_d = ts_date.split('-')[1]
        ic = clim_d.index(ts_d)
    elif params['temporalRes'] == 'seasonal':
        clim_d = [d.split('_')[1] for d in clim_dates]
        ts_d = [d.split('-')[1] for d in ts_date.split('_')]
        ts_d = '-'.join(ts_d)
        ic = clim_d.index(ts_d)
    elif params['temporalRes'] == 'annual':
        ic = 0
    else:
        return None

    mean = clim_data[0, ic, :, :]
    stdev = clim_data[1, ic, :, :]
    anom = _compute_anomalies(ts_data, params['anomaly'],
                tseries['VariableUnits'], mean, stdev)
    if anom is None: return None

    tseries['Data'] = anom['anomaly']
    tseries['Missing'] = anom['missval']
    tseries['VariableName'] = f"Anomaly {tseries['VariableName']}"
    tseries['VariableUnits'] = anom['units']

    return tseries
