import numpy as np
from .extract_dailydata import *
from .extract_dailyclim import *

def anomaly_gridded_dailydata(params, datainfo, bbox=None):
    tseries = extract_rectangular_grid_dailydata(params, bbox)
    if tseries['status'] == -1: return tseries
    climato = climatology_gridded_dailydata(params, datainfo, bbox)
    if climato['status'] == -1: return climato
    return {
            'status': 0,
            'tseries': tseries['data'],
            'climato': climato['data']
        }

def anomaly_polygons_grid_dailydata(params, datainfo):
    tseries = extract_polygons_grid_dailydata(params)
    if tseries['status'] == -1: return tseries
    climato = climatology_polygons_grid_dailydata(params, datainfo)
    if climato['status'] == -1: return climato
    return {
            'status': 0,
            'tseries': tseries['data'],
            'climato': climato['data']
        }

def anomaly_rectangle_point_dailydata(params, datainfo, bbox=None):
    tseries = extract_rectangle_point_dailydata(params, bbox)
    if tseries['status'] == -1: return tseries
    climato = climatology_rectangle_point_dailydata(params, datainfo, bbox)
    if climato['status'] == -1: return climato
    return _anomaly_return_data(tseries, climato)

def anomaly_polygons_point_dailydata(params, datainfo):
    tseries = extract_polygons_points_dailydata(params)
    if tseries['status'] == -1: return tseries
    climato = climatology_polygons_point_dailydata(params, datainfo)
    if climato['status'] == -1: return climato
    return _anomaly_return_data(tseries, climato)

def anomaly_geojson_dailydata(params, datainfo):
    tseries = extract_geojson_points_dailydata(params)
    if tseries['status'] == -1: return tseries
    climato = climatology_geojson_dailydata(params, datainfo)
    if climato['status'] == -1: return climato
    return _anomaly_return_data(tseries, climato)

def anomaly_multipoints_dailydata(params, datainfo):
    tseries = extract_multipoints_dailydata(params)
    if tseries['status'] == -1: return tseries
    climato = climatology_multipoints_dailydata(params, datainfo)
    if climato['status'] == -1: return climato
    return _anomaly_return_data(tseries, climato)

def _anomaly_return_data(tseries, climato):
    del tseries['status']
    del climato['status']
    return {
            'status': 0,
            'tseries': tseries,
            'climato': climato
        }

def get_anomaly_multipoints_dailydata(anomaly_data, anom_type, datainfo):
    clim = np.array(anomaly_data['climato']['data'])
    clim = clim.astype(float)
    cmiss = anomaly_data['climato']['missval']
    clim = np.where(clim == cmiss, np.nan, clim)
    ts = np.array(anomaly_data['tseries']['data'])
    ts = ts.astype(float)
    tmiss = datainfo['missval']
    ts = np.where(ts == tmiss, np.nan, ts)

    amiss = -9999.
    cmean = clim[:, :, 0].squeeze()
    csd = clim[:, :, 1].squeeze()

    if anom_type == 'difference':
        anom = ts - cmean
        anom = np.nan_to_num(anom, nan=amiss)
        rnd = 2
        units = datainfo['units']
    elif anom_type == 'standardized':
        mask = csd < 10e-5
        stdev = np.ma.masked_array(csd, mask=mask)
        anom = (ts - cmean)/stdev
        mask = np.array([mask] * ts.shape[0])
        anom[mask] = 0.
        anom = anom.filled(fill_value=amiss)
        rnd = 4
        units = ''
    elif anom_type == 'percentage':
        mask = cmean < 10e-5
        mean = np.ma.masked_array(cmean, mask=mask)
        anom = 100 * (ts - cmean)/mean
        mask = np.array([mask] * ts.shape[0])
        anom[mask] = 0.
        anom = anom.filled(fill_value=amiss)
        rnd = 2
        units = '%'

    anom = [
        [round(y, rnd) for y in x]
        for x in anom.tolist()
    ]
    coords = anomaly_data['tseries']['coords']

    ts_data = []
    for j in range(0, len(coords)):
        pts = {
            'Name': coords.iat[j, 0],
            'Longitude': coords.iat[j, 1].item(),
            'Latitude': coords.iat[j, 2].item(),
            'Values': [v[j] for v in anom]
        }
        if coords.shape[1] == 4:
            pts['Type'] = coords.iat[j, 3]

        ts_data += [pts]

    return {
            'Dates': anomaly_data['tseries']['dates'],
            'Data': ts_data,
            'VariableName': f"Anomaly {datainfo['name']}",
            'VariableUnits': units,
            'Missing': amiss
        }

def get_anomaly_gridded_dailydata(anomaly_data, anom_type):
    out_data = []
    for j in range(len(anomaly_data['tseries'])):
        clim = anomaly_data['climato'][j]
        cdata = np.array(clim['data'])
        cdata = cdata.astype(float)
        cmiss = clim['missval']
        cdata = np.where(cdata == cmiss, np.nan, cdata)

        tsdata = anomaly_data['tseries'][j]
        ts = tsdata['data']['data']

        cmean = cdata[0, :, :, :].squeeze()
        csd = cdata[1, :, :, :].squeeze()

        if anom_type == 'difference':
            anom = ts - cmean
            rnd = 2
        elif anom_type == 'standardized':
            mask = csd < 10e-5
            stdev = np.ma.masked_array(csd, mask=mask)
            anom = (ts - cmean)/stdev
            anom[mask] = 0.
            rnd = 4
        elif anom_type == 'percentage':
            mask = cmean < 10e-5
            mean = np.ma.masked_array(cmean, mask=mask)
            anom = 100 * (ts - cmean)/mean
            anom[mask] = 0.
            rnd = 2

        anom = np.round(anom, rnd)
        anom.fill_value = -9999.
        tsdata['data']['data'] = anom
        out_data += [tsdata]

    return out_data
