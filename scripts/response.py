import os
import re
import json
import numpy as np
import pandas as pd
import tempfile
import shutil
from .netcdf import write_netcdf_nc, write_netcdf_clim
from .util import convert_dict2csv, read_binary_file
from .dates import get_ncinfo_date

def _write_text_data(data, file):
   wf = open(file, 'w')
   wf.write(data)
   wf.close()

def response_data_csv(data, ncinfo):
    data = data['data']
    lon = [round(x, 6) for x in data['lon'].tolist()]
    lat = [round(x, 6) for x in data['lat'].tolist()]

    mlon, mlat = np.meshgrid(lon, lat)
    mlon = mlon.flatten()
    mlat = mlat.flatten()
    data = data['data'].flatten()
    data = data.filled()
    data = [round(x, 2) for x in data.tolist()]

    data_col = ncinfo['varid']
    csv_data = []
    for j in range(len(data)):
        csv_data += [{'Latitude': mlon[j],
                      'Longitude': mlat[j],
                      data_col: data[j]}]

    return convert_dict2csv(csv_data)

def response_data_json(data, ncinfo):
    poly = data['poly']
    data = data['data']
    date = data['date']
    lon = [round(x, 6) for x in data['lon'].tolist()]
    lat = [round(x, 6) for x in data['lat'].tolist()]
    dims = {'Latitude': len(lat), 'Longitude': len(lon)}
    miss = data['data'].fill_value.item()
    data = data['data'].filled()
    data = [[round(y, 2) for y in x] for x in data.tolist()]
    json_data = {'Date': date,
                 'Latitude': lat,
                 'Longitude': lon,
                 'Data': data,
                 'Dimensions': dims,
                 'VariableName': ncinfo['name'],
                 'VariableUnits': ncinfo['units'],
                 'Missing': miss}
    if poly:
        json_data['Name'] = poly
    return json.dumps(json_data)

def response_clim_csv(data, ncinfo):
    mdate, mlat, mlon = np.meshgrid(data['date'], data['lat'],
                                    data['lon'], indexing='ij')
    csv_data = {'Dates': mdate.flatten(),
                'Latitude': mlat.flatten(),
                'Longitude': mlon.flatten()}

    if len(data['ndims']) > 3:
        for i in range(len(data['dim4'])):
            csv_data[ncinfo['out_varid'][i]] = np.array(data['data'][i]).flatten()
    else:
        csv_data[ncinfo['varid']] = np.array(data['data']).flatten()

    csv_data = pd.DataFrame(csv_data)
    return csv_data.to_csv(index=False)

def response_clim_json(data, ncinfo):
    dims = {'Dates': len(data['date']),
            'Latitude': len(data['lat']),
            'Longitude': len(data['lon'])}
    json_data = {'Dates': data['date'],
                 'Latitude': data['lat'],
                 'Longitude': data['lon'],
                 'Data': data['data'],
                 'Dimensions': dims,
                 'VariableName': ncinfo['name'],
                 'VariableUnits': ncinfo['units'],
                 'Missing': data['missval']}
    if len(data['ndims']) > 3:
        key_dim = data['ndim4'].title()
        ex_dm = {key_dim: len(data['dim4'])}
        json_data['Dimensions'] = {**ex_dm, **dims}
        ex_dt = {key_dim: data['dim4']}
        json_data = {**ex_dt, **json_data}

    if data['poly']:
        json_data['Name'] = data['poly']

    return json.dumps(json_data)

def response_data_nc(data, ncinfo, timeinfo):
    tmp_file, ncfile = tempfile.mkstemp()
    lon = data['data']['lon']
    lat = data['data']['lat']
    data = data['data']['data']
    write_netcdf_nc(lon, lat, data, ncinfo, timeinfo, ncfile)
    os.close(tmp_file)
    nc_data = read_binary_file(ncfile)
    os.remove(ncfile)
    return nc_data

def response_clim_nc(data, ncinfo):
    tmp_file, ncfile = tempfile.mkstemp()
    write_netcdf_clim(data, ncinfo, ncfile)
    os.close(tmp_file)
    nc_data = read_binary_file(ncfile)
    os.remove(ncfile)
    return nc_data

def response_data_poly_json(data, ncinfo):
    json_data = []
    for d in data:
        jdata = response_data_json(d, ncinfo)
        json_data += [jdata]

    return json.dumps({'status': 0, 'data': json_data})

def response_clim_poly_json(data, ncinfo):
    json_data = []
    for d in data:
        jdata = response_clim_json(d, ncinfo)
        json_data += [jdata]

    return json.dumps({'status': 0, 'data': json_data})

def response_data_zip(data, outformat, ncinfo, timeinfo, filename):
    tmp_dir = tempfile.mkdtemp()

    for d in data:
        poly = re.sub(r'[^a-zA-Z0-9]', '', d['poly'])
        if outformat == 'netCDF-Format':
            ncfile = os.path.join(tmp_dir, f'{poly}_{filename}.nc')
            lon = d['data']['lon']
            lat = d['data']['lat']
            dat = d['data']['data']
            write_netcdf_nc(lon, lat, dat, ncinfo, timeinfo, ncfile)
        elif outformat == 'JSON-Format':
            jsondata = response_data_json(d, ncinfo)
            jsonfile = os.path.join(tmp_dir, f'{poly}_{filename}.json')
            _write_text_data(jsondata, jsonfile)
        elif outformat == 'CSV-Column-Format':
           csvdata = response_data_csv(d, ncinfo)
           csvfile = os.path.join(tmp_dir, f'{poly}_{filename}.csv')
           _write_text_data(csvdata, csvfile)
        else:
            return None

    tmp_file, zip_file = tempfile.mkstemp()
    ret_file = shutil.make_archive(zip_file, 'zip', tmp_dir)
    os.close(tmp_file)
    zip_data = read_binary_file(ret_file)
    os.remove(zip_file)
    os.remove(ret_file)
    shutil.rmtree(tmp_dir)
    return zip_data

def response_clim_zip(data, outformat, ncinfo, filename):
    tmp_dir = tempfile.mkdtemp()

    for d in data:
        poly = re.sub(r'[^a-zA-Z0-9]', '', d['poly'])
        if outformat == 'netCDF-Format':
            ncfile = os.path.join(tmp_dir, f'{poly}_{filename}.nc')
            write_netcdf_clim(d, ncinfo, ncfile)
        elif outformat == 'JSON-Format':
            jsondata = response_clim_json(d, ncinfo)
            jsonfile = os.path.join(tmp_dir, f'{poly}_{filename}.json')
            _write_text_data(jsondata, jsonfile)
        elif outformat == 'CSV-Column-Format':
           csvdata = response_clim_csv(d, ncinfo)
           csvfile = os.path.join(tmp_dir, f'{poly}_{filename}.csv')
           _write_text_data(csvdata, csvfile)
        else:
            return None

    tmp_file, zip_file = tempfile.mkstemp()
    ret_file = shutil.make_archive(zip_file, 'zip', tmp_dir)
    os.close(tmp_file)
    zip_data = read_binary_file(ret_file)
    os.remove(zip_file)
    os.remove(ret_file)
    shutil.rmtree(tmp_dir)
    return zip_data

def response_raw_points_json(data, dates, points, ncinfo):
    col_type = points.shape[1] == 4
    tmp = []
    for index, row in points.iterrows():
        val = [float(i) for i in data[index].tolist()]
        js = {
                'Name': row['loc'],
                'Longitude': row['lon'],
                'Latitude': row['lat'],
                'Values': val
             }
        if col_type:
            js['Type'] = row['type']
        tmp += [js]

    out = {'Dates': dates,
           'Data': tmp,
           'VariableName': ncinfo['name'],
           'VariableUnits': ncinfo['units'],
           'Missing': ncinfo['missval']}
    return json.dumps(out)

def response_clim_points_json(data, dates, points, ncinfo, mpars):
    col_type = points.shape[1] == 4
    multi_col = data[0].shape[1] > 1
    tmp = []
    for ix, row in points.iterrows():
        if multi_col:
            val = [[float(i) for i in data[ix][x].tolist()] for x in data[ix].columns]
        else:
            val = [float(i) for i in data[ix][0].tolist()]
        js = {
                'Name': row['loc'],
                'Longitude': row['lon'],
                'Latitude': row['lat'],
                'Values': val
             }
        if col_type:
            js['Type'] = row['type']
        tmp += [js]
    out = {'Dates': dates,
           'Data': tmp,
           'VariableName': ncinfo['name'],
           'VariableUnits': ncinfo['units'],
           'Missing': ncinfo['missval']}
    if mpars:
        out[mpars['name']] = mpars['values']
    return json.dumps(out)

def response_raw_points_cdt(data, dates, points):
    cdtheads = points.transpose()
    cdtheads.insert(0, 'date', ['Name', 'Longitude', 'Latitude'])
    data.insert(0, 'date', dates)
    out = pd.concat([cdtheads, data], axis=0, ignore_index=True)
    out = out.apply(lambda row: ','.join(row.values.astype(str)), axis=1)
    return '\n'.join(out.tolist())

def response_anomaly_points_cdt(data):
    out = [['Name', 'Longitude', 'Latitude'] + data['Dates']]
    for x in data['Data']:
        name = re.sub(r'[^a-zA-Z0-9]', '', x['Name'])
        hds = [name] + [x['Longitude']] + [x['Longitude']]
        out += [hds + x['Values']]
    out = np.array(out).T.tolist()
    return '\n'.join([','.join(x) for x in out])

def response_clim_points_cdt(data, dates, points):
    out = pd.concat(data, axis=1)
    out.columns = np.arange(0, out.shape[1])
    cdtheads = points.transpose()
    cdtheads.insert(0, 'date', ['Name', 'Longitude', 'Latitude'])
    out.insert(0, 'date', dates)
    out = pd.concat([cdtheads, out], axis=0, ignore_index=True)
    out = out.apply(lambda row: ','.join(row.values.astype(str)), axis=1)
    return '\n'.join(out.tolist())
