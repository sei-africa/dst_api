import os
import netCDF4 as nc
import xarray as xr
import numpy as np
from datetime import datetime
from .dates import aggregate_seq_dates
from app.scripts._global import GLOBAL_CONFIG

def format_netcdf_filename(time_res, date, nc_format):
    if time_res == 'dekadal':
        dkformat = nc_format.replace('%d', '%-d')
        filename = date.strftime(dkformat)
    else:
        filename = date.strftime(nc_format)

    return filename

def get_netcdf_file(data_set, time_res, variable, time):
    datasets = GLOBAL_CONFIG['datasets'][data_set]
    ncinfo = datasets[time_res]['netcdf'][variable]
    date = datetime.strptime(time, '%Y-%m-%d')
    return format_netcdf_filename(time_res, date, ncinfo['format'])

def get_netcdf_dir(data_set, time_res, variable):
    datasets = GLOBAL_CONFIG['datasets'][data_set]
    ncinfo = datasets[time_res]['netcdf'][variable]
    return ncinfo['dir']

def get_netcdf_path(data_set, time_res, variable, time):
    ncdir = get_netcdf_dir(data_set, time_res, variable)
    ncfile = get_netcdf_file(data_set, time_res, variable, time)
    return os.path.join(ncdir, ncfile)

def read_netcdf_nc(ncinfo):
    ncdata = nc.Dataset(ncinfo['ncfile'])
    data = ncdata.variables[ncinfo['varid']][:]

    mask = np.ma.getmask(data)
    if len(mask.shape) == 0:
        mask = np.zeros(np.prod(data.shape), dtype=bool)
        mask = mask.reshape(data.shape)
        data = np.ma.masked_array(data, mask = mask)
        data.fill_value = ncinfo['missval']

    lon = ncdata.variables[ncinfo['lon']][:]
    lat = ncdata.variables[ncinfo['lat']][:]
    time = None

    var_names = list(ncdata.variables.keys())
    if ncinfo['time'] in var_names:
        timeinfo = ncdata.variables[ncinfo['time']]
        units = timeinfo.units
        calendar = 'standard'
        if hasattr(timeinfo, 'calendar'):
            calendar = timeinfo.calendar
        time = nc.num2date(timeinfo[:], units=units, calendar=calendar)

    ncdata.close()
    return {'lon': lon, 'lat': lat, 'time': time, 'data': data}

def read_netcdf_xr(ncinfo):
    xrdata = xr.open_dataset(ncinfo['ncfile'])
    data = xrdata[ncinfo['varid']].values
    data = np.ma.masked_array(data, mask = np.isnan(data))
    data.fill_value = ncinfo['missval']
    lon = xrdata[ncinfo['lon']].values
    lat = xrdata[ncinfo['lat']].values
    xrdata.close()
    return {'lon': lon, 'lat': lat, 'data': data}

def extract_netcdf_bbox(data, bbox):
    ilon = np.logical_and(data['lon'] >= bbox['minLon'],
                          data['lon'] <= bbox['maxLon'])
    ilat = np.logical_and(data['lat'] >= bbox['minLat'],
                          data['lat'] <= bbox['maxLat'])
    lon = data['lon'][ilon]
    lat = data['lat'][ilat]
    data = data['data'][ilat, :][:, ilon]
    return {'lon': lon, 'lat': lat, 'data': data}

def get_netcdf_data(data_set, time_res, variable, time, bbox = None):
    datasets = GLOBAL_CONFIG['datasets'][data_set]
    ncinfo = datasets[time_res]['netcdf'][variable]
    date = datetime.strptime(time, '%Y-%m-%d')
    filename = format_netcdf_filename(time_res, date, ncinfo['format'])
    ncfile = os.path.join(ncinfo['dir'], filename)
    if not os.path.exists(ncfile):
        return None
    ncinfo['ncfile'] = ncfile
    # ncdata = read_netcdf_xr(ncinfo)
    ncdata = read_netcdf_nc(ncinfo)
    if bbox is not None:
        ncdata = extract_netcdf_bbox(ncdata, bbox)

    ncdata.update({'date': date})
    return ncdata

def aggregate_netcdf_data(params, dataset, bbox=None):
    seq_dates = aggregate_seq_dates(params['temporalRes'],
                            dataset['input'], params['Date'])
    minfrac = float(dataset['minfrac'])
    nl = len(seq_dates)

    nc_data = []
    for date in seq_dates:
        nc_data += [get_netcdf_data(params['dataset'], dataset['input'],
                                    params['variable'], date, bbox)]

    nb_miss = nc_data.count(None)
    nmiss_frac = (nl - nb_miss)/nl

    if len(seq_dates) == nb_miss:
        msg = 'No netCDF files found'
        return {'status': -1, 'message': msg}

    if nmiss_frac < minfrac:
        msg = 'Too many missing netCDF files'
        return {'status': -1, 'message': msg}

    nc_data = [x for x in nc_data if x is not None]
    lon = np.round(nc_data[0]['lon'], 6)
    lat = np.round(nc_data[0]['lat'], 6)
    nc_data = [x['data'] for x in nc_data]
    masks = [np.ma.getmask(x) for x in nc_data]
    masks = np.sum(masks, axis=0)
    masks = (nl - masks)/nl
    masks = masks < minfrac

    if dataset['function'] == 'sum':
        fun = np.ma.sum
    elif dataset['function'] == 'mean':
        fun = np.ma.mean
    else:
        msg = 'Unknown function to aggregate data'
        return {'status': -1, 'message': msg}

    nc_data = fun(nc_data, axis=0)
    nc_data = np.ma.masked_array(nc_data, mask = masks)
    nc_data = np.ma.round_(nc_data, 3)
    nc_data.fill_value = dataset['missval']
    out_data = {'lon': lon, 'lat': lat, 'data': nc_data}
    return {'status': 0, 'data': out_data}

def write_netcdf_nc(lon, lat, data, ncinfo, timeinfo, ncfile):
    nTime = ncinfo['time']
    nLat = ncinfo['lat']
    nLon = ncinfo['lon']

    ncout = nc.Dataset(ncfile, mode='w', format='NETCDF4')
    ncout.createDimension(nTime, None)
    ncout.createDimension(nLat, len(lat))
    ncout.createDimension(nLon, len(lon))

    # create time axis
    nc_time = ncout.createVariable(nTime, np.float64, (nTime,))
    nc_time.long_name = 'Time'
    nc_time.units = timeinfo['units']
    nc_time.calendar = 'standard'
    nc_time.axis = 'T'
    nc_time[:] = timeinfo['values']

    # create latitude axis
    nc_lat = ncout.createVariable(nLat, np.float32, (nLat,))
    nc_lat.standard_name = 'latitude'
    nc_lat.long_name = 'Latitude'
    nc_lat.units = 'degrees_north'
    nc_lat.axis = 'Y'
    nc_lat[:] = np.array(lat.tolist())

    # create longitude axis
    nc_lon = ncout.createVariable(nLon, np.float32, (nLon,))
    nc_lon.standard_name = 'longitude'
    nc_lon.long_name = 'Longitude'
    nc_lon.units = 'degrees_east'
    nc_lon.axis = 'X'
    nc_lon[:] = np.array(lon.tolist())

    # if ncinfo['prec'] == 'float'
    #     prec = np.float32
    nc_prec = np.float32
    nc_data = ncout.createVariable(ncinfo['varid'], nc_prec,
        (nTime, nLat, nLon), zlib=True, complevel=6)
    nc_data.long_name = ncinfo['name']
    nc_data.units = ncinfo['units']
    nc_data.missing_value = ncinfo['missval']
    data = data.filled()

    # add time dimension
    data = data[np.newaxis, :, :]
    nc_data[:, :, :] = data

    ncout.close()
    return 0

def write_netcdf_clim(data, ncinfo, ncfile):
    time = np.array(data['time'])
    lon = np.array(data['lon'])
    lat = np.array(data['lat'])

    ncout = nc.Dataset(ncfile, mode='w', format='NETCDF4')
    ncout.createDimension('time', len(time))
    ncout.createDimension('lat', len(lat))
    ncout.createDimension('lon', len(lon))

    # create time axis
    nc_time = ncout.createVariable('time', np.int32, ('time',))
    nc_time.long_name = 'Time'
    nc_time.units = ''
    nc_time.calendar = 'standard'
    nc_time.axis = 'T'
    nc_time[:] = time

    # create latitude axis
    nc_lat = ncout.createVariable('lat', np.float32, ('lat',))
    nc_lat.standard_name = 'latitude'
    nc_lat.long_name = 'Latitude'
    nc_lat.units = 'degrees_north'
    nc_lat.axis = 'Y'
    nc_lat[:] = lat

    # create longitude axis
    nc_lon = ncout.createVariable('lon', np.float32, ('lon',))
    nc_lon.standard_name = 'longitude'
    nc_lon.long_name = 'Longitude'
    nc_lon.units = 'degrees_east'
    nc_lon.axis = 'X'
    nc_lon[:] = lon

    # data
    if len(data['ndims']) > 3:
        for i in range(len(data['dim4'])):
            nc_data = ncout.createVariable(ncinfo['out_varid'][i], np.float32,
                ('time', 'lat', 'lon'), zlib=True, complevel=9)
            nc_data.long_name = ncinfo['name'][i]
            if type(ncinfo['units']) is list:
                nc_data.units = ncinfo['units'][i]
            else:
                nc_data.units = ncinfo['units']
            if type(ncinfo['missval']) is list:
                nc_data.missing_value = ncinfo['missval'][i]
            else:
                nc_data.missing_value = ncinfo['missval']
            nc_data[:, :, :] = np.array(data['data'][i])
    else:
        nc_data = ncout.createVariable(ncinfo['varid'], np.float32,
            ('time', 'lat', 'lon'), zlib=True, complevel=9)
        nc_data.long_name = ncinfo['name']
        nc_data.units = ncinfo['units']
        nc_data.missing_value = ncinfo['missval']
        nc_data[:, :, :] = np.array(data['data'])

    # date
    nc_date = ncout.createVariable('date', np.str_, ('time'))
    nc_date.long_name = 'Climatology dates'
    nc_date[:] = np.array(data['date'])

    ncout.close()
    return 0
