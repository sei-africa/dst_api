import os
import re
import numpy as np
import xarray as xr
from functools import partial
from datetime import datetime
from .dates import extract_ncfiles_datetime
from app.scripts._global import GLOBAL_CONFIG

def _list_all_netcdf_files(nc_dir, nc_format):
    frmt = re.escape(nc_format)
    pattern = re.sub(r'%[a-zA-Z]', '[0-9]+', frmt)
    ncfiles = os.listdir(nc_dir)
    ncfiles = [f for f in ncfiles if re.match(pattern, f)]
    if len(ncfiles) == 0:
        return None

    ncfiles.sort()
    return ncfiles

def _set_xrDataset_dimensions(xr_ds, time_res, nc_format, dims_info):
    file_path = os.path.basename(xr_ds.encoding['source'])
    ds_date = extract_ncfiles_datetime(file_path, nc_format, time_res)
    xr_ds = xr_ds.expand_dims(dim = {'time': ds_date})
    return xr_ds.rename(dims_info)

def _get_xrDataset(nc_files, var_info, time_res, var_name, zarr_chunks):
    dim_names_info = {
        var_info['lon']: 'lon',
        var_info['lat']: 'lat',
        var_info['varid']: var_name
    }
    nc_paths = [os.path.join(var_info['dir'], f) for f in nc_files]
    call_fun = partial(_set_xrDataset_dimensions, time_res=time_res,
                      nc_format=var_info['format'], dims_info=dim_names_info)
    xr_ds = xr.open_mfdataset(nc_paths, preprocess=call_fun, parallel=False)
    xr_ds = xr.Dataset().merge(xr_ds)
    return xr_ds.chunk(chunks=zarr_chunks)

def create_zarr_datasets():
    datasets = GLOBAL_CONFIG['datasets']
    for data_set in datasets:
        for time_res in datasets[data_set]:
            dataset = datasets[data_set][time_res]
            if dataset['compute']:
                continue

            zarr_dir = dataset['zarr_dir']
            if not os.path.exists(zarr_dir):
                os.makedirs(zarr_dir)

            zarr_chunks = dataset['chunks']
            nc_info = dataset['netcdf']

            for var_name in nc_info:
                var_info = nc_info[var_name]
                nc_files = _list_all_netcdf_files(var_info['dir'], var_info['format'])
                if nc_files is None:
                    print(f'No netCDF files found for: {data_set} {time_res} {var_name}')
                    continue

                nc_dates = extract_ncfiles_datetime(nc_files, var_info['format'], time_res)
                nc_dates = nc_dates.astype('datetime64[s]')

                zarr_var = os.path.basename(var_info['dir'])
                zarr_path = os.path.join(zarr_dir, zarr_var)

                ds_new = True
                if os.path.exists(zarr_path):
                    zarr_check = [var_name] + list(zarr_chunks.keys())
                    zarr_check = [os.path.join(zarr_path, x) for x in zarr_check]
                    zarr_check = [os.path.exists(x) for x in zarr_check]
                    if all(zarr_check):
                        ds_new = False
                        zarr_data = xr.open_zarr(zarr_path, consolidated=False)
                        zarr_dates = zarr_data['time'].values
                        zarr_dates = zarr_dates.astype('datetime64[s]')
                        nc_update = np.isin(nc_dates, zarr_dates)
                        if all(nc_update.tolist()):
                            print(f'No update for: {data_set} {time_res} {var_name}')
                            continue
                        else:
                            nc_update = ~nc_update

                        nc_dates = nc_dates[nc_update]
                        dt_dates = [x.item() for x in nc_dates]

                        if time_res == 'daily':
                            nc_files = [x.strftime(var_info['format']) for x in dt_dates]
                        elif time_res == 'dekadal':
                            nc_files = []
                            for d in dt_dates:
                                dy = int(d.strftime('%d'))
                                dk = 1 if dy < 11 else 3 if dy > 20 else 2
                                d = d.replace(day = dk)
                                dk_frmt = var_info['format'].replace('%d', '%-d')
                                nc_files += [d.strftime(dk_frmt)]
                        elif time_res == 'monthly':
                            nc_files = [x.strftime(var_info['format']) for x in dt_dates]
                        else:
                            print(f'Unknown time resolution "{time_res}" for {data_set} {var_name}')
                            continue
                    else:
                        os.makedirs(zarr_path, exist_ok=True)
                else:
                    os.makedirs(zarr_path)

                print(f'Converting netCDF files to zarr for: {data_set} {time_res} {var_name} ....')

                if ds_new:
                    xr_data = _get_xrDataset(
                        nc_files, var_info, time_res,
                        var_name, zarr_chunks
                    )
                    xr_data.to_zarr(
                        store=zarr_path,
                        mode='w',
                        consolidated=False
                    )
                else:
                    dv = zarr_chunks['time']
                    nc_chunks = [
                        nc_files[i:i+dv]
                        for i in range(0, len(nc_files), dv)
                    ]
                    for chk_file in nc_chunks:
                        xr_data = _get_xrDataset(
                            chk_file, var_info, time_res,
                            var_name, zarr_chunks
                        )
                        xr_data.to_zarr(
                            store=zarr_path,
                            append_dim='time',
                            consolidated=False
                        )

                print('Conversion to zarr done!')

def get_zarr_dataset(params):
    datasets = GLOBAL_CONFIG['datasets'][params['dataset']]
    if datasets[params['temporalRes']]['compute']:
        input_res = datasets[params['temporalRes']]['netcdf'][params['variable']]['input']
    else:
        input_res = params['temporalRes']

    dataset = datasets[input_res]['netcdf'][params['variable']]
    zarr_dir = datasets[input_res]['zarr_dir']
    zarr_chunks = datasets[input_res]['chunks']
    zarr_var = os.path.basename(dataset['dir'])
    zarr_path = os.path.join(zarr_dir, zarr_var)
    return xr.open_zarr(zarr_path, chunks=zarr_chunks, consolidated=False)
