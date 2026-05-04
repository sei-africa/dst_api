import os
import xarray as xr
from .extract_clim import climatology_gridded_data
from app.scripts._global import GLOBAL_CONFIG

import warnings
warnings.filterwarnings('ignore', category=UserWarning)

def compute_some_climatogies(clim):
    params = {'dataset': None, 'temporalRes': None, 'variable': None,
              'geomExtract': 'original', 'climFunction': None,
              'startYear': 1991, 'endYear': 2020, 'minYear': 29,
               'seasLength': 3, 'daysWindow': 0, 'climDate': None,
              'gridded': True, 'fullYear': True}

    if clim == 'mean-stdev':
        params['climFunction'] = clim
        ex_key = 'statistics'
    elif clim == 'percentile':
        params['climFunction'] = clim
        params['precentileValue'] = [5, 25, 50, 75, 95]
        ex_key = 'quantile'
    else:
        print('Unknown climatology function.')
        return None

    datasets = GLOBAL_CONFIG['datasets']
    # data_dir = GLOBAL_CONFIG['data_dir']
    # zarr_dir = os.path.join(data_dir, 'zarr_clim', clim)
    clim_dir = GLOBAL_CONFIG['climatology']
    zarr_dir = os.path.join(clim_dir['zarr_dir'], clim)
    if not os.path.exists(zarr_dir):
        os.makedirs(zarr_dir)

    dataset_var = []
    for d in datasets:
        for k in datasets[d]:
            for v in datasets[d][k]['netcdf']:
                dataset_var += [[d, k, v]]

    for dset in dataset_var:
        zarr_path = os.path.join(zarr_dir, dset[0], dset[1], dset[2])
        if not os.path.exists(zarr_path):
            os.makedirs(zarr_path)

        if datasets[dset[0]][dset[1]]['compute']:
           in_data = datasets[dset[0]][dset[1]]['netcdf'][dset[2]]['input']
           zarr_chunks = datasets[dset[0]][in_data]['chunks']
        else:
            zarr_chunks = datasets[dset[0]][dset[1]]['chunks']

        zarr_chunks = zarr_chunks.copy()
        zarr_chunks['time'] = -1
        zarr_chunks[ex_key] = -1

        params['dataset'] = dset[0]
        params['temporalRes'] = dset[1]
        params['variable'] = dset[2]

        dataset = datasets[params['dataset']][params['temporalRes']]
        dataset = dataset['netcdf'][params['variable']]

        print(f'Compute climatology for {dset[0]} > {dset[1]} > {dset[2]}')
        xr_clim = climatology_gridded_data(params, dataset, keep_DataArray=True)
        xr_clim = xr_clim.chunk(chunks=zarr_chunks)
        xr_clim.to_zarr(
            store=zarr_path,
            mode='w',
            consolidated=False,
            zarr_format=3
        )

    print('Computing climatology and conversion to zarr done!')

def get_zarr_clim(dset, tres, var, clim):
    datasets = GLOBAL_CONFIG['datasets']
    # data_dir = GLOBAL_CONFIG['data_dir']
    # zarr_dir = os.path.join(data_dir, 'zarr_clim', clim)
    clim_dir = GLOBAL_CONFIG['climatology']
    zarr_dir = os.path.join(clim_dir['zarr_dir'], clim)

    zarr_path = os.path.join(zarr_dir, dset, tres, var)
    zarr_chunks = datasets[dset]['dekadal']['chunks']
    zarr_chunks = zarr_chunks.copy()
    zarr_chunks['time'] = -1
    if clim == 'mean-stdev':
        ex_key = 'statistics'
    elif clim == 'percentile':
        ex_key = 'quantile'
    else:
        return None
    zarr_chunks[ex_key] = -1
    xr_clim = xr.open_zarr(
        zarr_path,
        chunks=zarr_chunks,
        consolidated=False
    )
    date_values = xr_clim.attrs['date_values']
    xr_clim['date'] = xr.DataArray(
        date_values,
        dims='time',
        name='date'
    )
    return xr_clim
