import os
import numpy as np
import xarray as xr
import datetime
from app.scripts._global import GLOBAL_CONFIG

def get_datasets_information():
    datasets = GLOBAL_CONFIG['datasets']

    data_info = {}
    for d_key in datasets:
        data_info[d_key] = {}
        for t_key, t_val in datasets[d_key].items():
            if not t_val['compute']:
                zarr_dir = datasets[d_key][t_key]['zarr_dir']
                zarr_chunks = datasets[d_key][t_key]['chunks']

                data_info[d_key][t_key] = {}
                for v_key in t_val['netcdf']:
                    dataset = t_val['netcdf'][v_key]
                    zarr_var = os.path.basename(dataset['dir'])
                    zarr_path = os.path.join(zarr_dir, zarr_var)
                    xr_data = xr.open_zarr(zarr_path, chunks=zarr_chunks, consolidated=False)
                    lon = xr_data['lon'].values
                    lat = xr_data['lat'].values
                    bbox = {
                        'minlon': round(np.min(lon).item(), 6),
                        'maxlon': round(np.max(lon).item(), 6),
                        'minlat': round(np.min(lat).item(), 6),
                        'maxlat': round(np.max(lat).item(), 6)
                        }
                    sres = {
                        'lon': round(np.mean(np.diff(lon)).item(), 6),
                        'lat': round(np.mean(np.diff(lat)).item(), 6)
                        }
                    time = xr_data['time'].values
                    t_min = np.min(time).astype('datetime64[s]')
                    t_max = np.max(time).astype('datetime64[s]')
                    t_min = t_min.astype(datetime.datetime)
                    t_max = t_max.astype(datetime.datetime)

                    if t_key == 'dekadal':
                        ym1 = t_min.strftime('%Y-%m')
                        ym2 = t_max.strftime('%Y-%m')
                        dk1 = int(t_min.strftime('%d'))
                        dk2 = int(t_max.strftime('%d'))
                        dk1 = 1 if dk1 <= 10 else 3 if dk1 > 20 else 2
                        dk2 = 1 if dk2 <= 10 else 3 if dk2 > 20 else 2
                        trange = {'start': f'{ym1}-{dk1}', 'end': f'{ym2}-{dk2}'}
                    else:
                        trange = {
                            'start': t_min.strftime('%Y-%m-%d'),
                            'end': t_max.strftime('%Y-%m-%d')
                            }

                    data_info[d_key][t_key][v_key] = {'bbox': bbox,
                                                      'res': sres,
                                                      'range': trange}
    datasets_info = {
                'ALL': 'Analysis datasets',
                'MON': 'Monitoring datasets',
                'CLM': 'Climatology datasets'
            }

    out = {}
    for d_key in datasets:
        out[d_key] = {}
        for t_key, t_val in datasets[d_key].items():
            out[d_key][t_key] = {}
            for v_key, v_val in t_val['netcdf'].items():
                tmp = {}
                tmp['dataset_name'] = d_key
                tmp['dataset_longname'] = datasets_info[d_key]
                tmp['variable_name'] = v_key
                tmp['variable_longname'] = v_val['name']
                tmp['variable_units'] = v_val['units']
                tmp['missing_value'] = v_val['missval']
                tmp['temporal_resolution'] = t_key

                if t_val['compute']:
                    in_res = v_val['input']
                    trange = data_info[d_key][in_res][v_key]['range']
                    if t_key == 'annual':
                        tr1 = trange['start'][0:4]
                        tr2 = trange['end'][0:4]
                    else:
                        tr1 = trange['start'][0:7]
                        tr2 = trange['end'][0:7]

                    tmp['temporal_coverage'] = {'start': tr1, 'end': tr2}
                    # tmp['temporal_coverage'] = data_info[d_key][in_res][v_key]['range']
                    tmp['spatial_resolution'] = data_info[d_key][in_res][v_key]['res']
                    tmp['spatial_coverage'] = data_info[d_key][in_res][v_key]['bbox']
                    tmp['computed_from'] = f'{d_key} {in_res} {v_key} data'
                else:
                    tmp['temporal_coverage'] = data_info[d_key][t_key][v_key]['range']
                    tmp['spatial_resolution'] = data_info[d_key][t_key][v_key]['res']
                    tmp['spatial_coverage'] = data_info[d_key][t_key][v_key]['bbox']

                out[d_key][t_key][v_key] = tmp

    return out
