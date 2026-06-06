import json
import numpy as np
import pandas as pd
import geopandas as gpd
import xarray as xr
from .shapefiles import *
from .extract import *
from .util import remove_duplicates_list
from .index_dailydata import get_daily_index_season
from .aggregate_dailydata import aggregate_daily_analysis
from .zarrdata import get_zarr_daily_dataset
from .netcdf import extract_netcdf_bbox
from .geojson import get_user_geojson, geojson_polygons_points

def extract_rectangular_grid_dailydata(params, bbox):
    xr_data = get_zarr_daily_dataset(params)
    xr_coords = get_coords_dataset(xr_data)

    if bbox:
        sindex = get_bbox_latlon_index(xr_coords, bbox)
        if sindex is None:
            msg = 'Rectangle outside of data spatial range.'
            return {'status': -1, 'message': msg}
        xr_ds = xr_data.isel(
            lat=sindex['lat'],
            lon=sindex['lon']
        )
    else:
        xr_ds = xr_data

    out = _get_grid_dailydata(xr_ds, params)
    if out['status'] == -1: return out

    out_data = [{'data': out['data'], 'poly': None}]
    return {'status': 0, 'data': out_data}

def extract_polygons_grid_dailydata(params):
    shpObj = get_shapefiles_data(params)
    if shpObj['status'] == -1: return shpObj

    multipolygons = False
    if type(shpObj['polys']) is list:
        if len(shpObj['polys']) > 1:
            multipolygons = True
        else:
            shpObj['polys'] = shpObj['polys'][0]

    xr_data = get_zarr_daily_dataset(params)
    out = _get_grid_dailydata(xr_data, params)
    if out['status'] == -1: return out

    if multipolygons:
        out_data = []
        for poly in shpObj['polys']:
            bbox = format_bbox_polygons(
                shpObj['bbox'],
                params['shpField'],
                poly
            )
            ret = extract_netcdf_bbox(out['data'], bbox)
            ext = extract_polygons_griddata(
                ret,
                shpObj['shp'],
                params['shpField'],
                poly
            )
            ext['date'] = params['Year']
            out_data += [{'data': ext, 'poly': poly}]
    else:
        bbox = format_bbox_polygons(
            shpObj['bbox'],
            params['shpField'],
            shpObj['polys']
        )
        ext = extract_polygons_griddata(
            out['data'],
            shpObj['shp'],
            params['shpField'],
            shpObj['polys']
        )
        ext['date'] = params['Year']
        out_data = [{'data': ext, 'poly': shpObj['polys']}]

    return {'status': 0, 'data': out_data}

def extract_polygons_points_dailydata(params):
    shpObj = get_shapefiles_data(params)
    if shpObj['status'] == -1: return shpObj

    if type(shpObj['polys']) is not list:
        shpObj['polys'] = [shpObj['polys']]

    xr_data = get_zarr_daily_dataset(params)
    xr_coords = get_coords_dataset(xr_data)

    sindex = []
    for poly in shpObj['polys']:
        sindex += [
            create_geom_polygons(
                xr_coords,
                shpObj['shp'],
                params['shpField'],
                poly
            )
        ]
    tmp = _extract_polygons_dailydata(
        xr_data, sindex, params
    )
    coords = table_coords_polygons(
            shpObj['shp'],
            params['shpField'],
            shpObj['polys']
        )

    return {
            'status': 0,
            'data': tmp['data'],
            'dates': tmp['dates'],
            'coords': coords
        }

def extract_rectangle_point_dailydata(params, bbox):
    xr_data = get_zarr_daily_dataset(params)
    xr_coords = get_coords_dataset(xr_data)

    sindex = get_bbox_latlon_index(xr_coords, bbox)
    if sindex is None:
        msg = 'Rectangle outside of data spatial range.'
        return {'status': -1, 'message': msg}

    xr_ds = xr_data.isel(
        lat=sindex['lat'],
        lon=sindex['lon']
    )
    xr_ds = xr_ds.mean(
        dim=['lon', 'lat'],
        skipna=True
    )
    out = _get_point_dailydata(xr_ds, params)
    out = pd.DataFrame(out)
    out = out.map(lambda x: f'{x:.2f}')
    clon = round((bbox['minLon'] + bbox['maxLon'])/2, 6)
    clat = round((bbox['minLat'] + bbox['maxLat'])/2, 6)
    coords = pd.DataFrame({
        'loc': ['Rectangle'],
        'lon': [clon],
        'lat': [clat]
    })
    dates = _format_dates_dailydata(params)

    return {
            'status': 0,
            'data': out,
            'dates': dates,
            'coords': coords
        }

def extract_multipoints_dailydata(params):
    if params['pointsSource'] == 'user':
        csvdata = read_user_csv_mpoints(
            params['pointsFile'],
            params['user']['username']
        )
    else:
        csvdata = format_list_mpoints_dict(
            params['pointsList']
        )

    xr_data = get_zarr_daily_dataset(params)
    xr_coords = get_coords_dataset(xr_data)

    sindex = create_geom_mpoints_bbox(
        xr_coords,
        csvdata,
        params['padLon'],
        params['padLat']
    )
    ret = _extract_points_dailydata(
        xr_data, sindex, params
    )

    return {
            'status': 0,
            'data': ret['data'],
            'dates': ret['dates'],
            'coords': csvdata
        }

def extract_geojson_points_dailydata(params):
    if params['geojsonSource'] == 'user':
        json_data = get_user_geojson(
            params['geojsonFile'],
            params['user']['username']
        )
        if json_data['status'] == -1: return json_data
        json_data = json.loads(json_data['geojson'])
    else:
        json_data = params['geojsonData']

    geojson = gpd.GeoDataFrame.from_features(json_data['features'])
    geojson = geojson_polygons_points(geojson)
    if geojson['status'] == -1: return geojson

    xr_data = get_zarr_daily_dataset(params)
    xr_coords = get_coords_dataset(xr_data)

    geojson = geojson['geojson']
    geom_type = list(geojson.geom_type)
    pls = ['Polygon' in s for s in geom_type]
    pts = ['Point' in s for s in geom_type]

    if any(pls):
        geom_polygons = geojson[pls]
        name = geom_polygons[params['geojsonField']].reset_index(drop=True)
        gt = pd.DataFrame(data={'type': ['polygon' for i in name]})
        crds = geom_polygons.centroid.get_coordinates().reset_index(drop=True)
        ncrds = dict(zip(crds.columns.tolist(), ['lon', 'lat']))
        crds = crds.rename(columns=ncrds)
        info_polygons = pd.concat([name, crds, gt], axis=1)

        npolys = name.tolist()
        params['Poly'] = remove_duplicates_list(npolys)
        geom_polygons.crs = 'EPSG:4326'

        sindex = []
        for poly in params['Poly']:
            sindex += [
                create_geom_polygons(
                    xr_coords,
                    geom_polygons,
                    params['geojsonField'],
                    poly
                )
            ]

        ret = _extract_polygons_dailydata(
            xr_data, sindex, params
        )
        out_polygons = ret['data']
        dates_polygons = ret['dates']

    if any(pts):
        geom_points = geojson[pts]
        name = geom_points[params['geojsonField']].reset_index(drop=True)
        gt = pd.DataFrame(data={'type': ['point' for i in name]})
        crds = geom_points.get_coordinates().reset_index(drop=True)
        ncrds = dict(zip(crds.columns.tolist(), ['lon', 'lat']))
        crds = crds.rename(columns=ncrds)
        info_points = pd.concat([name, crds, gt], axis=1)

        sindex = create_geom_mpoints_bbox(
            xr_coords,
            info_points,
            0, 0
        )
        ret = _extract_points_dailydata(
            xr_data, sindex, params
        )
        out_points = ret['data']
        dates_points = ret['dates']

    if any(pls) and any(pts):
        out = pd.concat(
            [out_polygons, out_points],
            axis=1
        )
        out = out.T.reset_index(drop=True).T
        crds = pd.concat(
            [info_polygons, info_points],
            axis=0
        )
        crds = crds.reset_index(drop=True)
        dates = dates_points
    elif any(pls):
        out = out_polygons
        crds = info_polygons
        dates = dates_polygons
    elif any(pts):
        out = out_points
        crds = info_points
        dates = dates_points
    else:
        msg = 'No polygons or points found.'
        return {'status': -1, 'message': msg}

    crds['lon'] = crds['lon'].round(6)
    crds['lat'] = crds['lat'].round(6)
    crds.columns = ['loc', 'lon', 'lat', 'type']

    return {
            'status': 0,
            'data': out,
            'dates': dates,
            'coords': crds
        }

def _get_point_dailydata(xr_ds, params):
    tindex = get_daily_index_season(
        xr_ds['time'].values,
        params['startMonth'],
        params['startDay'],
        params['endMonth'],
        params['endDay']
    )
    year1 = int(params['startDate'])
    year2 = int(params['endDate'])
    tindex = {
        n: {
            k: tindex[n][k]
            for k in tindex[n]
            if k >= year1 and k <= year2
        }
        for n in tindex
    }

    out_data = []
    for y in range(year1, year2 + 1):
        index = tindex['index'][y]
        frac = tindex['length'][y]['frac']
        if frac < params['minFrac']:
            out_data += [np.nan]
            continue
        nb_seas = tindex['length'][y]['nb_seas']
        out = aggregate_daily_analysis(
            xr_ds, params, index, nb_seas
        )
        out_arr = out.to_array()
        out_arr = out_arr.to_numpy()
        out_data += [out_arr.item()]

    out = np.array(out_data)
    out[np.isnan(out)] = -99
    return out

def _get_grid_dailydata(xr_ds, params):
    tindex = get_daily_index_season(
        xr_ds['time'].values,
        params['startMonth'],
        params['startDay'],
        params['endMonth'],
        params['endDay']
    )
    index = tindex['index'][params['Year']]
    frac = tindex['length'][params['Year']]['frac']
    if frac < params['minFrac']:
        msg = 'Not enough data to compute the seasonal parameter'
        return {'status': -1, 'message': msg}
    nb_seas = tindex['length'][params['Year']]['nb_seas']
    out = aggregate_daily_analysis(
        xr_ds, params, index, nb_seas
    )

    out_arr = out.to_array()
    out_arr = out_arr.to_numpy()
    out_arr = np.squeeze(out_arr)
    out_arr = np.ma.masked_invalid(out_arr)
    out_arr.fill_value = -99
    out_data = {
        'lon': np.round(out.lon.values, 6),
        'lat': np.round(out.lat.values, 6),
        'data': out_arr
    }
    out_data['date'] = params['Year']

    return {'status': 0, 'data': out_data}

def _extract_polygons_dailydata(xr_data, sindex, params):
    year1 = int(params['startDate'])
    year2 = int(params['endDate'])
    nb_year = year2 - year1 + 1

    out = []
    for s in sindex:
        if len(s[0]) == 0:
            out += [np.repeat(-99, nb_year)]
            continue

        ix = xr.DataArray(s[1], dims='points')
        iy = xr.DataArray(s[0], dims='points')
        xr_ds = xr_data.isel(lat=iy, lon=ix)
        xr_ds = xr_ds.mean(dim='points', skipna=True)
        out += [_get_point_dailydata(xr_ds, params)]

    out = pd.DataFrame(out).transpose()
    out = out.map(lambda x: f'{x:.2f}')
    dates = _format_dates_dailydata(params)
    return {'dates': dates, 'data': out}

def _extract_points_dailydata(xr_data, sindex, params):
    year1 = int(params['startDate'])
    year2 = int(params['endDate'])
    nb_year = year2 - year1 + 1

    out = []
    for s in sindex:
        if len(s[0]) == 0:
            out += [np.repeat(-99, nb_year)]
            continue

        xr_ds = xr_data.isel(
            lat=s[1],
            lon=s[0]
        )
        xr_ds = xr_ds.mean(
            dim=['lon', 'lat'],
            skipna=True
        )
        out += [_get_point_dailydata(xr_ds, params)]

    out = pd.DataFrame(out).transpose()
    out = out.map(lambda x: f'{x:.2f}')
    dates = _format_dates_dailydata(params)
    return {'dates': dates, 'data': out}

def _format_dates_dailydata(params):
    s_year = int(params['startDate'])
    s_mon = int(params['startMonth'])
    s_day = int(params['startDay'])
    e_year = int(params['endDate'])
    e_mon = int(params['endMonth'])
    e_day = int(params['endDay'])

    same_year_seas = (s_mon, s_day) <= (e_mon, e_day)
    seq_year1 = [y for y in range(s_year, e_year + 1)]
    if same_year_seas:
        seq_year2 = seq_year1
    else:
        seq_year2 = [y + 1 for y in seq_year1]

    dates = []
    for s, e in zip(seq_year1, seq_year2):
        start = f'{s}-{s_mon:02d}-{s_day:02d}'
        end = f'{e}-{e_mon:02d}-{e_day:02d}'
        dates += [f'{start}_{end}']

    return dates
