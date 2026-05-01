from datetime import datetime as dt

def checkParamsRequest_rawdata(params):
    dataset = _checkParamsKey(params, 'dataset', ['ALL', 'MON'])
    if dataset: return dataset

    # change to read directly from app/yaml/datasets-config.yaml
    tmpR = ['daily', 'dekadal', 'monthly', 'seasonal', 'annual']
    temporalRes = _checkParamsKey(params, 'temporalRes', tmpR)
    if temporalRes: return temporalRes

    # change to read directly from app/yaml/datasets-config.yaml
    params_available = ['precip', 'tmax', 'tmin', 'tmean', 'pmsl', 'pres', 'rad',
                        'rhmean', 'rhmax', 'rhmin', 'wspd', 'wdir', 'et0']
    variable = _checkParamsKey(params, 'variable', params_available)
    if variable: return variable

    outF = ['CSV-CDT-Format', 'JSON-Format', 'netCDF-Format', 'CSV-Column-Format']
    outFormat = _checkParamsKey(params, 'outFormat', outF)
    if outFormat: return outFormat

    geom_params = _checkExtractSupport(params)
    if geom_params['ret']: return geom_params['ret']
    params = geom_params['params']

    params['gridded'] = _isExtractGrid(params)

    ### 
    if not params['gridded']:
        startDate = _checkParamsKey(params, 'startDate')
        if startDate: return startDate
        endDate = _checkParamsKey(params, 'endDate')
        if endDate: return endDate

        if params['temporalRes'] == 'daily':
            st = _checkDateDaily('startDate', params['startDate'])
            if st: return st
            et = _checkDateDaily('endDate', params['endDate'])
            if et: return et
        elif params['temporalRes'] == 'dekadal':
            st = _checkDateDekadal('startDate', params['startDate'])
            if st: return st
            et = _checkDateDekadal('endDate', params['endDate'])
            if et: return et
        elif params['temporalRes'] == 'monthly':
            st = _checkDateMonthly('startDate', params['startDate'])
            if st: return st
            et = _checkDateMonthly('endDate', params['endDate'])
            if et: return et
        elif params['temporalRes'] == 'seasonal':
            st = _checkDateYear('startDate', params['startDate'])
            if st: return st
            et = _checkDateYear('endDate', params['endDate'])
            if et: return et

            seasStart = _checkseasonStartLength(params, 'seasStart', 1, 12)
            if seasStart: return seasStart
            seasLength = _checkseasonStartLength(params, 'seasLength', 2, 12)
            if seasLength: return seasLength
        else:
            st = _checkDateYear('startDate', params['startDate'])
            if st: return st
            et = _checkDateYear('endDate', params['endDate'])
            if et: return et
    else:
        Date = _checkParamsKey(params, 'Date')
        if Date: return Date

        if params['temporalRes'] == 'daily':
            st = _checkDateDaily('Date', params['Date'])
            if st: return st
        elif params['temporalRes'] == 'dekadal':
            st = _checkDateDekadal('Date', params['Date'])
            if st: return st
        elif params['temporalRes'] == 'monthly':
            st = _checkDateMonthly('Date', params['Date'])
            if st: return st
        elif params['temporalRes'] == 'seasonal':
            st = _checkDateSeasonal('Date', params['Date'])
            if st: return st
        else:
            st = _checkDateYear('Date', params['Date'])
            if st: return st

    webApp = _checkParamBoolean(params, 'webApp', False)
    if webApp['status'] == -1: return {'ret': webApp}
    params = webApp['params']

    params['finalOutput'] = True

    return {'status': 0, 'params': params}

def checkParamsRequest_climatology(params):
    dataset = _checkParamsKey(params, 'dataset', ['ALL', 'MON'])
    if dataset: return dataset

    # change to read directly from app/yaml/datasets-config.yaml
    tmpR = ['daily', 'dekadal', 'monthly', 'seasonal', 'annual']
    temporalRes = _checkParamsKey(params, 'temporalRes', tmpR)
    if temporalRes: return temporalRes

    # change to read directly from app/yaml/datasets-config.yaml
    params_available = ['precip', 'tmax', 'tmin', 'tmean', 'pmsl', 'pres', 'rad',
                        'rhmean', 'rhmax', 'rhmin', 'wspd', 'wdir', 'et0']
    variable = _checkParamsKey(params, 'variable', params_available)
    if variable: return variable

    outF = ['CSV-CDT-Format', 'JSON-Format', 'netCDF-Format', 'CSV-Column-Format']
    outFormat = _checkParamsKey(params, 'outFormat', outF)
    if outFormat: return outFormat

    geom_params = _checkExtractSupport(params)
    if geom_params['ret']: return geom_params['ret']
    params = geom_params['params']

    params['gridded'] = _isExtractGrid(params)

    ### 
    startYear = _checkDateYear('startYear', params['startYear'])
    if startYear: return startYear
    endYear = _checkDateYear('endYear', params['endYear'])
    if endYear: return endYear

    minYear = _checkParamInteger(params, 'minYear', 30)
    if minYear['status'] == -1: return minYear
    params = minYear['params']

    if params['temporalRes'] == 'seasonal':
        seasLength = _checkseasonStartLength(params, 'seasLength', 2, 12)
        if seasLength: return seasLength

    if params['temporalRes'] == 'daily':
        daysWindow = _checkParamInteger(params, 'daysWindow', 0)
        if daysWindow['status'] == -1: return daysWindow
        params = daysWindow['params']

    fullYear = _checkParamBoolean(params, 'fullYear', True)
    if fullYear['status'] == -1: return {'ret': fullYear}
    params = fullYear['params']
    if not params['fullYear']:
        if params['temporalRes'] != 'annual':
            climDate = _checkParamsKey(params, 'climDate')
            if climDate: return climDate

            if params['temporalRes'] == 'daily':
                val = f'2020-{params["climDate"]}'
                st = _checkDateDaily('climDate', val)
                if st: return st
                date = dt.strptime(val, '%Y-%m-%d')
                date = date.strftime('%m-%d')
                if date == '02-29':
                    msg = 'There is no climatology for 02-29'
                    return {'status': -1, 'message': msg}
            elif params['temporalRes'] == 'dekadal':
                val = f'2020-{params["climDate"]}'
                st = _checkDateDekadal('climDate', val)
                if st: return st
            elif params['temporalRes'] == 'monthly':
                val = f'2020-{params["climDate"]}'
                st = _checkDateMonthly('climDate', val)
                if st: return st
            elif params['temporalRes'] == 'seasonal':
                st = _checkseasonStartLength(params, 'climDate', 1, 12)
                if st: return st
            else:
                params['fullYear'] = True
        else:
            params['fullYear'] = True
    else:
        params['climDate'] = None

    climF = ['mean', 'median', 'min', 'max', 'stdev', 'percentile', 'cv', 'frequency', 'mean-stdev']
    climFunction = _checkParamsKey(params, 'climFunction', climF)
    if climFunction: return climFunction

    if params['climFunction'] == 'percentile':
        if type(params['precentileValue']) is list:
            if params['outFormat'] != 'CSV-CDT-Format':
                precentileValue = _checkParamFloatList(params, 'precentileValue')
                if precentileValue['status'] == -1: return precentileValue
            else:
                msg = 'Multiple percentiles can not be computed CDT for output format.'
                return {'status': -1, 'message': msg}
        else:
            precentileValue = _checkParamFloat(params, 'precentileValue')
            if precentileValue['status'] == -1: return precentileValue
        params = precentileValue['params']

    if params['climFunction'] == 'frequency':
        frequencyOper = _checkParamsKey(params, 'frequencyOper')
        if frequencyOper: return frequencyOper

        frequencyThres = _checkParamFloat(params, 'frequencyThres')
        if frequencyThres['status'] == -1: return frequencyThres
        params = frequencyThres['params']

    if params['climFunction'] == 'mean-stdev':
        if params['outFormat'] == 'CSV-CDT-Format':
            msg = 'The mean and stdev combined can not be computed CDT for output format.'
            return {'status': -1, 'message': msg}

    webApp = _checkParamBoolean(params, 'webApp', False)
    if webApp['status'] == -1: return {'ret': webApp}
    params = webApp['params']

    params['finalOutput'] = True

    return {'status': 0, 'params': params}

def checkParamsRequest_analysis(params):
    if params['analysis'] == 'anomaly':
        anomtype = ['difference', 'percentage', 'standardized']
        anomaly = _checkParamsKey(params, 'anomaly', anomtype)
        if anomaly: return anomaly
        tseries = checkParamsRequest_rawdata(params)
        if tseries['status'] == -1: return tseries
        params = tseries['params']
        params['climFunction'] = 'mean-stdev'
        params['fullYear'] = True
        params['outFormat_0'] = params['outFormat']
        params['outFormat'] = 'JSON-Format'
        climato = checkParamsRequest_climatology(params)
        if climato['status'] == -1: return climato
        params = climato['params']
        params['webApp'] = True
        params['finalOutput'] = False
    elif params['analysis'] == 'spi':
        return {'status': -1, 'message': 'Not implemented yet'}
    else:
        return {'status': -1, 'message': 'Unknown analysis type'}

    return {'status': 0, 'params': params}

########################

def _isExtractGrid(params):
    grid1 = params['geomExtract'] in ['rectangle', 'polygons']
    if grid1:
        if 'spatialAvg' in params:
            grid1 = grid1 and not params['spatialAvg']
        else:
            grid1 = False
    grid2 = params['geomExtract'] == 'original'
    return grid1 or grid2

def _checkExtractSupport(params):
    geomE = ['original', 'points', 'rectangle', 'polygons', 'geojson']
    geomExtract = _checkParamsKey(params, 'geomExtract', geomE)
    if geomExtract: return {'ret': geomExtract}

    if params['geomExtract'] in ['rectangle', 'polygons']:
        spatialAvg = _checkParamBoolean(params, 'spatialAvg', False)
        if spatialAvg['status'] == -1: return {'ret': spatialAvg}
        params = spatialAvg['params']

    if params['geomExtract'] == 'points':
        pointsSource = _checkParamsKey(params, 'pointsSource', ['user', 'upload'])
        if pointsSource: return {'ret': pointsSource}

        if params['pointsSource'] == 'user':
            pointsFile = _checkParamsKey(params, 'pointsFile')
            if pointsFile: return {'ret': pointsFile}

        if params['pointsSource'] == 'upload':
            pointsList = _checkParamsKey(params, 'pointsList')
            if pointsList: return {'ret': pointsList}

        padLon = _checkParamInteger(params, 'padLon', 0)
        if padLon['status'] == -1: return {'ret': padLon}
        params = padLon['params']

        padLat = _checkParamInteger(params, 'padLat', 0)
        if padLat['status'] == -1: return {'ret': padLat}
        params = padLat['params']

    elif params['geomExtract'] == 'rectangle':
        minLon = _checkParamFloat(params, 'minLon')
        if minLon['status'] == -1: return {'ret': minLon}
        params = minLon['params']

        maxLon = _checkParamFloat(params, 'maxLon')
        if maxLon['status'] == -1: return {'ret': maxLon}
        params = maxLon['params']

        minLat = _checkParamFloat(params, 'minLat')
        if minLat['status'] == -1: return {'ret': minLat}
        params = minLat['params']

        maxLat = _checkParamFloat(params, 'maxLat')
        if maxLat['status'] == -1: return {'ret': maxLat}
        params = maxLat['params']

    elif params['geomExtract'] == 'polygons':
        shpSource = _checkParamsKey(params, 'shpSource', ['user', 'default'])
        if shpSource: return {'ret': shpSource}

        shpFile = _checkParamsKey(params, 'shpFile')
        if shpFile: return {'ret': shpFile}

        shpField = _checkParamsKey(params, 'shpField')
        if shpField: return {'ret': shpField}

        allPolygons = _checkParamBoolean(params, 'allPolygons', False)
        if allPolygons['status'] == -1: return {'ret': allPolygons}
        params = allPolygons['params']

        if not params['allPolygons']:
            Poly = _checkParamsKey(params, 'Poly')
            if Poly: return {'ret': Poly}

    elif params['geomExtract'] == 'geojson':
        geojsonSource = _checkParamsKey(params, 'geojsonSource', ['user', 'upload'])
        if geojsonSource: return {'ret': geojsonSource}

        if params['geojsonSource'] == 'user':
            geojsonFile = _checkParamsKey(params, 'geojsonFile')
            if geojsonFile: return {'ret': geojsonFile}

        if params['geojsonSource'] == 'upload':
            geojsonData = _checkParamsKey(params, 'geojsonData')
            if geojsonData: return {'ret': geojsonData}

        geojsonField = _checkParamsKey(params, 'geojsonField')
        if geojsonField: return {'ret': geojsonField}

    ## 
    out_grid = ['netCDF-Format', 'CSV-Column-Format', 'JSON-Format']
    out_point = ['CSV-CDT-Format', 'JSON-Format']
    out_json = 'JSON-Format'

    if params['geomExtract'] in ['rectangle', 'polygons']:
        if params['spatialAvg']:
            out_format = _checkOutFormat(params['outFormat'], out_point)
        else:
            out_format = _checkOutFormat(params['outFormat'], out_grid)
    elif params['geomExtract'] == 'points':
        out_format = _checkOutFormat(params['outFormat'], out_point)
    elif params['geomExtract'] == 'geojson':
        out_format = _checkOutFormat(params['outFormat'], out_json)
    else:
        out_format = _checkOutFormat(params['outFormat'], out_grid)
    
    if out_format: return {'ret': out_format}

    return {'params': params, 'ret': None}

########################

def _checkParamsKey(params, key, values=None):
    ret_error = {'status': -1, 'message': None}
    if not key in params:
        ret_error['message'] = f'No parameter <{key}> found.'
        return ret_error
    else:
        if values:
            if not params[key] in values:
                ret_error['message'] = f'Invalid parameter <{key}: {params[key]}>.'
                return ret_error
    return None

def _checkOutFormat(value, out_format):
    msg = f'Incorrect parameter <outFormat: {value}>.'
    ret_error = {'status': -1, 'message': msg}
    if type(out_format) is list:
        if not value in out_format: return ret_error
    else:
        if value != out_format: return ret_error
    return None

def _checkDateDaily(key, value):
    msg = f'Invalid parameter <{key}: {value}>.'
    ret_error = {'status': -1, 'message': msg}
    try:
        date = dt.strptime(value, '%Y-%m-%d')
        return None
    except Exception:
        return ret_error

def _checkDateDekadal(key, value):
    msg = f'Invalid parameter <{key}: {value}>.'
    ret_error = {'status': -1, 'message': msg}
    try:
        date = dt.strptime(value, '%Y-%m-%d')
        if date.day < 0 or date.day > 3: return ret_error
        return None
    except Exception:
        return ret_error

def _checkDateMonthly(key, value):
    msg = f'Invalid parameter <{key}: {value}>.'
    ret_error = {'status': -1, 'message': msg}
    try:
        date = dt.strptime(value, '%Y-%m')
        return None
    except Exception:
        return ret_error

def _checkDateYear(key, value):
    msg = f'Invalid parameter <{key}: {value}>.'
    ret_error = {'status': -1, 'message': msg}
    try:
        yr = int(value)
        if yr < 1000 or yr > 3000: return ret_error
        return None
    except Exception:
        return ret_error

def _checkDateSeasonal(key, value):
    msg = f'Invalid parameter <{key}: {value}>.'
    ret_error = {'status': -1, 'message': msg}
    try:
        seas = value.split('_')
        s1 = dt.strptime(seas[0], '%Y-%m')
        s2 = dt.strptime(seas[1], '%Y-%m')
        mo = (s2 - s1).days
        if mo < 30 or mo > 365: return ret_error
        return None
    except Exception:
        return ret_error

def _checkseasonStartLength(params, key, minv, maxv):
    ret_error = {'status': -1, 'message': None}
    if not key in params:
        ret_error['message'] = f'No parameter <{key}> found.'
        return ret_error
    else:
        ret_error['message'] = f'Invalid parameter <{key}: {params[key]}>.'
        try:
            s = int(params[key])
            if s < minv or s > maxv: return ret_error
            return None
        except Exception:
            return ret_error

def _checkParamBoolean(params, key, default=None):
    ret_error = {'status': -1, 'message': None}
    if not key in params:
        if default is not None:
            params[key] = default
            return {'status': 0, 'params': params}
        else:
            ret_error['message'] = f'No parameter <{key}> found.'
            return ret_error
    else:
        ret_error['message'] = f'Invalid parameter <{key}: {params[key]}>.'
        try:
            if isinstance(params[key], str):
                b = params[key].title()
                b = b[0:4] if b[0] == 'T' else b[0:5]
                try:
                    params[key] = eval(b)
                    return {'status': 0, 'params': params}
                except Exception:
                    return ret_error
            else:
                if not isinstance(params[key], bool):
                    return ret_error
                else:
                    return {'status': 0, 'params': params}
        except Exception:
            return ret_error

def _checkParamInteger(params, key, default=None):
    ret_error = {'status': -1, 'message': None}
    if not key in params:
        if default is not None:
            params[key] = default
            return {'status': 0, 'params': params}
        else:
            ret_error['message'] = f'No parameter <{key}> found.'
            return ret_error
    else:
        ret_error['message'] = f'Invalid parameter <{key}: {params[key]}>.'
        try:
            if isinstance(params[key], str):
                params[key] = int(params[key])
                return {'status': 0, 'params': params}
            else:
                if not isinstance(params[key], int):
                    return ret_error
                else:
                    return {'status': 0, 'params': params}
        except Exception:
            return ret_error

def _checkParamFloat(params, key):
    ret_error = {'status': -1, 'message': None}
    if not key in params:
        ret_error['message'] = f'No parameter <{key}> found.'
        return ret_error
    else:
        try:
            params[key] = float(params[key])
            return {'status': 0, 'params': params}
        except Exception:
            ret_error['message'] = f'Invalid parameter <{key}: {value}>.'
            return ret_error

def _checkParamFloatList(params, key):
    ret_error = {'status': -1, 'message': None}
    if not key in params:
        ret_error['message'] = f'No parameter <{key}> found.'
        return ret_error
    else:
        try:
            params[key] = [float(p) for p in params[key]]
            return {'status': 0, 'params': params}
        except Exception:
            ret_error['message'] = f'Invalid parameter <{key}: {value}>.'
            return ret_error
