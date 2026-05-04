import yaml
import io
from flask import make_response, jsonify

def load_yaml_file(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        try:
            conf = yaml.safe_load(f)
        except yaml.YAMLError as e:
            print(f'Error {e}')

    return conf

def format_get_request(args):
    params = args.to_dict(flat = False)
    pr = dict()
    for key, value in params.items():
        pr[key] = value if len(value) > 1 else value[0]

    return pr

def convert2json(cursor):
    row_headers = [x[0] for x in cursor.description]
    result = cursor.fetchall()
    json_data = []
    for res in result:
        json_data.append(dict(zip(row_headers, res)))

    return json_data

def convert_dict2csv(csv_dict):
    header = [','.join(map(str, list(csv_dict[0].keys())))]
    csv_str = [','.join(map(str, list(obj.values()))) for obj in csv_dict]
    csv_str = header + csv_str
    csv_str = '\n'.join(csv_str)
    return csv_str

def split_list(x, f):
    y = []
    for ix in sorted(list(set(f))):
        tx = (i for i, j in enumerate(f) if j == ix)
        y += [[x[k] for k in tx]]

    return y

def split_dict(x, f):
    y = {}
    for ix in sorted(list(set(f))):
        tx = (i for i, j in enumerate(f) if j == ix)
        y[ix] = [x[k] for k in tx]

    return y

def remove_duplicates_list(x):
    # respect order
    y = [j for i, j in enumerate(x) if j not in x[:i]]
    # y = list(dict.fromkeys(x))
    # not respecting order
    # y = list(set(x))
    return y

def read_binary_file(filename):
    with open(filename, 'rb') as b:
        buf = io.BytesIO(b.read())

    return buf

def response_download_file(data, filename, mimetype):
    response = make_response(data, 200)
    response.mimetype = mimetype
    response.status_code = 200
    if filename is not None:
        response.headers['Content-Disposition'] = f'attachment; filename={filename}'
    response.headers['API-Service'] = 'ENACTS Data Sharing Tool'
    return response

def response_download_error(message, filename, code=422):
    response = make_response(jsonify({'message': message, 'status': -1}), code)
    response.mimetype = 'application/json'
    response.status_code = code
    if filename is not None:
        response.headers['Content-Disposition'] = f'attachment; filename={filename}.json'
    response.headers['API-Service'] = 'ENACTS Data Sharing Tool'
    return response

def response_download_json(data, filename=None):
    mimetype = 'application/json'
    if filename is not None:
        filename = f'{filename}.json'
    return response_download_file(data, filename, mimetype)

