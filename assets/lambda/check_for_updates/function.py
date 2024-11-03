import os
import boto3


metadata_tab = os.getenv('ETL_METADATA_TAB')
if not metadata_tab:
    raise NameError('Env var ETL_METADATA_TAB is not defined')


base_path = os.getenv('CSV_PATH')
if not base_path:
    raise NameError('Env var CSV_PATH is not defined')

    
dynamodb = boto3.client("dynamodb")
pk_field = "etl-name"
val_field = "loaded-until"

st = f'SELECT "{val_field}" FROM "{metadata_tab}" WHERE "{pk_field}" = \'sales\''

# simulation dates
dates = [
    '2016-01-01',
    '2017-01-01',
    '2018-01-01',
    '2019-01-01',
    '2020-01-01',
]


def handler(event, context):
    res = dynamodb.execute_statement(Statement=st)
    
    dt = None
    nxt_dt = None
    
    if res["Items"]:
        dt = res["Items"][0][val_field]["S"]
        
    if dt:
        # get next date from dates
        idx = next((i for i,d in enumerate(dates) if d == dt), None)
        nxt_dt = dates[idx + 1] if idx is not None and len(dates) > idx + 1 else None 
    else:
        nxt_dt = dates[0]
        
    if not nxt_dt:
        resp = {
            'nodata': True # end simulation
        }
    else:
        path = f"{base_path}/{nxt_dt}" # next
        
        resp = {
            'nodata': False,
            'data': {
                'users_csv': f"{path}/users.csv",
                'products_csv': f"{path}/products.csv",
                'orders_csv': f"{path}/orders.csv"
            },
            'date': nxt_dt
        }

    resp |= event
    
    return resp