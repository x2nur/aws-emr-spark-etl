import os
import boto3


metadata_tab = os.getenv('ETL_METADATA_TAB')
if not metadata_tab:
    raise NameError('Env var ETL_METADATA_TAB is not defined')
    

dynamodb = boto3.client("dynamodb")
val_field = "loaded-until"
pk_field = "etl-name"
etl_name = "sales"


def handler(event, context):
    dt = event['processed_date']
    
    st = f'SELECT "{val_field}" FROM "{metadata_tab}" WHERE "{pk_field}" = \'{etl_name}\''
    res = dynamodb.execute_statement(Statement=st)
    
    if res['Items']:
        old_dt = res['Items'][0][val_field]['S']
        st = f"UPDATE \"{metadata_tab}\" SET \"{val_field}\" = '{dt}' WHERE \"{pk_field}\" = '{etl_name}'"
        print(st)
        dynamodb.execute_statement(Statement=st)
    else:
        new_obj = {
            pk_field: etl_name,
            val_field : dt
        }
        st = f"INSERT INTO \"{metadata_tab}\" VALUE { new_obj }"
        dynamodb.execute_statement(Statement=st)
    
    return { 'success': True }
    
    
