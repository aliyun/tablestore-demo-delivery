import tablestore as ots
from datetime import datetime
import json
import cfg

LOCATIONS = {
    'Beijing': '116.46,39.92',
    'Shanghai': '121.29,31.13',
    'Chengdu': '104.06,30.67',
    'Shenzhen': '114.06,22.55'}

def new_ots_client(context):
    cred = context.credentials
    return ots.OTSClient(cfg.OTS_ENDPOINT,
                         cred.access_key_id,
                         cred.access_key_secret,
                         cfg.OTS_INSTANCE,
                         sts_token=cred.security_token)

def main(event, context):
    otsc = new_ots_client(context)
    now = datetime.now() - datetime(1970, 1, 1)
    aligned_now = (int(now.total_seconds())  / 10 - 1) * 10 
    _, row, _ = otsc.get_row(cfg.OTS_FLYWIRE_TABLE, 
                             [('Timestamp', aligned_now)], 
                             max_version=1)
    result = {"isBase64Encoded": False, 
              "statusCode": 200, 
              "headers": {"Content-Type": "application/json"}, 
              "body": []}
    if row is None:
        return json.dumps(result)
    else:
        attrs = row.attribute_columns
        names = [x[0] for x in attrs]
        cities = [x.split('_') for x in names]
        locs = [{"from": LOCATIONS[x], "to": LOCATIONS[y]} for x,y in cities]
        result["body"] = locs
        return json.dumps(result)
