import cbor
import fc
from datetime import datetime
import codecs
import logging
import tablestore as ots
import cfg

LOGGER = logging.getLogger()

def new_ots_client(context):
    cred = context.credentials
    return ots.OTSClient(cfg.OTS_ENDPOINT,
                         cred.access_key_id,
                         cred.access_key_secret,
                         cfg.OTS_INSTANCE,
                         sts_token=cred.security_token)

def new_fc_client(context):
    cred = context.credentials
    return fc.Client(endpoint=cfg.FC_ENDPOINT,
                     accessKeyID=cred.access_key_id,
                     accessKeySecret=cred.access_key_secret,
                     securityToken=cred.security_token)

# a record looks like:
# {u'Info': {u'Timestamp': 1506667478896000},
#  u'Type': u'PutRow', 
#  u'PrimaryKey': [{u'ColumnName': u'DeliveryId', u'Value': u'e006bea0-4794-4153-a468-3306232096bc'}, 
#                  {u'ColumnName': u'SeqNum', u'Value': 1506667478897000}], 
#  u'Columns': [{u'ColumnName': u'OpType', u'Type': u'Put', u'Value': u'NewDelivery', u'Timestamp': 1506667478897}, 
#               {u'ColumnName': u'Scanner', u'Type': u'Put', u'Value': u'2b985a71-1908-45d8-8e34-f8d528ae916d', u'Timestamp': 1506667478897}]}

def op_type(record):
    attrs = record[u'Columns']
    for x in attrs:
        if x[u'ColumnName'] == u'OpType':
            return x[u'Value']
    assert False, Exception('Column "OpType" is required.')
    return None

class DeliveryInfo(object):
    def __init__(self):
        self.source_city = ''
        self.destination_city = ''

def fetch_delivery_info(otsc, deliveryId):
    _, row, _ = otsc.get_row(
        cfg.OTS_PACKAGE_INFO, 
        [('DeliveryId', deliveryId)], 
        max_version=1)
    attrs = row.attribute_columns
    assert len(attrs) >= 2
    attrs = [tuple(x) for x in attrs]
    res = DeliveryInfo()
    for name, value, _ in attrs:
        if name == 'SourceCity':
            res.source_city = value
        elif name == 'DestinationCity':
            res.destination_city = value
    return res

def flywire(otsc, fcc, records):
    if len(records) == 0:
        return
    xs = [{'Timestamp': x[u'Info'][u'Timestamp'],
           'DeliveryId': x[u'PrimaryKey'][0][u'Value'],
           'OpType': op_type(x)} \
          for x in records]
    xs = [x for x in xs if x['OpType'] == u'SignIn']
    deliveries = set() # pairs from source city to destination city
    for x in xs:
        deli_id = codecs.encode(x['DeliveryId'], 'utf-8')
        delivery = fetch_delivery_info(otsc, deli_id)
        deliveries.add((delivery.source_city,
                        delivery.destination_city,
                        x['Timestamp']))
    deliveries = list(deliveries)
    LOGGER.info('flywire paylod: %s', deliveries)
    if len(deliveries) > 0:
        fcc.async_invoke_function(
            cfg.FC_SERVICE,
            'flywireUpdater',
            payload=cbor.dumps(deliveries))
    
def on_delivery(otsc, fcc, records):
    xs = [{'DeliveryId': x[u'PrimaryKey'][0][u'Value'],
           'OpType': op_type(x)} \
          for x in records]
    xs = [x for x in xs if x['OpType'] in [u'SignIn', u'SignOff']]
    acc = {}
    for x in xs:
        deli_id = codecs.encode(x['DeliveryId'], 'utf-8')
        delivery = fetch_delivery_info(otsc, deli_id)
        dest = delivery.destination_city
        if dest not in acc:
            acc[dest] = 0
        if x['OpType'] == u'SignIn':
            acc[dest] += 1
        elif x['OpType'] == u'SignOff':
            acc[dest] -= 1
    acc = [(k, v) for k, v in acc.items() if v != 0]
    LOGGER.info('accumulator paylod: %s', acc)
    if len(acc) > 0:
        fcc.async_invoke_function(
            cfg.FC_SERVICE,
            'accumulator',
            payload=cbor.dumps(acc))
    
def main(event, context):
    records = cbor.loads(event)[u'Records']
    otsc = new_ots_client(context)
    fcc = new_fc_client(context)

    flywire(otsc, fcc, records)
    on_delivery(otsc, fcc, records)
