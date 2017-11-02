import cbor
from datetime import datetime
import tablestore as ots
import codecs
import logging
import cfg

LOGGER = logging.getLogger()

def new_ots_client(context):
    cred = context.credentials
    return ots.OTSClient(cfg.OTS_ENDPOINT,
                         cred.access_key_id,
                         cred.access_key_secret,
                         cfg.OTS_INSTANCE,
                         sts_token=cred.security_token)

def align_timestamp(ts):
    return ts / 1000000 / 10 * 10

def to_rows(deliveries):
    deliveries = [('%s_%s' % (codecs.encode(x, 'utf-8'), codecs.encode(y, 'utf-8')),
                   align_timestamp(z))
                  for x,y,z in deliveries]
    rows = {}
    for x,y in deliveries:
        if y not in rows:
            rows[y] = set()
        rows[y].add(x)
    rows = [(x, list(y)) for x, y in rows.items()]
    return rows

def to_req(rows):
    cond = ots.Condition(ots.RowExistenceExpectation.IGNORE)
    rows = [ots.Row([('Timestamp', x)],
                    {'put': [(z, True) for z in y]})
            for x, y in rows]
    row_items = [ots.UpdateRowItem(x, cond) for x in rows]
    table_item = ots.TableInBatchWriteRowItem(cfg.OTS_FLYWIRE_TABLE, row_items)
    req = ots.BatchWriteRowRequest()
    req.add(table_item)
    return req

def main(event, context):
    deliveries = cbor.loads(event)
    LOGGER.info('deliveries: %s', deliveries)
    
    otsc = new_ots_client(context)

    rows = to_rows(deliveries)
    req = to_req(rows)
    resp = otsc.batch_write_row(req)
    puts = resp.get_put()
    for _, fails in puts:
        for x in fails:
            LOGGER.error('put error: %s', x)
