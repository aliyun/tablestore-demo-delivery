import cbor
from time import sleep
import random 
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

def extract_row_value(row):
    attrs = row.attribute_columns
    assert len(attrs) == 1
    attr = attrs[0]
    assert attr[0] == 'value'
    return attr[1]

def backoff(last_backoff, max_backoff):
    last_backoff *= 2
    if last_backoff > max_backoff:
        last_backoff = max_backoff
    real_backoff = 0.01 * random.randint(1, last_backoff)
    sleep(real_backoff)
    return last_backoff

def main(event, context):
    random.seed()
    incr = cbor.loads(event)
    LOGGER.info('payload %s', incr)
    otsc = new_ots_client(context)

    for city, inc_val in incr:
        city = codecs.encode(city, 'utf-8')
        last_backoff = 1
        max_backoff = 512
        while True:
            _, row, _ = otsc.get_row(cfg.OTS_BUBBLE_TABLE, 
                                     [('DestinationCity', city)], 
                                     columns_to_get=['value'], 
                                     max_version=1)
            old_val = extract_row_value(row)
            new_val = old_val + inc_val
            new_row = ots.Row([('DestinationCity', city)], {'put': [('value', new_val)]})
            try:
                _ = otsc.update_row(cfg.OTS_BUBBLE_TABLE,
                                    new_row,
                                    ots.Condition(ots.RowExistenceExpectation.IGNORE,
                                                  ots.SingleColumnCondition('value',
                                                                            old_val,
                                                                            ots.ComparatorType.EQUAL,
                                                                            pass_if_missing = False)))
                break
            except ots.OTSError as ex:
                last_backoff = backoff(last_backoff, max_backoff)
