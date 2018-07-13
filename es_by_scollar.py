#!/usr/bin/python

from collections import defaultdict
from datetime import datetime
import os
import sys
import time
import ujson as json

from elasticsearch import Elasticsearch
import pyarrow as pa
import pyarrow.parquet as pq


DAY_SECS = 60 * 60 * 24
ES_CONF = [{'host': '127.0.0.1', 'port': 20042}]
ES_OPTS = {
    'timeout': 60,
    'max_retries': 1,
    'retry_on_status': (500, 502, 503, 504),
    'retry_on_timeout': 2
    }
INDEX_NAME = 'dfi_flow'
STORED_FIELDS = ['flow_id_str', 'ip_src', 'ip_dst', 'port_src', 'port_dst',
                 'tcp_flags_0', 'start_time', 'end_time', 'duration', 'proto',
                 'total_byte_cnt_0', 'total_pkt_cnt_0']
TIME_FIELD = 'end_time'
SCROLL_TIMEOUT = '2m'
SCROLL_SIZE = 100

# strange behavior: uint16/uint8 will cause segmentation fault
PQ_TYPES = {
    'flow_id': pa.int64(),
    'net_src': pa.string(),
    'ip_src': pa.string(),
    'ip_dst': pa.string(),
    'port_src': pa.int64(),
    'port_dst': pa.int64(),
    'tcp_flags_0': pa.int64(),
    'start_time': pa.int64(),
    'end_time': pa.int64(),
    'duration': pa.int64(),
    'proto': pa.int64(),
    'total_byte_cnt_0': pa.int64(),
    'total_pkt_cnt_0': pa.int64()
}
PQ_SCHEMA = pa.schema([pa.field(it, PQ_TYPES[it]) for it in PQ_TYPES.keys()])


def timestamps_to_index_dates(start_ts, end_ts, margin=600):

    def ts_to_day_start(ts):
        return int(time.mktime(datetime.fromtimestamp(ts).date().timetuple()))

    start_day_ts = ts_to_day_start(start_ts - margin)
    end_day_ts = ts_to_day_start(end_ts + margin)
    gen = xrange(start_day_ts, end_day_ts + DAY_SECS, DAY_SECS)
    return [datetime.fromtimestamp(it).strftime('%y%m%d00') for it in gen]


def get_flow_batch(start_ts, end_ts):
    es_client = Elasticsearch(ES_CONF, **ES_OPTS)
    indices = ','.join([INDEX_NAME + '__0_*_' + it
                        for it in timestamps_to_index_dates(start_ts, end_ts)])
    query_body = {
        'query': {
                    'range': {
                        TIME_FIELD: {
                            'gte': start_ts,
                            'lt': end_ts
                        }
                    }
        },
        'stored_fields': STORED_FIELDS
    }
    ret = es_client.search(index=indices, body=query_body,
                           scroll=SCROLL_TIMEOUT, size=SCROLL_SIZE)
    while True:
        sid = ret['_scroll_id']
        size = len(ret['hits']['hits'])
        if size == 0:
            break
        yield ret['hits']['hits']
        ret = es_client.scroll(scroll_id=sid, scroll=SCROLL_TIMEOUT)
    es_client.clear_scroll(scroll_id=sid)


def get_1m_flow_batch(ts):
    for it in get_flow_batch(ts, ts + 60):
        yield it


def es_batch_to_pq_table(batch):

    def ip_to_net(ip):
        if ip is None:
            return None
        else:
            return ip[0:it.rfind('.')] + '.0'

    columns = defaultdict(list)
    for it in batch:
        for field in STORED_FIELDS:
            columns[field].append(it['fields'].get(field, [None])[0])
    columns['flow_id'] = [int(it) for it in columns['flow_id_str']]
    del columns['flow_id_str']
    columns['net_src'] = [ip_to_net(it) for it in columns['ip_src']]
    columns['start_time'] = [int(it) for it in columns['start_time']]
    columns['end_time'] = [int(it) for it in columns['end_time']]
    data = [pa.array(columns[it]) for it in PQ_TYPES.keys()]
    return pa.Table.from_arrays(data, schema=PQ_SCHEMA)


def write_1m_flow_to_parquet(ts, datas_dir=""):
    ts = ts // 60 * 60
    filename = datetime.fromtimestamp(ts).strftime('%y%m%d%H%M') + '.parquet'
    filepath = os.path.join(datas_dir, filename)
    pqwriter = pq.ParquetWriter(filepath, PQ_SCHEMA, flavor='spark')
    for it in get_1m_flow_batch(ts):
        table = es_batch_to_pq_table(it)
        pqwriter.write_table(table)
    pqwriter.close()


if __name__ == '__main__':
    if len(sys.argv) < 2:
        sys.exit(-1)
    else:
        write_1m_flow_to_parquet(int(sys.argv[1]))
