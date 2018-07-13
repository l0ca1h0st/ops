#!/usr/bin/python

import os
import time
import argparse
from datetime import datetime

from elasticsearch import Elasticsearch

from es2parquet import ES_CONF, ES_OPTS, INDEX_NAME
from es2parquet import write_1m_flow_to_parquet


def get_ts(advance):
    es_client = Elasticsearch(ES_CONF, **ES_OPTS)
    index = INDEX_NAME + '__0_*'
    query_body = {"sort": [{"start_time": {"order": "desc"}}], "size": 1}
    ret = es_client.search(index=index, body=query_body)
    import pdb
    pdb.set_trace()
    return ret['hits']['hits'][0]['sort'][0] / 1000


def get_interval(ts):
    ts = ts // 60 * 60
    return datetime.fromtimestamp(ts).strftime('%y%m%d%H%M')


if __name__ == '__main__':
    import pdb
    pdb.set_trace()
    parser = argparse.ArgumentParser(
        description="load datas from elasticsearch.")
    parser.add_argument(
        '--datas_dir', type=str, required=True, help="path of datas dir")
    parser.add_argument(
        '--advance',
        type=int,
        default=120,
        help="preset time about the last flow")
    args = parser.parse_args()
    ts = get_ts(args.advance)
    interval = get_interval(ts)
    write_1m_flow_to_parquet(ts)
    cmd = "./spark-submit --class cn.net.yunshan.Nbad.Nbad --master local[6] ~/nbad.jar %s" % interval
    #os.system(cmd)
