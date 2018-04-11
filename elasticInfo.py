#——*——coding:utf-8
from elasticsearch import ElasticsearchException,Elasticsearch
from optparse import OptionParser
from sys import argv
from os.path import realpath


eshost = "localhost"
esport = 20042
es = Elasticsearch([{"host": eshost, "port": esport, "timeout": 2}])

printTitle = True       #是否打印标题

def printDict(mydict):
    global printTitle
    _keys = mydict.keys()
    _values = [mydict.get(_) for _ in _keys]
    _title = "|{:^}  "*len(_keys) + "|"
    if printTitle:
        print _title.format(*_keys)
        printTitle = False
    print _title.format(*_values)


def printToConsole(function):
    def wrapper(*args,**kwargs):
        results = function(*args,**kwargs)
        map(printDict,results)
        return results
    return wrapper

@printToConsole
def IsRightBoxType(_index,box_type):
    """
    :param _index:索引名称
    :param box_type: 索引节点类型 warm or hot
    :return: bool
    """
    return es.indices.get(index=_index,flat_settings=True).get(_index).get("settings").get("index.routing.allocation.require.box_type") == box_type

@printToConsole
def getIndexByType(indexname,boxtype):
    """
    :param boxtype: 索引的类型
    :return: list(dict)
    """
    return filter(lambda _index: IsRightBoxType(_index,box_type=boxtype),es.indices.get(index=indexname,flat_settings=True))

@printToConsole
def getMenUsedInfo(_indexname,_sort):
    """
    :param _indexname:  索引名称，支持正则
    :param _sort: 排序规则 字段:desc/asc
    :return: list(dict)
    """
    displayFields = ["index", "fielddata.memory_size", "query_cache.memory_size", "request_cache.memory_size",
                     "segments.memory", "store.size"]
    MemInfos = []
    if not isinstance(_indexname,list):
        MemInfos = es.cat.indices(index=_indexname, bytes="k", format="json",s=_sort, v=True, h=displayFields)
    else:
        map(lambda _index:MemInfos.extend(
            es.cat.indices(index=_index, bytes="k", format="json", s=_sort, v=True, h=displayFields)),
            _indexname)
    return MemInfos

@printToConsole
def getRecoveryProcess(_indexname,_sort):
    """
    :param indexname:
    :return:Recovery  list
    """
    RecoveryResults = []
    displayFields = ["index", "shard", "type", "files_recovered", "files_percent", "files_total",
                     "bytes_recovered", "bytes_percent", "bytes_total"]

    if not isinstance(_indexname, list):
        RecoveryResults = es.cat.recovery(index=_indexname, bytes="k", v=True,format="json", s=_sort, h=displayFields)
    else:
        map(lambda _index: RecoveryResults.extend(
            es.cat.recovery(index=_index, bytes="k", format="json", s=_sort, v=True, h=displayFields)),
            _indexname)

    return RecoveryResults

@printToConsole
def getUnassigned(_indexname,_sort):
    """

    :param _indexname:
    :return:
    """
    UnassignedReasons = []
    displayFields = ["index", "shard", "state", "node", "unassigned.reason", "unassigned.details"]

    if not isinstance(_indexname, list):
        UnassignedReasons = es.cat.shards(index=_indexname,format="json",v=True,s=_sort,h=displayFields)
    else:
        map(lambda _index: UnassignedReasons.extend(
            es.cat.shards(index=_index, format="json", s=_sort, v=True, h=displayFields)),
            _indexname)

    return UnassignedReasons


def Main():
    usage = "Usage: %prog [-m|-r|-u] <-i indexname> < -t boxtype> <-s key:desc|asc>"
    parser = OptionParser(usage=usage)
    parser.add_option("-i","--index",action="store",
                      dest="index", default=False,
                      help="Elasricsearch index name")
    parser.add_option('-t',"--type",action="store",
                      dest="type",default=False,
                      help="the box type of index,eg: hot or warm")
    parser.add_option('-s','--sort',action="store",
                      dest="sort",default=False,
                      help="eg: index_name:[desc|asc]")
    parser.add_option("-m","--mem",action="store_true",
                      dest="mem",default=False,
                      help="display mem used info")
    parser.add_option("-r","--revovery",action="store_true",
                      dest="recovery",default=False,
                      help="display the discovery process")
    parser.add_option("-u","--unassigned",action="store_true",
                      dest="unassigned",default=False,
                      help="display details for shard unassigned reason")
    (option,args) = parser.parse_args()


    _specIndexName = "_all" if not option.index else option.index         #索引名称
    _sort = None if not option.sort else option.sort                #指定p排序规则
    _type = None if not option.type else option.type                   #指定索引存储类型

    _specIndexName = getIndexByType(indexname=_specIndexName,boxtype=_type) if _type else _specIndexName
    _specIndexName = "_all" if not _specIndexName else _specIndexName        #得到需要处理的索引名称

    if option.mem:
        result = getMenUsedInfo(_indexname=_specIndexName,_sort=_sort)
    elif option.recovery:
        result = getRecoveryProcess(_indexname=_specIndexName,_sort=_sort)
    elif option.unassigned:
        result = getUnassigned(_indexname=_specIndexName,_sort=_sort)
    else:
        print u"查看详细帮助"
        print "python {} -h".format(realpath(argv[0]))



Main()