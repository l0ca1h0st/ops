#_coding:utf-8
from elasticsearch import Elasticsearch
import sys
##################################ES配置信息##############################
eshost = "localhost"
esport = 20042
##########################################################################

resultIndex = []
#设置要查找的类型，默认为hot
box_type="hot" if  len(sys.argv) < 2  else sys.argv[1]


#################################连接ES获取ES实例#########################
def getEsInstance(eshot,esport):
    return Elasticsearch([{"host":eshost,"port":esport,"timeout":2}])

es = getEsInstance(eshost,esport)

###################################判断是否是hot还是warm类型###############
def IsRightBoxType(_index,box_type):
    return  es.indices.get(index=_index,flat_settings=True).get(_index).get("settings").get("index.routing.allocation.require.box_type") == box_type

###################################获取结果################################
resultIndex =  filter(lambda _index: IsRightBoxType(_index,box_type),es.indices.get("_all",flat_settings=True))


##################打印结果##################################################

for _index in resultIndex:
    print _index
