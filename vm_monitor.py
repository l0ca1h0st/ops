#_*_coding:utf-8

import pymongo
import commands
from optparse import OptionParser
import MySQLdb
import random


#mysql client
# localhost mysql监听地址   20130为mysql 监听的端口  根据实际情况修改   
mysqldb = MySQLdb.connect(host="localhost", user="root", port=20130, db="livecloud",passwd="security421" ,charset='utf8' )

# mongo client
# localhost 为mongo监听的地址  20011为mongo监听的端口  根据情况实际修改
conn = pymongo.MongoClient("localhost",20011)
mongo_db_name = "livecloud"
mongo_db = conn[mongo_db_name]

def get_all_vm(kvm=None):
    hostInformation = {}
    cursor = mysqldb.cursor()
    # 从mysql中获取所有的vm机
    cursor.execute("select id,name,launch_server from vm_v2_2 where os not like '%Topsec-vfw%' and os not like '%LoadBalancer%'")
    data = cursor.fetchall()
    [hostInformation.update({int(vmid):{"vmname": vmname,"kvm":kvmip}}) for (vmid,vmname,kvmip) in data] 
    return hostInformation


def get_vm_info(vmid):
    _vminfo = mongo_db.vm_load_v2_2.find({"vm_id":int(vmid)}).sort("timestamp",pymongo.DESCENDING).limit(1)
    try:
        _vminfo = _vminfo[0]
        _cpu = float("%.2f" % sum(_vminfo.get("cpu_usage")))
        _mem = float("%.2f" % _vminfo.get("mem_usage"))
    except:
       _cpu = float(0.00)
       _mem = float(0.00)
    return {"cpu":_cpu ,"mem":_mem}

def Main():
    usage = "Usage: %prog -k kvm -s key"
    parser = OptionParser(usage=usage)
    parser.add_option("-k","--kvm",action="store",
                      dest="kvm", default=False,
                      help=u"指定kvm上的虚拟机")
    parser.add_option('-s',"--sort",action="store",
                      dest="key",default=False,
                      help=u"指定排序的字段[mem|cpu]")
    (option,args) = parser.parse_args()


    _kvm = "\'\'" if not option.kvm else option.kvm
    _key = None if not option.key else option.key

    all_vm_list = get_all_vm(kvm=_kvm)
    all_vm_infos = {}

    for id,vm in all_vm_list.items():
        all_vm_infos[id] = dict(vm, **get_vm_info(id))
    # 如果指定key则对key进行排序，key支持对cpu字段排序和对mem字段排序
    if _key:
        all_vm_infos = sorted(all_vm_infos.items(), cmp=lambda value1,value2:cmp(value1.get(_key),value2.get(_key)),  key=lambda item: item[1],reverse=True)
    else:
        all_vm_infos = tuple(all_vm_infos.items())

    print "|%32s|%16s|%8s|%8s|" % ("vmname","kvm","mem","cpu")
    for id,vminfo in all_vm_infos:
        print "|%-32s|%16s|%8s|%8s|" % (vminfo.get("vmname"),vminfo.get("kvm"),vminfo.get("mem"),vminfo.get("cpu"))

Main()
