#_*_coding:utf-8


from influxdb import InfluxDBClient
from optparse import OptionParser


InfluxdbHost = "10.26.1.3"
InfluxdbPort = "20044"
InfluxdbDB = "telegraf"



client = InfluxDBClient(InfluxdbHost,InfluxdbPort,"root","",InfluxdbDB)

def toKb(bytes):
    return str(bytes/1024/1024)+'Mb'



def printDictCpu(mydict):
    #total
    totalinfo = mydict.get("all")
    for sigcpu in totalinfo:
        sigcpuinfo = totalinfo.get(sigcpu)
        print "|{:^20}|{:18}|{:12}|{:12}|{:14}|".format("Time","CpuSys","CpuUser","CpuIo","CpuIdel")
        for sig in sigcpuinfo:
            print "|{:12}|{:18.4}|{:12}|{:12}|{:14}|".format(sig.get("time"),sig.get("usage_system"),sig.get("usage_guest"),sig.get("usage_iowait"),sig.get("usage_idle"))
    #avg


def printDictMem(mydict):
    
    #total
    totalinfo = mydict.get("all")
    for item in totalinfo:
        print "Avg:{}".format(mydict.get("avg").get(item)[0].get("mean"))
        print "|{:^20}|{:^18}|{:^12}|{:^12}|{:^14}|".format("Time","memTotal","memUsed","MemAvailable","MemFree")
        cpuinfo = totalinfo.get(item)
        for sig in cpuinfo:
            print "|{:^20}|{:^18}|{:^12}|{:^12}|{:^14}|".format(sig.get("time"),sig.get("total"),sig.get("used"),sig.get("available"),sig.get("free"))

def printToConsole(function):
    def wrapper(*args,**kwargs):
        results = function(*args,**kwargs)
        if results.get("type") == "cpu":
            printDictCpu(results)
        elif results.get("type") == "mem":
            printDictMem(results)
        else:
            print "dsdsa"
        return results
    return wrapper



@printToConsole
def getCpuInfo(vmname=None,beginTime=None,endTime=None):
    Result = {}
    _query = "SELECT time,host,usage_idle,usage_system,usage_guest,usage_iowait FROM cpu WHERE host='{vmname}' and (time >='{begintime}' and time <= '{endtime}')".format(
        vmname=vmname,begintime=beginTime,endtime=endTime
    )
    for item in client.query(_query).items():
        ([_,cpuname],cpuValue) = item
        cpuname = "cpu0" if not cpuname else cpuname
        if not  Result.has_key(cpuname):
            Result[cpuname] = [_ for _ in cpuValue]
    return {"all":Result,"avg":None,"type":"cpu"}


@printToConsole
def getMemInfo(vmname=None,beginTime=None,endTime=None):
    ResultAll = {}
    ResultAvg = {}
    _queryAll = "SELECT * FROM mem WHERE host='{vmname}' and (time >='{begintime}' and time <= '{endtime}')".format(
        vmname=vmname, begintime=beginTime, endtime=endTime
    )
    _queryAvg = "select MEAN(used)  FROM mem WHERE host='{vmname}' and (time >='{begintime}' and time <= '{endtime}')".format(
        vmname=vmname, begintime=beginTime, endtime=endTime
    )
    for item in client.query(_queryAll).items():
        ([_,memname],memValue) = item
        memname = "mem0" if not memname else memname
        if not ResultAll.has_key(memname):
            ResultAll[memname] = [_ for _ in memValue]
    for item in client.query(_queryAvg).items():
        ([_,memname],memValue) = item
        memname = "mem0" if not memname else memname
        if not ResultAvg.has_key(memname):
            ResultAvg[memname] = [_ for _ in memValue]
        
    return {"all":ResultAll,"avg":ResultAvg,"type":"mem"}





def Main():
    usage = "Usage: %prog [-m|-c] <-t target> <-b begintime> < -e endtime> <-d delaydays>"
    parser = OptionParser(usage=usage)
    parser.add_option("-b","--begin",action="store",
                      dest="begintime", default=False,
                      help="begintime  eg:2017-12-18T09:49:00Z")
    parser.add_option("-t","--target",action="store",
                      dest="target", default=False,
                      help="vmname")
    parser.add_option('-e',"--endtime",action="store",
                      dest="endtime",default=False,
                      help="endtime eg:2017-12-18T09:49:00Z")
    parser.add_option('-d','--delay',action="store",
                      dest="delay",default=False,
                      help="delay days")
    parser.add_option("-m","--meminfo",action="store_true",
                      dest="meminfo",default=False,
                      help="display mem used info")
    parser.add_option("-c","--cpuinfo",action="store_true",
                      dest="cpuinfo",default=False,
                      help="display the cpu info")
    (option,args) = parser.parse_args() 

    vmname = option.target if option.target else None
    begintime = option.begintime  if option.begintime else None
    endtime = option.endtime if option.endtime else None
    
    if not all([vmname,begintime,endtime]):
        print "usage: -h"
        exit()
    
    if option.meminfo:
        getMemInfo(vmname=vmname,beginTime=begintime,endTime=endtime)
    elif option.cpuinfo:
        getCpuInfo(vmname=vmname,beginTime=begintime,endTime=endtime)
    
Main()
