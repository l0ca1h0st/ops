#!/bin/env python
#coding:utf=8
#desciption：用于修改客户虚拟机密码

from commands import getoutput
import sys
from re import compile


"""
python changepass.py vn_name newpass
"""

ipobj = compile(r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}')


def getSerAndCtlMac(vm_name):
    """根据虚拟机的名称分别获取控制网卡mac地址和服务网卡的mac地址	"""
    ret = {"ctlmac":None,"srvmac":None}
    result = getoutput("virsh domiflist  %s" % vm_name)
    splitresult=  result.strip("\n").split("\n")
    ctlinfo = splitresult[-1]
    srvinfo = splitresult[-2]

    ctlmac = ctlinfo.split()[-1]
    srvmac = srvinfo.split()[-1]
    ret['ctlmac'] = None if not ctlmac else ctlmac
    ret['srvmac'] = None if not srvmac else srvmac 
    return ret
"""
    for item in  result.split('\n'):
		if "vnet15" in item:
			srvmac = item.split()[-1]
			ret['srvmac'] = srvmac
		if "vnet16" in item:
			ctlmac = item.split()[-1]
			ret['ctlmac'] = ctlmac
    return ret

"""

def getIpfromMac(mac):
    """
	根据mac地址获取ip地址，此处应是控制网卡mac地址
    """
    ret = {"ip":None}
    result = getoutput("arp -a |grep %s" % mac)
    ip = ipobj.findall(result)
    if len(ip) == 1:
        ret["ip"] = ip[0]
	return ret	

def shortVmName(vm_name):
    vm_name = vm_name.split("-")
    vmname = vm_name[0]+'-'+vm_name[1]
    return vmname

def changePass(vmname,hostname,kvmip,ctlmac,ctlip,srvmac,newpass):
    CMD = """virsh qemu-agent-command %s '{"execute": "guest-init","arguments":{"hypervisor-ip":"%s","init-password": "%s","ctrl-device": "eth6", "hostname": "%s", "srv-device": "eth5","ctrl-mac": "%s","srv-mac":"%s", "ctrl-ip-address": "%s/17"}}'""" % (vmname,kvmip,newpass,hostname,ctlmac,srvmac,ctlip)
    result = getoutput(CMD)


def getLocalKvmIp():
    result = getoutput("ip addr show dev nspbr0|grep 'inet '")
    ipsearch = ipobj.findall(result)
    if len(ipsearch) == 2:
        ip = ipsearch[0]
    else:
        ip = None
    return ip


if __name__ == "__main__":
    macinfo =  getSerAndCtlMac(sys.argv[1])
    ctlmac = macinfo.get("ctlmac")
    srvmac = macinfo.get("srvmac")
    ctlip = getIpfromMac(macinfo.get("ctlmac")).get("ip")
    vmname = sys.argv[1]
    hostname = shortVmName(vmname)
    newpass = sys.argv[2]
    kvmip = getLocalKvmIp()
    checkList = (vmname,hostname,kvmip,ctlmac,ctlip,srvmac,newpass)
    print "\n欢迎来到修改密码系统，请核对信息是否有误\n"
    print "当前kvm主机ip地址：\t%s" % kvmip
    print "需要修改密码的主机：\t%s" % vmname
    print "服务网卡mac地址：\t%s" % srvmac
    print "控制网卡mac地址：\t%s" % ctlmac
    print "控制ip地址：\t%s" % ctlip
    print "新的密码：\t%s" % newpass
    print "============================================================="
    flag = raw_input("输入Y确认修改密码，输入N退出修改：")
    print flag
    if flag == "N":
        sys.exit()
    if all(checkList):
        changePass(vmname,hostname,kvmip,ctlmac,ctlip,srvmac,newpass)
