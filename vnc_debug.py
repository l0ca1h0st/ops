#!/usr/bin/python
#coding: utf-8
import commands
import os

kvm_pass = 'yunshanop'

def get_local_mem():
	# 获取本地主机内存信息
	total= commands.getoutput("free -m|sed '1d'|head -n1|awk '{print $2,$3,$4,$6,$7}'").split(' ')
	return total

def get_local_cpu():
	# 获取本地主机cpu负载情况
	total = commands.getoutput("uptime|awk '{print $10,$11,$12}'")
	return [load.strip() for load in total.split(',')]


def getvnc():
	"统计vnc连接情况，输出vnc进程pid和对应的端口"
	vnc_result_info = {}
	vncinfos = commands.getoutput("ps aux|grep /var/www/lcweb/public/plugin/noVNC/utils/launch.sh|grep -v grep|awk '{print $2,$14}'")
	if not vncinfos:
		return None
	for sig in vncinfos.split('\n'):
		pid,port = sig.split()
		if vnc_result_info.has_key(pid):
			pass
		else:
			vnc_result_info[pid] = port.split(":")[1]
	return vnc_result_info


def get_kvm_from_connect_port(port):
	"""根据VNC的端口找到对应的kvm主机名称"""
	# 找到 根据vnc的pid找到ssh进程的pid
	pid = commands.getoutput("netstat -tnp|grep %s|grep -v  python|awk '{print $7}'" % port)
	if not pid:
		return {port: None}
	# 根据vnc的pid找到对应kvm连接的主机
	kvm_host = commands.getoutput("ps uax|grep %s|grep -v grep |awk '{print $13}'" % pid.split('/')[0])
	if not kvm_host:
		return {port: None}
	return {port: kvm_host}

def get_all_relative_process_from_port(port):
	# 根据提供的端口号找到所有和此端口相关的进程pid 
	pids = commands.getoutput("netstat -tnp|grep %s|awk '{print $7}'" % port)
	if not pids:
		return []
	result = [pid.split('/')[0] for pid in pids.split('\n')]
	return result

def get_number_openfiles_according_pid(pid):
	# 根据pid统计该进程打开了多少文件描述符
	try:
		pid = str(int(pid))
		pid_proc_path = os.path.join('/proc/',pid,'fd')
		numfile = len(os.listdir(pid_proc_path))
	except:
		numfile = 0
	return {pid: numfile}


	

def Main():
	print "-"*93
	print "|%s|%s|%s|%s|%s|%s|%s|%s|" % ("总内存".center(12),"已使用".center(12),"free".center(12),"buffer".center(12),"cache".center(12),"1分钟".center(12),"5分钟".center(12),"15分钟".center(12))
	print "-"*93
	meminfo = get_local_mem()
	cpuinfo = get_local_cpu()
	print "|%s|%s|%s|%s|%s|%s|%s|%s|" % (meminfo[0].center(9),meminfo[1].center(9),meminfo[2].center(12),meminfo[3].center(12),meminfo[4].center(12),cpuinfo[0].center(10), cpuinfo[1].center(10),cpuinfo[2].center(10))
	print "-"*93
	
	all_vnc = getvnc()
	if not all_vnc:
		print "\n没有任何vnc进程存在"
		return None
	print "\n当前系统总共存在[ %d ]个VNC进程\n" % len(all_vnc)
	

	print "-" * 93
	print "|%s|%s|%s|%s|%s|" % ("vnc进程号".center(18), "vnc监听端口".center(18), "vm所属KVM主机".center(28),"vnc打开文件".center(16),"相关端口".center(26))
	print "-" * 93
	for vnc_pid,vnc_port in all_vnc.items():
		#print "-"*61
		# 获取所有和该vnc监听端口相关的所有的进程
		all_relative_pid = get_all_relative_process_from_port(vnc_port)
		display_relative_pid = "  ".join(all_relative_pid)
		all_relative_pid.append(vnc_pid)
		_tmp_number_openfile_dic = {}
		# 统计每个相关进程所打开的文件描述符，并且累加
		map(lambda _:_tmp_number_openfile_dic.update(get_number_openfiles_according_pid(_)), all_relative_pid)
		openfiles_number = reduce(lambda _,__:_+__, _tmp_number_openfile_dic.values())
		# 获取该vnc连接转发到对应kvm主机的主机名称
		kvm_host_dic = get_kvm_from_connect_port(vnc_port)		
		kvm_host = kvm_host_dic.get(vnc_port) if kvm_host_dic.get(vnc_port) else "Invalid Connection"

		print "|%s|%s|%s|%s|%s|" % (vnc_pid.center(15),vnc_port.center(14),kvm_host.center(24),str(openfiles_number).center(12),display_relative_pid.center(22))
	print "-" * 93

Main()
