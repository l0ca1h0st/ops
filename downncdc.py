#!/usr/bin/python  
#-*- coding: utf-8 -*-  
  
import sys  
import os  
from ftplib import FTP  
  
#ftp 服务器链接  
def ftpconnect():  
    ftp_server = 'ftp3.ncdc.noaa.gov'  
    username = ''  
    password = ''  
    ftp=FTP()  
    ftp.set_debuglevel(2) #打开调试级别2，显示详细信息  
    ftp.connect(ftp_server,21) #连接  
    ftp.login(username,password) #登录，如果匿名登录则用空串代替即可  
    return ftp  
  
#开始下载文件  
def downloadfile():    
    ftp = ftpconnect()      
    #print ftp.getwelcome() #显示ftp服务器欢迎信息  
    datapath = "/pub/data/noaa/isd-lite/"  
    year=int(sys.argv[1])       #年份循环  
    currentyear = year          #当前执行年份  
    while year<=int(sys.argv[2]):  
        path=datapath+str(year)  
        li = ftp.nlst(path)  
          
        #创建指定年份的目录  
        path = sys.argv[3]+'/'  
        dir = str(year)  
        new_path = os.path.join(path, dir)  
        if not os.path.isdir(new_path):  
            os.makedirs(new_path)  
              
        for eachFile in li:  
            localpaths = eachFile.split("/")  
            localpath = localpaths[len(localpaths)-1]  
            localpath=new_path + '/'+ str(year) + '--'+localpath#把日期放在最前面，方便排序  
            bufsize = 1024 #设置缓冲块大小        
            fp = open(localpath,'wb') #以写模式在本地打开文件  
            ftp.retrbinary('RETR ' + eachFile,fp.write,bufsize) #接收服务器上文件并写入本地文件  
        year=year+1  
    ftp.set_debuglevel(0) #关闭调试  
    fp.close()  
    ftp.quit() #退出ftp服务器  
  
  
if __name__=="__main__":  
    downloadfile() 