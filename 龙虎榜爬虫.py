#coding=utf-8

import gzip 
import http.cookiejar
import urllib.request
import urllib.parse
import json
import os
import time
import datetime

def getOpener(head):
    # deal with the Cookies
    cj = http.cookiejar.CookieJar()
    pro = urllib.request.HTTPCookieProcessor(cj)
    opener = urllib.request.build_opener(pro)
    header = []
    for key, value in head.items():
        elem = (key, value)
        header.append(elem)
    opener.addheaders = header
    return opener

def ungzip(data):
    try:       
        print('正在解压.....')
        data = gzip.decompress(data)
        print('解压完毕!')
    except:
        print('未经压缩, 无需解压')
    return data
def writeFile(fname,data):
    filename = r'files/'+fname+'.txt'
    if os.path.exists(filename):
        message = '文件 + '+filename +' 已存在，跳过'
    else:
        message = '文件 + '+filename +' 不存在，新建'
        
        f=open(filename,'w')
        f.write(data)
        f.close()
    print ('文件：'+fname+' 处理完毕。')
'''
读取抓取数据开始日期
如果不存在该日期，从10日前开始读取
如果存在从文件内日期开始读取
读到今天
'''
header = {
    'Connection': 'Keep-Alive',
    'Accept': '*/*',
    'Accept-Language': 'zh-CN,zh;q=0.8',
    'User-Agent': 'Mozilla/5.0 (Windows NT 6.2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/40.0.2214.111 Safari/537.36',
    'Accept-Encoding': 'gzip, deflate',
    'Host': '',
    'Referer' : ''
}
shUrl = 'http://query.sse.com.cn/infodisplay/showTradePublicFile.do?dateTx='#2015-09-28
szUrl = ['http://www.szse.cn/szseWeb/common/szse/files/text/jy/jy',#150923.txt
         'http://www.szse.cn/szseWeb/common/szse/files/text/smeTxt/gk/sme_jy',#150708.txt
         'http://www.szse.cn/szseWeb/common/szse/files/text/nmTxt/gk/nm_jy']#150902.txt
startFileName = r'startDay.txt'
endDay = datetime.datetime.now()

if os.path.exists(startFileName):
    print('日期配置文件存在，开始读取')
    f=open(startFileName,'rt')
    s = f.readline()
    f.close()
    if s!='':
        print('将从日期：'+s+' 开始读取')
        timeArray = time.strptime(s, "%Y%m%d")
        timeStamp = int(time.mktime(timeArray))
        fromDay = datetime.datetime.utcfromtimestamp(timeStamp)
    else:
        print('日期配置文件为空，将从10日前日期开始读取')
        fromDay = endDay - datetime.timedelta(days = 10)
else:
    print('日期配置文件不存在，将从10日前日期开始读取')
    fromDay = endDay - datetime.timedelta(days = 10)

endDay = endDay + datetime.timedelta(days = 1)

while fromDay.strftime("%Y%m%d")!=endDay.strftime("%Y%m%d"):
    print(fromDay.strftime("%Y%m%d"))
    
    '''
    循环上面日期
    抓取上证，深证，中小创交易龙虎榜数据

    如果内容不为空

    文件不存在

    写入文件
    '''
    #抓取上证龙虎榜数据
    url = shUrl + fromDay.strftime("%Y-%m-%d")
    print('读取上证龙虎榜\n'+url)

    header['Host'] = 'query.sse.com.cn'
    header['Referer'] = 'http://www.sse.com.cn/disclosure/diclosure/public/'

    try:
        opener = getOpener(header)
        op = opener.open(url)
        data = op.read()
        data = data.decode()
        jsonData = json.loads(data)
        outData = ''
        if (jsonData['fileContents']!=''):
            for info in jsonData['fileContents']:
                outData= outData+ info+'\n'
            writeFile(fromDay.strftime("%Y-%m-%d")+'_上证',outData)
    except:
        print(fromDay.strftime("%Y-%m-%d")+'跳过')

    
    #抓取深证，中小创交易龙虎榜数据
    i=1
    for url in szUrl:
        if(i==1):
            name = '深证'
        elif(i==2):
            name = '中小板'
        else:
            name = '创业板'
        url = url + fromDay.strftime("%y%m%d")+'.txt'
        print('读取'+name+'龙虎榜\n'+url)
        header['Host'] = 'www.szse.cn'
        header['Referer'] = 'http://www.szse.cn'
        try:
            opener = getOpener(header)
            op = opener.open(url)
            data = op.read()
            data = ungzip(data)
            data = data.decode('gbk')
            writeFile(fromDay.strftime("%Y-%m-%d")+'_'+name,data)
        except:
            print(fromDay.strftime("%Y-%m-%d")+'跳过')
        i=i+1

    fromDay = fromDay + datetime.timedelta(days = 1)

#最后更新日期为当前日期
print('设置最新日期')
fromDay = fromDay - datetime.timedelta(days = 1)
f=open(startFileName,'w')
f.write(fromDay.strftime("%Y%m%d"))
f.close()
print('读取完成')
