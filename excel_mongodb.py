import xlrd
import sys
import json
from pymongo import MongoClient
import os
import win32com.client as win32

# xls基于同花顺问财


# 连接数据库
client = MongoClient('localhost', 27017)
db = client.stock_list
coll = db.biggest_gain


# 返回文件夹下所有formats路径
def file_name(file_dir, formats):
    L = []
    for root, dirs, files in os.walk(file_dir):
        for file in files:
            if os.path.splitext(file)[1] == '.{}'.format(formats):
                L.append(file_dir + '\\' + file)
    return L


# 将文件夹下的xls转换成xlsx
def xls_to_xlsx():
    file_dir = 'F:\\BaiduYunDownload\\下半部\\tushare\\download_mongo\\biggest_gain'
    formats = 'xls'
    xls_list = file_name(file_dir, formats)
    for fname in xls_list:
        excel = win32.gencache.EnsureDispatch('Excel.Application')
        wb = excel.Workbooks.Open(fname)
        wb.SaveAs(fname + 'x', FileFormat=51)
        wb.Close()
        excel.Application.Quit()


def excle_mongo():
    xls_to_xlsx()
    file_dir = 'F:\\BaiduYunDownload\\下半部\\tushare\\download_mongo\\biggest_gain'
    formats = 'xlsx'
    xlsx_list = file_name(file_dir, formats)
    for fname in xlsx_list:
        data = xlrd.open_workbook(fname)
        table = data.sheets()[0]
        # 读取excel第一行作为存入mongodb的字段名
        rowstag = table.row_values(0)
        k, v = rowstag[4].split('(%)')
        rowstag.append('涨幅区间')
        new_rowstag = []
        for i in rowstag:
            t = i.replace('.', '/')
            if '区间涨跌幅' in i:
                t, f = t.split('(%)')
            new_rowstag.append(t)
        print(new_rowstag)
        nrows = table.nrows
        # ncols=table.nrows
        # print('rowstag',rowstag)
        # print('nrows',nrows)
        # print('ncols',ncols)
        returnData = {}
        for i in range(1, nrows):
            # zip打包为元组的列表,
            row_values = table.row_values(i)
            row_values.append(v)
            row_values[0] = row_values[0][:-3]
            print(row_values)
            returnData[i] = json.dumps(dict(zip(new_rowstag, row_values)))
            # 转换为字典,转换为json格式,通过编解码还原数据
            returnData[i] = json.loads(returnData[i])
            print(returnData[i])
            coll.insert(returnData[i])


if __name__ == '__main__':
    excle_mongo()
