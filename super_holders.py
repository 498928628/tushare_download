# 本地处理,非web端
from difflib import SequenceMatcher
import traceback
import pymongo
import json
import tushare as ts
import time
import datetime
from download_mongo.exception_handing import update_new_cache
from download_mongo.data_cleaning import holders_time_convert
from download_mongo.stock_list import add_stock_list, add_stock_cache, stock_list
import string
import random
from collections import defaultdict

# 连接mongo数据库,主机是本机,端口是默认的端口
client = pymongo.MongoClient("mongodb://localhost:27017")
print('连接数据库成功', client)

# 设置要使用的数据库
mongodb_name = 'stock_list'
db = client[mongodb_name]
coll = db['top10_holders']
coll_gain = db['biggest_gain']

# 读取最大涨幅的时间,period 例2014.12.31-2015.03.31,得到要分析的代码表
period = "2016.06.30-2016.09.30"
nation_cooperator = ['中央汇金投资有限责任公司',
                     "香港中央结算有限公司",
                     '中央汇金资产管理有限公司',
                     '中国证券金融股份有限公司',
                     '博时基金-农业银行-博时中证金融资产管理计划',
                     '工银瑞信基金-农业银行-工银瑞信中证金融资产管理计划',
                     '中欧基金-农业银行-中欧中证金融资产管理计划',
                     '嘉实基金-农业银行-嘉实中证金融资产管理计划',
                     '南方基金-农业银行-南方中证金融资产管理计划',
                     '华夏基金-农业银行-华夏中证金融资产管理计划',
                     '易方达基金-农业银行-易方达中证金融资产管理计划',
                     '广发基金-农业银行-广发中证金融资产管理计划',
                     '大成基金-农业银行-大成中证金融资产管理计划',
                     '银华基金-农业银行-银华中证金融资产管理计划',
                     '北京凤山投资有限责任公司',
                     '北京坤藤投资有限责任公司',
                     '梧桐树投资平台有限责任公司',
                     '中国银行股份有限公司-招商丰庆灵活配置混合型发起式证券投资基金',
                     '中国农业银行股份有限公司-易方达瑞惠灵活配置混合型发起式证券投资基金',
                     '中国工商银行股份有限公司-南方消费活力灵活配置混合型发起式证券投资基金',
                     '中国工商银行股份有限公司-嘉实新机遇灵活配置混合型发起式证券投资基金',
                     '中国银行股份有限公司-华夏新经济灵活配置混合型发起式证券投资基金',
                     '中央汇金资产管理有限责任公司',]
# 更多的限制条件在实践中添加
other_fund = ['全国社保基金', '指数', '中国人寿', '新华人寿','信用交易担保','定增','员工持股','集团','LOF','全国社会保障基金','(LOF)','中国平安人寿',
              '保险','混合型证券投资基金','证券投资基金','中央汇金',
              '约定购回','股指','企业年金','新股申购','国有资产','ETF','中央结算','证券股份有限公司']




def time_args(period):
    query = {
        '涨幅区间': period,
    }
    field = {
        '股票代码': 1,
        '涨幅区间': 1,
        '_id': 0,
    }
    df = list(coll_gain.find(query, field))
    time_list = []
    for i in df:
        time_dic = {}
        time_dic[i['股票代码']] = i['涨幅区间']
        time_list.append(time_dic)
    print('time_list', time_list)
    # [{'601519': '2014.12.31-2015.03.31'}, {'002280': '2014.12.31-2015.03.31'}, {'000626': '2014.12.31-2015.03.31'}, {'300085': '2014.12.31-2015.03.31'}, {'002195': '2014.12.31-2015.03.31'}, {'002075': '2014.12.31-2015.03.31'}, {'000540': '2014.12.31-2015.03.31'}, {'300350': '2014.12.31-2015.03.31'}, {'300130': '2014.12.31-2015.03.31'}, {'002631': '2014.12.31-2015.03.31'}, {'002107': '2014.12.31-2015.03.31'}, {'002657': '2014.12.31-2015.03.31'}, {'601766': '2014.12.31-2015.03.31'}, {'601010': '2014.12.31-2015.03.31'}, {'000066': '2014.12.31-2015.03.31'}, {'002055': '2014.12.31-2015.03.31'}, {'300297': '2014.12.31-2015.03.31'}, {'002260': '2014.12.31-2015.03.31'}, {'300104': '2014.12.31-2015.03.31'}, {'300089': '2014.12.31-2015.03.31'}, {'300170': '2014.12.31-2015.03.31'}, {'300248': '2014.12.31-2015.03.31'}, {'300278': '2014.12.31-2015.03.31'}, {'300359': '2014.12.31-2015.03.31'}, {'300032': '2014.12.31-2015.03.31'}, {'300033': '2014.12.31-2015.03.31'}, {'002622': '2014.12.31-2015.03.31'}]
    return time_list


# 季度最大涨幅股票的协同股东(code period)
def code_find1(time_list):
    # time_list = time_args(period)
    name20 = []
    for x in time_list:
        for k, v in x.items():
            code = k
            time1, time2 = v.split('-')
            time1 = datetime.datetime.strptime(time1, '%Y.%m.%d')
            time2 = datetime.datetime.strptime(time2, '%Y.%m.%d')
            query = {
                'code': code,
                '$or': [
                    {
                        'quarter': time1,
                    },
                    {
                        'quarter': time2,
                    }
                ]
            }
            field = {
                'code': code,
                'name': 1,
                '_id': 0,
            }
            df = list(coll.find(query, field))
            # 去重
            name_list = list(set([x['name'] for x in df]))
            name_dic = {}
            name_dic[code] = name_list
            name20.append(name_dic)
    print('name20', name20)  # [{'601519': ['张长虹', '沈宇', '深圳钦舟实业发展有限公司', '
    return name20


# 找name所买过的code
def name_find(time_list):
    for i in code_find1(time_list):
        for k, v in i.items():
            code = k
            name_list = v
            rng = ''.join(random.choice('abcdefghijklmnopqrstuvwxyz') for _ in range(3))
            col = db[code + rng]
            n = 0
            while len(name_list) > n:
                name = name_list[n]
                query = {
                    'name': name,
                    'sharetype': '流通A股',
                }
                field = {
                    'code': 1,
                    'name': 1,
                    'quarter': 1,
                    '_id': 0,
                }
                try:
                    if other_funds(name):
                        col.insert(db.top10_holders.find(query, field))
                except:
                    print('为非流通股股东')
                finally:
                    n += 1
            col.delete_many({'code': code})
            from_code = code
            cooperator_first(reclean(col), from_code)
    print('全部完成协同分析')


def reclean(col):
    namecode = col.aggregate([
        {'$group': {
            '_id': {
                'code': '$code', 'name': '$name'
            },
            # 'count': {
            #     '$sum': 1
            # }
        }},
        # {
        #     '$match': {'count': {'$gt': 0}}
        # }
    ])
    namecode_list = list(namecode)
    col.drop()
    print('namecode_list', namecode_list)
    return namecode_list

#name_set_list ['张长虹', '李玉民', '王玫', '张婷', '中国工商银行股份有限公司-中欧明睿新起点混合型证券投资基金', '张志宏', '易小强']
def cooperator_first(namecode_list, from_code):
    d = defaultdict(list)
    for i in namecode_list:
        namecode_dic = i['_id']
        code = namecode_dic['code']
        name = namecode_dic['name']
        if other_funds(name):
            d[code].append(name)
    name_list = []
    for i in d:
        if len(d.get(i)) > 1:
            name_list = name_list + d.get(i)
            print('协同股东', i, d.get(i))
            u = {
                'coper_code': i,
                'cooperator': d.get(i),
                'from_code': from_code,
            }
            db.cooperation.insert(u)
###################################################这个name_set_list后面可能用到
#['中国工商银行股份有限公司-中欧明睿新起点混合型证券投资基金', '张长虹', '李玉民', '张婷', '易小强', '王玫', '张志宏']
    name_set_list = list(set(name_list))

    print('#######################name_set_list', name_set_list)
    for name in name_set_list:
        # single_holder(single_holder(name, from_code),from_code)
        if other_funds(name):
            single_holder(name, from_code)



def other_funds(name):
    nn = 0
    for x in other_fund:
        if name.find(x) >= 0:
            nn= -1
    if nn == 0 and name not in nation_cooperator:
        return True
    else:
        return False

# print(other_funds('中国建设银行股份有限公司-博时裕富沪深300指数证券投资基金'))


# http://funds.hexun.com/2017-11-03/191491903.html
def national_team(from_code):
    pass


# 从列表里传topname
# 之后分析后改成list,从数据库读
def add_topname(top_name, from_code):
    nation_cooperator = ['中央汇金投资有限责任公司',
                         "香港中央结算有限公司",
                         '中央汇金资产管理有限公司',
                         '中国证券金融股份有限公司',
                         '博时基金-农业银行-博时中证金融资产管理计划',
                         '工银瑞信基金-农业银行-工银瑞信中证金融资产管理计划',
                         '中欧基金-农业银行-中欧中证金融资产管理计划',
                         '嘉实基金-农业银行-嘉实中证金融资产管理计划',
                         '南方基金-农业银行-南方中证金融资产管理计划',
                         '华夏基金-农业银行-华夏中证金融资产管理计划',
                         '易方达基金-农业银行-易方达中证金融资产管理计划',
                         '广发基金-农业银行-广发中证金融资产管理计划',
                         '大成基金-农业银行-大成中证金融资产管理计划',
                         '银华基金-农业银行-银华中证金融资产管理计划',
                         '北京凤山投资有限责任公司',
                         '北京坤藤投资有限责任公司',
                         '梧桐树投资平台有限责任公司',
                         '中国银行股份有限公司-招商丰庆灵活配置混合型发起式证券投资基金',
                         '中国农业银行股份有限公司-易方达瑞惠灵活配置混合型发起式证券投资基金',
                         '中国工商银行股份有限公司-南方消费活力灵活配置混合型发起式证券投资基金',
                         '中国工商银行股份有限公司-嘉实新机遇灵活配置混合型发起式证券投资基金',
                         '中国银行股份有限公司-华夏新经济灵活配置混合型发起式证券投资基金', ]
    for name in nation_cooperator:
        query = {
            'top_name': top_name,
            'name': name,
            'from_code': from_code,
        }
        db.top_name.insert(query)
    print('完成list的top_name添加')


# add_topname(top_name='国家队', from_code='collect')


def frequency(namecode_list, from_code):
    d = defaultdict(list)
    nameca = []
    for i in namecode_list:
        namecode_dic = i['_id']
        name = namecode_dic['name']
        nameca.append(name)
    namecache = list(set(nameca))
    print('namecache',namecache)
    for i in namecode_list:
        namecode_dic = i['_id']
        code = namecode_dic['code']
        name = namecode_dic['name']
        # 处理信托,私募产品的关联
        ''''''''''''''''''
        index_list = []
        for x in namecache:
            if similarity(name, x) > 0.8:
                index_list.append(namecache.index(x))
                index_list.sort()
        name = '&'.join([namecache[x] for x in index_list])
        d[name].append(code)
    name_list = []
    for i in d:
        if len(list(set(d.get(i)))) > 1:
            name_list.append(i)
            print('关联操作   ', i, d.get(i))
            u = {
                'coper_code': d.get(i),
                'cooperator': i,
                'from_code': from_code,
            }
            db.cooperation.insert(u)
    name_set = list(set(name_list))
    print('#####name_set', name_set)
    # return name_set


def name_find1(name):
    query = {
        'name': name,
        'sharetype': '流通A股',
    }
    field = {
        'code': 1,
        'name': 1,
        'quarter': 1,
        '_id': 0,
    }
    df = list(coll.find(query, field))
    name_list = [x['name'] for x in df]
    print('adfsd', name_list)
    print(df)


def similarity(strA, strB):
    similarity = SequenceMatcher(lambda x: x == " ", strA, strB).ratio()
    return similarity


# 按操作过的个股出现次数来关联
# def single_holder(name):
#     query = {
#         'name': name,
#         'sharetype': '流通A股',
#     }
#     field = {
#         'code': 1,
#         'name': 1,
#         '_id': 0,
#     }
#     name_coll = db[name]
#     name_coll.insert(coll.find(query, field))
#     namecode = name_coll.aggregate([
#         {'$group': {
#             '_id': {
#                 'code': '$code', 'name': '$name'
#             },
#         }},
#     ])
#     namecode_list = list(namecode)
#     print('namecode_list', namecode_list)
#     name_coll.drop()
#     code_list1 = []
#     for i in namecode_list:
#         namecode_dic = i['_id']
#         code = namecode_dic['code']
#         code_list1.append(code)
#     code_list = list(set(code_list1))
#     print('code_list',code_list)
#     for code in code_list:
#         query = {
#             'code': code,
#             'sharetype': '流通A股',
#         }
#         field = {
#             'code': 1,
#             'name': 1,
#             '_id': 0,
#         }
#         db.single.insert(coll.find(query, field))
#     frequency( reclean(db.single))


def single_holder(name, from_code):
    query = {
        'name': name,
        'sharetype': '流通A股',
    }
    field = {
        'code': 1,
        # 'name': 1,
        'quarter': 1,
        '_id': 0,
        # '$not':{'name':name}
    }
    df = list(coll.find(query, field))
    rng = ''.join(random.choice('abcdefghijklmnopqrstuvwxyz') for _ in range(3))
    name_coll = db[name + rng]
    for i in df:
        query = i
        field = {
            'code': 1,
            'name': 1,
            '_id': 0,
        }
        try:
            name_coll.insert(coll.find(query, field))
        except:
            print('个人名字有误')
    name_coll.delete_many({'name': name})
    for x in other_fund:
        name_coll.delete_many({"name":{'$regex':"{}".format(x)}})
        print('**********************************************************************')

    frequency(reclean(name_coll),from_code)



if __name__ == '__main__':
    time_list = [{'601519': '2014.12.31-2015.03.31'}]
    name_find(time_list)