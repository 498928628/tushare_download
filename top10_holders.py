import pymongo
import json
import tushare as ts
import time
import datetime
from download_mongo.exception_handing import update_new_cache
from download_mongo.data_cleaning import holders_time_convert
from download_mongo.stock_list import add_stock_list, add_stock_cache, stock_list

# 连接mongo数据库,主机是本机,端口是默认的端口
client = pymongo.MongoClient("mongodb://localhost:27017")
print('连接数据库成功', client)

# 设置要使用的数据库
mongodb_name = 'stock_list'
db = client[mongodb_name]
# #定义初缓存列表和错误代码缓存列表
coll_stock_cache = db['holders_stock_cache']
coll_new_cache = db['holders_new_cache']
coll = db['top10_holders']


# first_start
def top10_holders():
    n = 0
    stock_count = len(stock_list(coll_stock_cache))
    for code in stock_list(coll_stock_cache):
        try:
            print('开始读取code:', code)
            df, data = ts.top10_holders(code=code, gdtype='1')
            data['code'] = code
            coll.insert(json.loads(data.to_json(orient='records')))
            print('保存code:', code)
            coll_stock_cache.delete_one({'code': code})
            print('从缓存中删除code:', code)
            n += 1
            sleep_time = n / 100
            print('当前进度{}:{}/{}'.format(code, n, stock_count))
            print('▂▃▄▅▆▇█▉▉█▇▆▅▄▃▂暂停区')
            if sleep_time.is_integer():
                print('休息三分钟')
                time.sleep(180)
        except:
            try:
                # coll.deleteMany({'code': code})
                update_new_cache(coll_stock_cache, coll_new_cache, code)
            except:
                coll_stock_cache.delete_one({'code': code})
            finally:
                print('重新开始top10_holders')
                restar(top10_holders())
    print('开始转换时间')
    holders_time_convert(coll)
    print('任务完成')


# 返回从最大值至今的year,quarter列表 [{2017: '4'}, {2018: '1'}, {2018: '2'}]
# 1.当该股的列表不存在时(如新股),报错返回None
# 2.当更新时间小于下一个节点,返回[]
def date_holders(code):
    try:
        old_date = update_date(coll, code)
        new_date = datetime.datetime.now()
        start_month = []
        while new_date > old_date:
            time_pro = {}
            old_date = datetime.datetime.strptime(get_q_date(old_date.year, int((old_date.month) / 3)), '%Y-%m-%d')
            l = int((old_date.month) / 3)
            ct = {'0': '1', '4': '4', '1': '1', '2': '2', '3': '3'}
            time_pro[old_date.year] = ct[str(l)]
            start_month.append(time_pro)
        start_month.pop()
    except:
        start_month = 233
    finally:
        return start_month


# 返回下一个hodeler时间节点
def get_q_date(year, quarter):
    dt = {'0': '-03-31', '4': '-03-31', '1': '-06-30', '2': '-09-30', '3': '-12-31'}
    if quarter == 4:
        year = year + 1
    start_date = '%s%s' % (str(year), dt[str(quarter)])
    return start_date


# 更新
def update_top10_holders():
    n = 0
    stock_count = len(stock_list(coll_stock_cache))
    for code in stock_list(coll_stock_cache):
        try:
            print('开始读取code:', code)
            # 新股等,列表不存在,返回233
            if date_holders(code) == 233:
                query = {
                    'code': code,
                }
                field = {
                    'code': 1,
                    '_id': 1,  # mogodb中连_id字段在内都相同的完全一样的字段不能插入

                }
                coll_new_cache.insert(coll_stock_cache.find(query, field))
                print('可能为新股,不存在该代码,读取下一个')
            # 不用更新的情况,返回[]
            elif not date_holders(code):
                print('未到更新时间,请等待')
            # 报错为数据还未出来的股票
            else:
                for i in date_holders(code):
                    for k, v in i.items():
                        year = k
                        quarter = int(v)
                        df, data = ts.top10_holders(code=code, year=year, quarter=quarter, gdtype='1')
                        data['code'] = code
                        coll.insert(json.loads(data.to_json(orient='records')))
                        print('保存{}:{}年第{}季度:'.format(code, year, quarter))
            coll_stock_cache.delete_one({'code': code})
            print('从缓存中删除code:', code)
            n += 1
            sleep_time = n / 100
            print('当前进度{}:{}/{}'.format(code, n, stock_count))
            print('▂▃▄▅▆▇█▉▉█▇▆▅▄▃▂暂停区')
            if sleep_time.is_integer():
                print('休息三分钟')
                time.sleep(180)
        except:
            try:
                update_new_cache(coll_stock_cache, coll_new_cache, code)
            except:
                # 出现new_cache中有重复code
                coll_stock_cache.delete_one({'code': code})
            finally:
                print('重新开始update_top10_holders')
                restar(update_top10_holders())
    print('开始转换时间')
    holders_time_convert(coll)
    print('任务完成')


# 判断是否代码表为空
def restar(restar_func):
    if not stock_list(coll_stock_cache):
        print('new_stock_cache待完成')
    else:
        print('重新开top10_holders')
        restar_func


# 返回code中,holders的日期最大值
def update_date(coll, code):
    query = {
        'code': code,
    }
    df = list(coll.find(query, {'quarter': 1, '_id': 0}).sort('quarter', -1).limit(1))
    quarter = [x['quarter'] for x in df][0]
    return quarter


#

# 将未完成的代码放入stock_cache中
def second_exc():
    coll_stock_cache.remove()
    query = {}
    field = {
        '_id': 1,
        'code': 1
    }
    coll_stock_cache.insert(coll_new_cache.find(query, field))
    print('从new_cache中继承剩余代码')
    coll_new_cache.remove()


# 初始data下载
def first_start():
    top10_holders()


# 下载更新后,检查报错中的遗失
def new_cache_exc():
    second_exc()
    top10_holders()


# 更新
def update_route():
    add_stock_list(db)
    add_stock_cache(db, coll_stock_cache)
    update_top10_holders()


if __name__ == '__main__':
    # new_cache_exc()
     update_route()


