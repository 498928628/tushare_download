import pymongo
import json
import tushare as ts
import time
import datetime
from download_mongo.exception_handing import update_new_cache
from download_mongo.stock_list import add_stock_list, add_stock_cache, stock_list

# 连接mongo数据库,主机是本机,端口是默认的端口
client = pymongo.MongoClient("mongodb://localhost:27017")
print('连接数据库成功', client)

# 设置要使用的数据库
mongodb_name = 'stock_list'
db = client[mongodb_name]

# 声明一个行情连接的变量，然后重复使用它来获取数据
cons = ts.get_apis()
# 定义初缓存列表和错误代码缓存列表
coll_stock_cache = db['hist_stock_cache']
coll_new_cache = db['hist_new_cache']


# day数据下载,更新
def stock_hist_data():
    n = 0
    stock_count = len(stock_list(coll_stock_cache))
    for code in stock_list(coll_stock_cache):
        coll = db[code]
        try:
            print('残余timestrap转换')
            time_format_convert(coll, code)
            print('开始读取代码:', code)
            start_time = update_date(coll)
            print('将从日期：' + start_time + ' 开始读取')
            df_cache = ts.bar(code,
                              conn=cons, start_date=start_time,
                              end_date='', ma=[5, 13, 34, 55, 89, 144, 233, 987],
                              factors=['vr', 'tor'])
            df = df_cache.reset_index(['datetime'])
            coll.insert(json.loads(df.to_json(orient='records')))
            print('保存code:', code)
            print('修改时间为datetime')
            time_format_convert(coll, code)
            coll_stock_cache.delete_one({'code': code})
            print('从缓存中删除code:', code)
            n += 1
            sleep_time = n / 100
            print('当前进度{}:{}/{}'.format(code, n, stock_count))
            print('▂▃▄▅▆▇█▉▉█▇▆▅▄▃▂')
            if sleep_time.is_integer():
                print('休息三分钟')
                time.sleep(180)
        except:
            try:
                update_new_cache(coll_stock_cache, coll_new_cache, code)
            except:
                coll_stock_cache.delete_one({'code': code})
            finally:
                print('重新开始stock_hist_date')
                restar(stock_hist_data())
    print('任务完成')


# 判断是否代码表为空,非空则重启程序
def restar(restar_func):
    if not stock_list(coll_stock_cache):
        print('new_stock_cache待完成')
    else:
        print('重新开top10_holders')
        restar_func


# 时间int64转datetime
def time_format_convert(coll, code):
    time_dic = list(coll.find({"datetime": {"$type": 18}}))
    time_list = [x['datetime'] for x in time_dic]
    print('time_list', time_list)
    for time in time_list:
        query = {
            'datetime': time,
        }
        form = {
            '$set': {
                'datetime': time_convert(time / 1000),
            }
        }
        options = {
            'multi': True,
        }
        coll.update(query, form, **options)
        print('{}时间格式修改为datetime'.format(code))


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


def time_convert(time_strap):
    date_time = datetime.datetime.fromtimestamp(time_strap)
    print('date_time', date_time)
    print('now', date_time)
    return date_time

#返回code上一次更新的日期+1天
def update_date(coll):
    try:
        df = list(coll.aggregate(
            [{
                "$group": {
                    "_id": "$code",
                    "max_date": {"$max": "$datetime"}
                }
            }]
        ))
        new_time = [x['max_date'] for x in df][0]
        delta = datetime.timedelta(day=1)
        update_date = new_time + delta
        update_time = update_date.strftime('%Y/%m/%d')
    except:
        update_time = '2015/6/30'
    finally:
        return update_time



# 下载跟新后,检查报错中的遗失
def new_cache_exc():
    second_exc()
    stock_hist_data()


# 更新
def update_route():
    add_stock_list(db)
    add_stock_cache(db, coll_stock_cache)
    stock_hist_data()

#测试update_date
def test_update_date():
    coll = db['itttii']
    print(update_date(coll))
    #IndexError: list index out of range


if __name__ == '__main__':
    update_route()




