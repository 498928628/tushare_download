import datetime
import pymongo

client = pymongo.MongoClient("mongodb://localhost:27017")

# 设置要使用的数据库
mongodb_name = 'stock_list'
db = client[mongodb_name]


# 初始化开始日期
def add_start_date():
    u = {
        'start_date': '2015/6/30',
    }
    db.stock_cache.insert(u)
    print('添加时间完成')


# 调用开始日期
def start_date():
    query = {}
    field = {
        # 字段: 1 表示提取这个字段
        # 不传的 默认是 0 表示不提取
        'start_date': 1,
        '_id': 0
    }
    data = list(db.start_date.find(query, field))
    stock_date = [x['start_date'] for x in data][0]
    return stock_date

#开始时间更新
def start_date_update():
    today = datetime.datetime.now()
    end_date = "{}/{}/{}".format(today.year, today.month, today.day)
    start_time = start_date()
    query = {
        'start_date': start_time,
    }
    form = {
        '$set': {
            'start_date': end_date,
        }
    }
    options = {
        'multi': True,
    }
    db.start_date.update(query, form, **options)
    print('时间更新到:{}'.format(end_date))

def stock_list():
    try:
        query = {}
        field = {
            # 字段: 1 表示提取这个字段
            # 不传的 默认是 0 表示不提取
            'code': 1,
            '_id': 0
        }
        data = list(db.stock_cache.find(query, field))
        print(data)
        stock_list = [x['code'] for x in data]
    except:
        stock_list = None
    finally:
        return stock_list


