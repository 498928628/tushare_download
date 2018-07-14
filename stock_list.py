import json
import tushare as ts


# 更新源代码列表
def add_stock_list(db):
    db.stock_list.remove()
    df = ts.get_area_classified()
    db.stock_list.insert(json.loads(df.to_json(orient='records')))
    print('添加代码表完成')

#更新缓存代码列表
def add_stock_cache(db,coll_stock_cache):
    add_stock_list(db)
    coll_stock_cache.remove()
    query = {}
    field = {
        # 字段: 1 表示提取这个字段
        # 不传的 默认是 0 表示不提取
        'code': 1,
        '_id': 1
    }
    coll_stock_cache.insert(db.stock_list.find(query, field))
    print('代码表缓存完毕')

#调用代码列表
def stock_list(coll_stock_cache):
    try:
        query = {}
        field = {
            # 字段: 1 表示提取这个字段
            # 不传的 默认是 0 表示不提取
            'code': 1,
            '_id': 0
        }
        data = list(coll_stock_cache.find(query, field))
        stock_list = [x['code'] for x in data]
    except:
        stock_list = None
    finally:
        return stock_list


