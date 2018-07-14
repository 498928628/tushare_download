
#将cache中异常的code放入新表
def update_new_cache(coll_stock_cache, coll_new_cache, code):
    query = {
        'code': code,
    }
    field = {
        'code': 1,
        '_id': 1,  # mogodb中连_id字段在内都相同的完全一样的字段不能插入

    }
    coll_new_cache.insert(coll_stock_cache.find(query, field))
    coll_stock_cache.delete_one({'code': code})
