import datetime

#str时间格式转换
def holders_time_convert(coll):
    time_dic = list(coll.find({"quarter": {"$type": 2}}))
    time_list = [x['quarter'] for x in time_dic]
    time_list_set = list(set(time_list))
    count = len(time_list_set)
    print('time_list_set', time_list_set)
    print('time_list_set总计{}'.format(count))
    for i,time in enumerate(time_list_set):
        query = {
            'quarter': time,
        }
        form = {
            '$set': {
                'quarter': datetime.datetime.strptime(time,'%Y-%m-%d'),
            }
        }
        options = {
            'multi': True,
        }
        coll.update(query, form, **options)
        print('{}时间格式修改为datetime,进度:{}/{}'.format(time,i,count))
    print('时间格式修改完成')