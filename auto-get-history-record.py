# -*- coding: utf-8 -*-
# Written by Xunjueyulin
# Version:0.1

import pymysql
import calendar
import time
import datetime
import math
# 根据年月计算区间unixtime值和采样点，包括指定年月和当前年月
# 初始化变量
start_sampling_time2 = time.time()
end_sampling_time2 = time.time()
itemid = 32148  # 初始化随机赋值，后期可写循环遍历监控id列表
results = {}
sampling_points = 0
five_percent_sampling_points = 0
''' zabbix5.0将历史记录分开存放，需要先在items表中获取对应itemid的value_type；当值为0，表示监控记录存放于表history；当值为1，表示
监控记录存放于表history_str；当值为2，表示监控记录存放于表history_log；当值为3，表示监控记录存放于表history_uint；当值为4，表示监控
记录存放于表history_text'''
history_type = 'history'
# 本地存储zabbix历史记录类型
value_type = {'0': 'history', '1': 'history_str', '2': 'history_log', '3': 'history_uint', '4': 'history_text'}


def specified_sampling():
    #  输入年月来计算采样天数和采样点数量
    year = int(input('请输入年份：'))
    month = int(input('请输入月份：'))
    sampling_days = calendar.monthrange(year, month)[1]  # 计算采样天数，使用monthlen（）方法有些python版本会报错
    print('当前时间为：' + str(datetime.datetime.now()))
    print('本月采样天数为：' + str(sampling_days))
    global sampling_points, five_percent_sampling_points
    sampling_points = sampling_days*12*24  # 计算采样点数量
    five_percent_sampling_points = math.floor(sampling_points*0.05)  # 前5%的取值点，再向下取整
    print('本月采样点数量为：' + str(sampling_points))
    print('本月前5%采样点数量为：' + str(five_percent_sampling_points))
    five_percent_sampling_points = five_percent_sampling_points+1  # 这里需要加1计算实际取值点
    start_sampling_time = '%s-%s-01 00:00:00' % (year, month)  # 获取当月第一天零时零分零秒
    end_sampling_time = '%s-%s-%s 23:59:59' % (year, month, sampling_days) # 获取当月最后一天23时59分59秒
    start_sampling_time1 = datetime.datetime.strptime(start_sampling_time, "%Y-%m-%d %H:%M:%S")  # 转换为time格式时间，下同
    end_sampling_time1 = datetime.datetime.strptime(end_sampling_time, "%Y-%m-%d %H:%M:%S")
    # 这里由于start_sampling_time是str格式，所以要先用strp转换为time格式，再使用mktime转换为unixtime格式
    # 由于zabbix系统使用的是10位unixtime，所以打印的时候要先用round四舍五入，再用int取整
    global start_sampling_time2, end_sampling_time2
    # 这里将start_sampling_time2和end_sampling_time2调用全局变量，方便后面引用
    start_sampling_time2 = int(round(time.mktime(start_sampling_time1.timetuple())))
    end_sampling_time2 = int(round(time.mktime(end_sampling_time1.timetuple())))
    print('本月采样开始时间为：' + str(start_sampling_time2))
    print('本月采样结束时间为：' + str(end_sampling_time2))
    # 要考虑实际采样达不到采样点数量的情况


def current_sampling():
    # 获取当前系统时间来计算采样天数和采样点数量
    year = datetime.datetime.now().year
    month = datetime.datetime.now().month
    sampling_days = calendar.monthrange(year, month)[1]  # 计算采样天数，使用monthlen（）方法有些python版本会报错
    print('当前时间为：'+ str(datetime.datetime.now()))
    print('本月采样天数为：' + str(sampling_days))
    global sampling_points, five_percent_sampling_points
    sampling_points = sampling_days * 12 * 24  # 计算采样点数量
    five_percent_sampling_points = math.floor(sampling_points*0.05)  # 前5%的取值点，再向下取整
    print('本月采样点数量为：' + str(sampling_points))
    print('本月前5%采样点数量为：' + str(five_percent_sampling_points))
    five_percent_sampling_points = five_percent_sampling_points + 1  # 这里需要加1计算实际取值点
    start_sampling_time = '%s-%s-01 00:00:00' % (year, month)  # 获取当月第一天零时零分零秒
    end_sampling_time = '%s-%s-%s 23:59:59' % (year, month, sampling_days)  # 获取当月最后一天23时59分59秒
    start_sampling_time1 = datetime.datetime.strptime(start_sampling_time, "%Y-%m-%d %H:%M:%S")  # 转换为time格式时间，下同
    end_sampling_time1 = datetime.datetime.strptime(end_sampling_time, "%Y-%m-%d %H:%M:%S")
    # 这里由于start_sampling_time是str格式，所以要先用strp转换为time格式，再使用mktime转换为unixtime格式
    # 由于zabbix系统使用的是10位unixtime，所以打印的时候要先用round四舍五入，再用int取整
    global start_sampling_time2, end_sampling_time2
    # 这里将start_sampling_time2和end_sampling_time2调用全局变量，方便后面引用
    start_sampling_time2 = int(round(time.mktime(start_sampling_time1.timetuple())))
    end_sampling_time2 = int(round(time.mktime(end_sampling_time1.timetuple())))
    print('本月采样开始时间为：' + str(start_sampling_time2))
    print('本月采样结束时间为：' + str(end_sampling_time2))
    # 要考虑实际采样达不到采样点数量的情况


# 登录MySQL，进行数据查询和取回本地
def connect_and_operate_database():
    # 连接zabbix的数据库,charset参数是为了避免中文乱码，cursorclass是为了让后面fetchall返回的对象是dict字典
    db = pymysql.connect(host="", user="root", password="", port=3306, database="zabbix1", charset='utf8',
                         cursorclass=pymysql.cursors.DictCursor)
    # 创建游标
    cursor = db.cursor()
    # 进行查询，先去items表里获取itemid对应的value_type值
    value_type_sql_statement = "SELECT value_type FROM items WHERE itemid=%s" % itemid
    cursor.execute(value_type_sql_statement)
    value_results = cursor.fetchone()
    global history_type
    # 从fetchone的字典里取出value_type值在value_type字典的中匹配，字典采用get方式获得key对应的value值
    value_results2 = value_results['value_type']
    history_type = value_type.get(str(value_results2))
    # 进行查询，这里使用字符串格式化将上面计算的start_sampling_time和end_sampling_time两个全局变量传入，指定itemid降序排列
    sql_statement = "SELECT * FROM (SELECT @rownum:=@rownum+1 AS rownum,itemid,clock,value,ns FROM (SELECT @rownum:=0)r," \
                    "%s  WHERE clock > %s AND clock < %s AND itemid = %s ORDER BY value DESC)AS TEST WHERE rownum=%s;"\
                    % (history_type, start_sampling_time2, end_sampling_time2, itemid, five_percent_sampling_points)
    try:
        cursor.execute(sql_statement)
    # 获取查询结果
        global results
        results = cursor.fetchall()
        print('共获取采样点个数为：' + str(len(results)) + '；采样表格为：' + str(history_type) + '；监控id为：' + str(itemid))
        print('第' + str(five_percent_sampling_points) + '个采样点的带宽峰值为' + str(results[0]['value']/1000/1000) + 'Mb/s')
        print('第' + str(five_percent_sampling_points) + '个采样点的带宽峰值为' + str(results[0]['value']/1000/1000/1000) + 'Gb/s')
    except BaseException as error:
        print("Error:unable to fetch data")
    # 关闭数据库
    db.close()
# 调用API取值，待开发

# 以下执行流程


if __name__ == '__main__':
    # sampling_type = input('请输入数字选择采样模式，按回车结束，1.指定时间模式；2.使用当前时间模式：')
    # if sampling_type < str(2):
    # specified_sampling()
    # else:
    # current_sampling()
    current_sampling()
    print('执行当前时间采样模式')
    input_itemids = input('请输入需要监控的id，以英文逗号为间隔，按回车结束：').split(",")  # 将监控项传入转换为list
    for b in input_itemids: # 对列表进行遍历，每个itemid执行一次connect_and_operate_database模块
        itemid = b
        connect_and_operate_database()







