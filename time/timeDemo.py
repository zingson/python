import time
import calendar

ticks = time.time()

print("当前时间戳为：",ticks)

t2 = time.localtime()
print("t2:",t2)

print("格式化时间：",time.strftime("%Y-%m-%d %H:%M:%S",time.localtime()))
print("解析时间：",time.strptime("2018-10-11 12:32:24","%Y-%m-%d %H:%M:%S"))

cal = calendar.month(2018,10)
print(cal)
