import datetime
from apscheduler.schedulers.blocking import BlockingScheduler

def augie_30_sec():
    print("I play at every half minute in the day. The time is: ", datetime.datetime.now())

scheduler=BlockingScheduler()
scheduler.add_job(augie_30_sec, 'cron', second=30)
scheduler.start()