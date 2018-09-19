from time import sleep

from apscheduler.schedulers.blocking import BlockingScheduler


def a_cycle():
    print("Tick")
    sleep(2)
    print("Tock")
    sleep(2)
    

def b_cycle():
    print("In")
    sleep(2)
    print("Out")
    sleep(2)


scheduler=BlockingScheduler()
scheduler.add_job(a_cycle, 'cron', second = '*/5')
scheduler.add_job(b_cycle, 'cron', second = '*/5')
scheduler.start()
