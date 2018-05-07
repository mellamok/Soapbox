import datetime
import Puller
import Ranker
from apscheduler.schedulers.blocking import BlockingScheduler

def cycle_1_min():
    #Pass the timestamp to the various programs
    now = datetime.datetime.now()

    #File components
    iteration = now
    iteration_str = "{year} {month} {day} {hour} {minute}".format(year=now.year, month=now.month, day=now.day, hour=now.hour, minute=now.minute)
    iteration_file_puller = "Puller {year} {month} {day} {hour} {minute}.sqlite".format(year=now.year, month=now.month, day=now.day, hour=now.hour, minute=now.minute)

    #Do Puller
    print("Pulling file = ", iteration_file_puller)
    startpull = Puller.Scrape(iteration_file_puller)
    startpull.main()
    print("Pulling complete!")
    pullerfinish = datetime.datetime.now() - now
    print("Time to Pull: ", pullerfinish)
    

    #Do Alltime Ranker
    print("Ranking alltime...")
    alltime = Ranker.Ranker(iteration, iteration_str, "alltime")
    alltime.main()
    print("Ranking alltime complete!")

    #Do Daily Ranker
    print("Ranking daily...")
    daily = Ranker.Ranker(iteration, iteration_str, "daily")
    daily.main()
    print("Ranking daily complete!")
    
    #Do Hourly Ranker
    print("Ranking hourly...")
    hourly = Ranker.Ranker(iteration, iteration_str, "hourly")
    hourly.main()
    print("Ranking hourly complete!")

    #Program Time
    finishtime = datetime.datetime.now() - now
    print("Time to Complete: ", finishtime)

scheduler=BlockingScheduler()
scheduler.add_job(cycle_1_min, 'cron', second=1)
scheduler.start()