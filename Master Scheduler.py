import datetime
import Puller
import Ranker
import Popcorn
from apscheduler.schedulers.blocking import BlockingScheduler

def cycle_1_min():
    #Pass the timestamp to the various programs
    now = datetime.datetime.now()

    #File components
    iteration = now
    iteration_str = "{year} {month} {day} {hour} {minute}".format(year=now.year, month=now.month, day=now.day, hour=now.hour, minute=now.minute)
    iteration_file_puller = "Puller {year} {month} {day} {hour} {minute}.sqlite".format(year=now.year, month=now.month, day=now.day, hour=now.hour, minute=now.minute)

    #Do Puller
    pullerids = ["%23soapbox -filter:retweets -filter:media", "@internetsoapbox -filter:retweets -filter:media",]

    print("Pulling file = ", iteration_file_puller)
    startpull = Puller.Scrape(iteration_file_puller, pullerids)
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

    #return startvar and stable iterations
    global stable_iteration
    stable_iteration = iteration
    global stable_iteration_str
    stable_iteration_str = iteration_str

    global regen_popcorn_flag
    regen_popcorn_flag = 1

    global first_start_var
    first_start_var = 1

def popcorn_cycle():
    global first_start_var
    global regen_popcorn_flag
    global stable_iteration
    global stable_iteration_str
    global lasttweetdate
    global lasttweetid
    global popcorn


    if first_start_var == 0:
        print('Not Finished Setup')
        lasttweetdate = None
        lasttweetid = None
    elif first_start_var == 1:
        if regen_popcorn_flag == 1:
            print(lasttweetdate)
            print(lasttweetid)
            popcorn = Popcorn.Popcorn(stable_iteration, stable_iteration_str, "daily", lasttweetid, lasttweetdate)
            popcorn.main()
            regen_popcorn_flag = 0

        elif regen_popcorn_flag == 0:
            popcorn.main()
            lasttweetdate = popcorn.lasttweetdate
            lasttweetid = popcorn.lasttweetid


if __name__ == "__main__":
    global first_start_var
    first_start_var = 0
    global regen_popcorn_flag
    regen_popcorn_flag = 0
    global popcorn
    popcorn = None
    
    scheduler=BlockingScheduler()
    scheduler.add_job(cycle_1_min, 'cron', second=1)
    scheduler.add_job(popcorn_cycle, 'cron', second='*/10')
    scheduler.start()