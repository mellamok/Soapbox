import datetime
import Puller
import Ranker
from apscheduler.schedulers.blocking import BlockingScheduler

def cycle_1_min():
    #Pass the timestamp to the various programs
    now = datetime.datetime.now()

    #File components
    iteration = "%d %d %d %d %d" % (now.year, now.month, now.day, now.hour, now.minute)
    iteration_file_puller = "Puller %d %d %d %d %d.sqlite"  % (now.year, now.month, now.day, now.hour, now.minute)
    iteration_file_ranker = "Ranker %d %d %d %d %d.csv" % (now.year, now.month, now.day, now.hour, now.minute)

    #Do Puller
    print("I play at every half minute in the day. The time is: ", datetime.datetime.now())
    print("Puller file = ", iteration_file_puller)
    startpull = Puller.Scrape(iteration_file_puller)
    startpull.main()
    
    #Do Alltime Ranker
    print("Ranker file = ", iteration_file_ranker)
    alltime = Ranker.Ranker(iteration, "alltime")
    alltime.main()
    
    """ data_master_loc = Ranker.find_file_location(iteration_file_puller) #define the main data
    conn_master = Ranker.create_master_connection(data_master_loc) #connect to it
    x = Ranker.datapull_master(conn_master) #Pull all data from hastags table and display it
    for row in x:
        print(row)
    Ranker.to_csv(x, iteration_file_ranker)
    # """
scheduler=BlockingScheduler()
scheduler.add_job(cycle_1_min, 'cron', second=1)
scheduler.start()