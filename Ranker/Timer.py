import time
import datetime

start_time = datetime.datetime.now()

def five_second_time():
    try: dt
    except NameError: dt = None
    while True:
        if dt is None:
            dt = datetime.datetime.now() + datetime.timedelta(seconds=5)
        else:
            while datetime.datetime.now() < dt:
                time.sleep(1)
            print('Time is ', dt)
            dt = datetime.datetime.now() + datetime.timedelta(seconds=5)


while True:
    input_cmd = input('Enter Command:')
    print("Entered:" + input_cmd)

    #Exit
    if input_cmd.strip() == "stop":
        print('Stopping Code')
        break

    #Status
    if input_cmd.strip() == "time":
        print("Current date and time: " , datetime.datetime.now())
        current_time = datetime.datetime.now()
        elapsed_time = current_time - start_time
        print("Time since program start: " , elapsed_time)
    
    #5 Second Call
    if input_cmd.strip() == "5sec":
        five_second_time()

    #Test
    if input_cmd.strip() == "echo":
        print('Echoing command ' + input_cmd)
        
    else:
        print('Sorry, command is unrecognized.')

    
