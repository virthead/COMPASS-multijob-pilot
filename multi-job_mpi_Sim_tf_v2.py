#!/usr/bin/env python

import sys
import os
import subprocess
import time
from os.path import abspath as _abspath, join as _join
from mpi4py import MPI
import random

#---------------------------------------------
# Main start here
#---------------------------------------------
# Obtain MPI rank
def main():
    start_g = time.time()
    start_g_str = time.asctime(time.localtime(start_g))
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    max_rank = comm.Get_size()
    print("%s | Rank: %s from %s ranks started" % (start_g_str, rank, max_rank))

    # Get a file name with job descriptions
    input_file = sys.argv[1]
    
    #Read file into an array removing new line.
    try:
        file = open(input_file)
        temp = file.read().splitlines()
        file.close()
    except IOError as e:
        print("I/O error({0}): {1}" . format(e.errno, e.strerror))
        print("Exit from rank %s" % (rank))
        return
    
    # Number of commands in the input file
    n_comm = len(temp)
    #print("Number of commands: %s" % n_comm)
    
    # Safeguard here. Just in case things are messed up.
    if n_comm != max_rank:
        print("Rank %s: problem with input file! Number of commands not equal to number of ranks! Exiting" % (rank))
        exit()
    
    #PandaID of the job for the command
    wkdirname = temp[rank].split("!!")[0].strip()
    # Get the corresponding command from input file
    my_command = temp[rank].split("!!")[1]
    #filename for stdout & srderr
    payloadstdout_s = temp[rank].split("!!")[2]
    payloadstderr_s = temp[rank].split("!!")[3]
    
    PanDA_jobId = wkdirname[wkdirname.find("_") + 1:]    

    # create working directory for a given rank
    curdir = _abspath (os.curdir)

    # wkdirname = "PandaJob_%s" % my_job_index
    wkdir = _abspath (_join(curdir, wkdirname))
    if not os.path.exists(wkdir):
        os.mkdir(wkdir)
    '''
        print "Directory ", wkdir, " created"
    else:
        print "Directory ", wkdir, " exists"
    '''
    # Go to local working directory (job rank)
    os.chdir(wkdir)
    
    # If rank == 0 then setup MySQL db, all others wait for db to start
    if rank % 32 == 0:
        print("Rank %s is going to start MySQL db" % (rank))
        print("Preparing environment")
        dbsetup_comm_arr = ['cp /projects/sciteam/balh/panda/db_setup.sh .',
                            'sh db_setup.sh &>dbsetup.log &'
                        ]
        dbsetup_comm = "\n" . join(dbsetup_comm_arr)
        p = subprocess.Popen(dbsetup_comm, shell=True)
        time.sleep(90)
        
        print("Going to check MySQL server status")
        output = subprocess.Popen("ps -C mysqld -o pid=", stdout=subprocess.PIPE, shell=True).communicate()[0]
        print("MySQL server pid: %s" % output.rstrip())
        if len(output) > 0:
            print("MySQL database is running, going to send GO! broadcast")
            
            output = subprocess.Popen("echo $HOSTNAME", stdout=subprocess.PIPE, shell=True).communicate()[0]
            print("MySQL database is running on %s" % output.rstrip())
        else:
            print("MySQL database is not running, exiting")
    else:
        print("Rank %s is going to sleep a little bit to allow MySQL db to start" % (rank))
        time.sleep(90 + random.randint(10, 100))
    
    mysql_host = "0.0.0.0"
    coral_cdbserver_comm = "export CDBSERVER=%s;" % mysql_host
    my_command = coral_cdbserver_comm + my_command
#    pcmd = subprocess.call(coral_cdbserver_comm, shell=True)
    
    payloadstdout = open(payloadstdout_s, "w")
    payloadstderr = open(payloadstderr_s, "w")
    
    localtime = time.asctime(time.localtime(time.time()))
    print("%s | %s Rank: %s Starting payload: %s" % (localtime, PanDA_jobId, rank, my_command))
    t0 = os.times()
    exit_code = subprocess.call(my_command, stdout=payloadstdout, stderr=payloadstderr, shell=True)
    t1 = os.times()
    localtime = time.asctime(time.localtime(time.time()))
    t = map(lambda x, y:x-y, t1, t0)
    t_tot = reduce(lambda x, y:x+y, t[2:3])
    
    report = open("rank_report.txt", "w")
    report.write("cpuConsumptionTime: %s\n" % (t_tot))
    report.write("exitCode: %s" % exit_code)
    report.close()
    
    # Finished all steps here
    localtime = time.asctime( time.localtime(time.time()) )
    end_all = time.time()
    print("%s | %s Rank: %s Finished. Program run took.  %s min. Exit code: %s cpuConsumptionTime: %s" % (localtime, PanDA_jobId,rank,(end_all-start_g)/60. ,exit_code, t_tot))

    return 0
    # Wait for all ranks to finish
    #comm.Barrier()

if __name__ == "__main__":
    sys.exit(main())
