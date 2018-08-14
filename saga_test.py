import saga

try:
    # add the full job command to the job_setup.sh file
    to_script = "\n".join(setup_commands)
    to_script = to_script + "\n" +"\n".join(_env)
    #to_script = "%s\naprun -n %s -d %s %s/multi-job_mpi_Sim_tf_v2.py list_jobs" % (to_script, cpu_number/self.number_of_threads, self.number_of_threads, cur_dir)    
    to_script = "%s\nmpiexec -pernode /u/sciteam/petrosya/panda/hello.py list_jobs" % to_script    
    
    # Simple SAGA fork variant
    tolog("******* SAGA call to execute payload *********")
    try:

        js = saga.job.Service("pbspro://localhost")
        jd = saga.job.Description()
        jd.project = self.project_id # should be taken from resourse description (pandaqueue)
        jd.wall_time_limit = walltime 
        jd.executable      = to_script
        jd.total_cpu_count = cpu_number 
        jd.output = "MPI_stdout.txt"
        jd.error = "MPI_stderr.txt"
        jd.queue = self.executed_queue   # should be taken from resourse description (pandaqueue)
        jd.working_directory = cur_dir
        
        fork_job = js.create_job(jd)
        fork_job.add_callback(saga.STATE, self.jobStateChangeNotification)
        
        #tolog("\n(PBS) Command: %s\n"  % to_script)
        fork_job.run()
        tolog("Local Job ID: %s" % fork_job.id)
        for j in jobs:
            j.hpcStatus = fork_job.state 

        for i in range(self.waittime * 60):
            time.sleep(1)
            if fork_job.state != saga.job.PENDING:
                for j in jobs:
                    j.hpcStatus = fork_job.state
                break
        if fork_job.state == saga.job.PENDING:
            repeat_num = repeat_num + 1
            tolog("Wait time (%s min.) exceed, job will be rescheduled (%s)" % (self.waittime, repeat_num))
            fork_job.cancel()
            fork_job.wait()
            for j in jobs:  
                j.hpcStatus = "Re-scheduled By Pilot"
            #rt = RunJobUtilities.updatePilotServer(j, self.getPilotServer(), self.getPilotPort())
            return self.executePayload(thisExperiment, runCommandsList, jobs, repeat_num)
            
            #tolog("Wait time (%s s.) exceed, job cancelled" % self.waittime)
        
        for j in jobs[:]: 
            j.coreCount = self.cpu_number_per_node
            if j.jobId in _to_be_failed:
                tolog("Job %s should be failed as rejected by LRMS" % j.jobId)
                j.hpcStatus = "Rejected"
                runJob.failJob(0, 0, j, ins=j.inFiles, pilotErrorDiag='No availble slot')
                jobs.remove(j)
            else:
                j.hpcStatus = fork_job.state
                hpcjobId = fork_job.id
                j.HPCJobId = hpcjobId.split('-')[1][1:-1]
                j.startTime = self.GetUTCTime(fork_job.started)
                rt = RunJobUtilities.updatePilotServer(j, self.getPilotServer(), self.getPilotPort())
                if rt:
                    tolog("TCP Server updated with HPC Status and CoreCount")
                    tolog("Job start time: %s" % j.startTime)
                else:
                    tolog("TCP Server NOT updated with HPC Status and CoreCount (WHY? - who knows ;-)")
                
            
        fork_job.wait()
        tolog("MPI task ID            : %s" % (fork_job.id))
        tolog("Job State              : %s" % (fork_job.state))
        tolog("Exitcode               : %s" % (fork_job.exit_code))
        tolog("Create time            : %s" % (fork_job.created))
        tolog("Start time             : %s" % (fork_job.started))
        tolog("End time               : %s" % (fork_job.finished))
        tolog("Walltime limit         : %s (min)" % (jd.wall_time_limit))
        tolog("Allocated nodes (cores): %s (%s)" % (nodes,cpu_number))
        cons_time = datetime.strptime(fork_job.finished, '%c') - datetime.strptime(fork_job.started, '%c')
        cons_time_sec =  (cons_time.microseconds + (cons_time.seconds + cons_time.days * 24 * 3600) * 10**6) / 10**6
        tolog("Execution time         : %s (sec. %s)" % (str(cons_time), cons_time_sec))

        UTCstartTime = self.GetUTCTime(fork_job.started)
        if fork_job.started == fork_job.finished:
            UTCendTime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        else:
            UTCendTime = self.GetUTCTime(fork_job.finished)

        tolog("Start time (jobDef)    : %s " % UTCstartTime)
        tolog("End time (jobDef)      : %s " % UTCendTime)

        if not fork_job.exit_code is None:
            _final_hpc_status = fork_job.state+"_EC_"+str(fork_job.exit_code)
            res_tuple = (fork_job.exit_code, "Look into: %s" % jd.output)
        else:
            _final_hpc_status = fork_job.state+"_EC_"+str(fork_job.exit_code)
            res_tuple = (0, "Look into: %s" % jd.output)

        ####################################################
    except saga.SagaException, ex:
        # Catch all saga exceptions
        tolog("An exception occured: (%s) %s " % (ex.type, (str(ex))))
        # Trace back the exception. That can be helpful for debugging.
        tolog(" \n*** Backtrace:\n %s" % ex.traceback)
        
    tolog("**********************************************")
    tolog("******* SAGA call finished *******************")
    tolog("**********************************************")  

    for j in jobs:
        j.hpcStatus = _final_hpc_status
        j.coreCount = self.cpu_number_per_node
        j.startTime = UTCstartTime
        j.endTime = UTCendTime
        j.timeExe = cons_time_sec
        rt = RunJobUtilities.updatePilotServer(j, self.getPilotServer(), self.getPilotPort())
        if rt:
            tolog("TCP Server updated with HPC Status and CoreCount. Start time: %s End time: %s" %(j.startTime, j.endTime))
        else:
            tolog("TCP Server NOT updated with HPC Status and CoreCount (WHY? - who knows ;-)")

except Exception, e:
    tolog("!!FAILED!!3000!! Failed to run command %s" % str(e))
    if 'format_exc' in traceback.__all__:
        tolog(" \n*** Backtrace:\n %s" % traceback.format_exc())
    getstatusoutput_was_interrupted = True
    if self.getFailureCode:
        for j in jobs: 
            j.result[2] = self.getFailureCode()
        tolog("!!FAILED!!3000!! Failure code: %s" % (self.getFailureCode()))

