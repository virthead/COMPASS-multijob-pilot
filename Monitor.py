import atexit
import time
import os
import commands
import sys
import pUtil
import signal
import traceback

from shutil import copy, copy2
from random import shuffle
from glob import glob
from JobRecovery import JobRecovery
from processes import killProcesses, checkProcesses, killOrphans, getMaxMemoryUsageFromCGroups, isCGROUPSSite
from PilotErrors import PilotErrors
from FileStateClient import createFileStates, dumpFileStates, getFileState
from WatchDog import WatchDog
from PilotTCPServer import PilotTCPServer
from UpdateHandler import UpdateHandler
from RunJobFactory import RunJobFactory
from threading import Thread

import inspect
from pUtil import tolog

def lineno():
    """ Returns the current line number in our program """

    return inspect.currentframe().f_back.f_lineno

globalSite = None

class Monitor:

    __skip = False
    __localsizelimit_stdout = 2*1024**2   # size limit of payload stdout size during running. unit is in kB

    def __init__(self, env):
        self.__error = PilotErrors()
        self.__env = env

        #self.__env['pilot_startup'] = None
        self.__env['create_softlink'] = True
        self.__env['return_code'] = None
        self.__env['curtime_sp'] = int(time.time())
        self.__env['lastTimeFilesWereModified'] = {}
        self.__wdog = WatchDog()
        self.__runJob = None # Remember the RunJob instance

        # register cleanup function
        atexit.register(pUtil.cleanup, self.__wdog, self.__env['pilot_initdir'], self.__env['wrapperFlag'], self.__env['rmwkdir'])

        signal.signal(signal.SIGTERM, pUtil.sig2exc)
        signal.signal(signal.SIGQUIT, pUtil.sig2exc)
        signal.signal(signal.SIGSEGV, pUtil.sig2exc)
        signal.signal(signal.SIGXCPU, pUtil.sig2exc)
        signal.signal(signal.SIGBUS,  pUtil.sig2exc)
        signal.signal(signal.SIGUSR1, pUtil.sig2exc)

    def __allowLoopingJobKiller(self):
        """ Should the looping job killer be run? """

        # This decision is not on an Experiment level but is relevant only for the type of subprocess that is requested
        # E.g. the looping job killer should normally be run for a normal ATLAS job, but not on an HPC. Therefore, the
        # decision is made inside the subprocess class (RunJob, RunJobHPC, ..)

        allow = True

        # Which RunJob class do we need to ask?
        for j in self.__env['jobDic']["prod"][1]:
            if not self.__runJob:
                # First get an Experiment object, which will tell us which subprocess to ask
                thisExperiment = pUtil.getExperiment(self.__env['experiment'])
                subprocessName = thisExperiment.getSubprocessName(j.eventService)
                pUtil.tolog("subprocessName = %s" % (subprocessName))
    
                # Now get an instance of the corresponding class from the RunJobFactory
                factory = RunJobFactory()
                _rJ = factory.newRunJob(subprocessName)
                self.__runJob = _rJ()
    
            if self.__runJob:
                # Is the looping job killer allowed by the subprocess?
                allow = self.__runJob.allowLoopingJobKiller()
                name = os.path.splitext(self.__runJob.getRunJobFileName())[0]
                if allow:
                    pUtil.tolog("Looping job killer is allowed by subprocess %s" % (name))
                else:
                    pUtil.tolog("Looping job killer is not allowed by subprocess %s" % (name))
            else:
                pUtil.tolog("!!WARNING!!2121!! Could not get an instance of a RunJob* class (cannot decide about looping job killer)")
                allow = False

        return allow

    def __checkPayloadStdout(self):
        """ Check the size of the payload stdout """    
    
        _workdir = self.__env['thisSite'].workdir
        
        fileList = []
        for j in self.__env['jobDic']['prod'][1]:
            fileList += glob("%s/log.*" % j.workdir)
            fileList.append("%s/%s" % (j.workdir, j.stdout))
        
        fileList = list(set(fileList))
        
            # now loop over all files and check each individually (any large enough file will fail the job)
        for filename in fileList:
           
            if os.path.exists(filename):
                try:
                    # get file size in bytes
                    fsize = os.path.getsize(filename)
                except Exception, e:
                    pUtil.tolog("!!WARNING!!1999!! Could not read file size of %s: %s" % (filename, str(e)))
                else:
                    # is the file too big?
                    if fsize > self.__env['localsizelimit_stdout'] * 1024: # convert to bytes
                        pilotErrorDiag = "Payload stdout file too big: %d B (larger than limit %d B)" % (fsize, self.__env['localsizelimit_stdout'] * 1024)
                        pUtil.tolog("!!FAILED!!1999!! %s" % (pilotErrorDiag))
                        # kill the job
                        #pUtil.tolog("Going to kill pid %d" %lineno())
                        killProcesses(self.__env['jobDic']['prod'][0], self.__env['jobDic']['prod'][2])
                        for j in self.__env['jobDic'][1][0]:
                            j.result[0] = "failed"
                            j.currentState = j.result[0]
                            j.result[2] = self.__error.ERR_STDOUTTOOBIG
                            j.pilotErrorDiag = pilotErrorDiag
                            # remove any lingering input files from the work dir
                            if j.inFiles:
                                if len(j.inFiles) > 0:
                                    ec = pUtil.removeFiles(j.workdir, j.inFiles)
                        
                        self.__skip = True

                        # remove the payload stdout file after the log extracts have been created

                    else:
                        pUtil.tolog("Payload stdout (%s) within allowed size limit (%d B): %d B" % (filename, self.__env['localsizelimit_stdout']*1024, fsize))
            else:
                    pUtil.tolog("(Skipping file size check of payload stdout file (%s) since it has not been created yet)" % (filename))

    def __getMaxWorkDirSize(self):
        """
        Return the maximum allowed size of the work directory for user jobs
        """
    
        try:
            maxwdirsize = int(pUtil.readpar('maxwdir'))*1024**2 # from MB to B, e.g. 16336 MB -> 17,129,537,536 B
        except:
            maxInputSize = pUtil.getMaxInputSize()
            maxwdirsize = maxInputSize + self.__env['localsizelimit_stdout']*1024
            pUtil.tolog("Work directory size check will use %d B as a max limit (maxinputsize [%d B] + local size limit for stdout [%d B])" %\
                  (maxwdirsize, maxInputSize, self.__env['localsizelimit_stdout']*1024))
        else:
            pUtil.tolog("Work directory size check will use %d B as a max limit (maxwdirsize)" % (maxwdirsize))
    
        return maxwdirsize

    def __checkWorkDir(self):
        """
        Check the size of the pilot work dir for use jobs
        """
        
        # get the limit of the workdir
        maxwdirsize = self.__getMaxWorkDirSize()
    
        # after multitasking was removed from the pilot, there is actually only one job
        for j in self.__env['jobDic']['prod'][1]:
            # get size of workDir
            workDir = "%s" % (j.workdir)
            if os.path.exists(workDir):
                try:
                    # get the size in kB
                    size_str = commands.getoutput("du -sk %s" % (workDir))
                except Exception, e:
                    pUtil.tolog("Warning: failed to check remaining space: %s, %s" % (workDir, str(e)))
                else:
                    # e.g., size_str = "900\t/scratch-local/nilsson/pilot3z"
                    try:
                        # remove tab and path, and convert to int (and B)
                        size = int(size_str.split("\t")[0])*1024
                    except Exception, e:
                        pUtil.tolog("Warning: failed to convert to int: %s" % str(e))
                    else:
                        # is user dir within allowed size limit?
                        if size > maxwdirsize:
                            pilotErrorDiag = "Work directory (%s) too large: %d B (must be < %d B)" %\
                                             (workDir, size, maxwdirsize)
                            pUtil.tolog("!!FAILED!!1999!! %s" % (pilotErrorDiag))
    
                            # kill the job
                            killProcesses(self.__env['jobDic']['prod'][0], self.__env['jobDic']['prod'][2])
                            j.result[0] = "failed"
                            j.currentState = j.result[0]
                            j.result[2] = self.__error.ERR_USERDIRTOOLARGE
                            j.pilotErrorDiag = pilotErrorDiag
                            self.__skip = True

                            # remove any lingering input files from the work dir
                            if j.inFiles:
                                if len(j.inFiles) > 0:
                                    ec = pUtil.removeFiles(j.workdir, j.inFiles)
                        else:
                            pUtil.tolog("Checked size of user analysis work directory %s: %d B (within %d B limit)" 
                                        %(workDir, size, maxwdirsize))
            else:
                pUtil.tolog("(Skipping size check of workDir since it has not been created yet)")
    
    def __checkLocalSpace(self, disk):
        """ check the remaining local disk space during running """
    
        spaceleft = int(disk)*1024**2 # B (node.disk is in MB)
        _localspacelimit = self.__env['localspacelimit'] * 1024 # B
    
        # do we have enough local disk space to continue running the job?
        if spaceleft < _localspacelimit:
            pilotErrorDiag = "Too little space left on local disk to run job: %d B (need > %d B)" % (spaceleft, _localspacelimit)
            pUtil.tolog("!!FAILED!!1999!! %s" % (pilotErrorDiag))
            # kill the job
            killProcesses(self.__env['jobDic']['prod'][0], self.__env['jobDic']['prod'][2])
            for j in self.__env['jobDic']['prod'][1]:
                #pUtil.tolog("Going to kill pid %d" %lineno())
                j.result[0] = "failed"
                j.currentState = j.result[0]
                j.result[2] = self.__error.ERR_NOLOCALSPACE
                j.pilotErrorDiag = pilotErrorDiag
                self.__skip = True

                # remove any lingering input files from the work dir
                if j.inFiles:
                    if len(j.inFiles) > 0:
                        ec = pUtil.removeFiles(j.workdir, j.inFiles)
        else:
            pUtil.tolog("Remaining local disk space: %d B" % (spaceleft))
    
    def __check_remaining_space(self):
        """
        every ten minutes, check the remaining disk space and size of user workDir (for analysis jobs)
        and the size of the payload stdout file
        """
        if (int(time.time()) - self.__env['curtime_sp']) > self.__env['update_freq_space']:
            # check the size of the payload stdout
            self.__skip = self.__checkPayloadStdout()

            # update the worker node info (i.e. get the remaining disk space)
            self.__env['workerNode'].collectWNInfo(self.__env['thisSite'].workdir)
            self.__skip = self.__checkLocalSpace(self.__env['workerNode'].disk)

            # check the size of the workdir for user jobs
            if self.__env['uflag']:
                self.__skip = self.__checkWorkDir()

            # update the time for checking disk space
            self.__env['curtime_sp'] = int(time.time())
            
    def __createSoftLink(self):
        """ create a soft link to the athena stdout in the site work dir """
        # create_softlink is mutable
    
        # will only point to the first stdout file currently
        for k in self.__env['jobDic'].keys():
            # is this a multi-trf job?
            nJobs = self.__env['jobDic'][k][1].jobPars.count("\n") + 1
            for _i in range(nJobs):
                _stdout = self.__env['jobDic'][k][1].stdout
                if nJobs > 1:
                    _stdout = _stdout.replace(".txt", "_%d.txt" % (_i + 1))
                filename = os.path.join(self.__env['jobDic'][k][1].workdir, _stdout)
                # create the soft link only if the stdout has been created
                if os.path.exists(filename):
                    lnfilename = os.path.join(self.__env['thisSite'].workdir, _stdout)
                    # only create the soft link once..
                    if not os.path.exists(lnfilename):
                        # ..and only if the size of stdout is > 0
                        if os.path.getsize(filename) > 0:
                            ec, rs = commands.getstatusoutput("ln -s %s %s" % (filename, lnfilename))
                            if ec == 0:
                                pUtil.tolog("Created soft link to %s in sitedir: %s" % (_stdout, lnfilename))
                            else:
                                pUtil.tolog("!!WARNING!!1999!! Could not create soft link: %d, %s" % (ec, rs))
                            self.__env['create_softlink'] = False
                    else:
                        self.__env['create_softlink'] = False
                else:
                    pUtil.tolog("(%s has not been created yet)" % (_stdout))
    
    def create_softlink(self):
        """
        create a soft link in the site workdir to the payload stdout
        """
        if self.__env['create_softlink']:
            try:
                self.__createSoftLink()
            except Exception, e:
                pUtil.tolog("!!WARNING!!1999!! Caught an exception during soft link creation: %s" % str(e))
                self.__env['create_softlink'] = False
    
    def __failMaxTimeJob(self):
        """
        Reached maximum batch system time limit, fail the job
        """
    
        pUtil.tolog("!!WARNING!!1999!! The pilot has decided to kill the job since there is less than 10 minutes of the allowed batch system running time")
        pilotErrorDiag = "Reached maximum batch system time limit"
        pUtil.tolog("!!FAILED!!1999!! %s" % (pilotErrorDiag))
    
        # after multitasking was removed from the pilot, there is actually only one job
        killProcesses(self.__env['jobDic']['prod'][0], self.__env['jobDic']['prod'][2])
        
        for j in self.__env['jobDic']['prod'][1]:
            # kill the job
            #pUtil.tolog("Going to kill pid %d" %lineno())
            j.result[0] = "failed"
            j.currentState = j.result[0]
            j.result[2] = self.__error.ERR_REACHEDMAXTIME
            j.pilotErrorDiag = pilotErrorDiag
    
    def __monitor_processes(self):
        # monitor the number of running processes and the pilot running time
        if (int(time.time()) - self.__env['curtime_proc']) > self.__env['update_freq_proc']:
            # check the number of running processes
            nProc = checkProcesses(self.__env['jobDic']["prod"][0])
            if nProc > self.__env['maxNProc']:
                self.__env['maxNProc'] = nProc

            # monitor the pilot running time (once every five minutes = update_freq_proc)
            time_passed_since_pilot_startup = int(time.time()) - self.__env['pilot_startup']
            pUtil.tolog("Time passed since pilot startup = %d s (maximum allowed batch system time = %d s)" 
                        % (time_passed_since_pilot_startup, self.__env['maxtime']))
            if (self.__env['maxtime'] - time_passed_since_pilot_startup) < 10*60 and not self.__env['stageout']:
                # reached maximum batch system time limit
                self.__failMaxTimeJob()
                self.__skip = True

            # update the time for checking processes
            self.__env['curtime_proc'] = int(time.time())
            
    def __verifyOutputFileSizes(self):
        """ Verify output file sizes """
    
        pilotErrorDiag = ""
        job_index = 0
        rc = 0
    
        pUtil.tolog("Verifying output file sizes")
        for j in self.__env['jobDic']['prod'][1]:
            if len(j.outFiles) > 0:
                for file_name in j.outFiles:
                    findFlag = False
                    # locate the file first
                    out = commands.getoutput("find %s -name %s" % (j.workdir, file_name))
                    if out != "":
                        for line in out.split('\n'):
                            try:
                                file_size = os.path.getsize(line)
                                findFlag = True
                                if file_size > self.__env['outputlimit']:
                                    pilotErrorDiag = 'File: \"%s\" is too large %d > %d B)' % (line, file_size, self.__env['outputlimit'])
                                    pUtil.tolog('!!WARNING!!2999!!%s' % (pilotErrorDiag))
                                    job_index = j
                                    rc = self.__error.ERR_OUTPUTFILETOOLARGE
                                else:
                                    pUtil.tolog('File: \"%s\" currently has size %d < %d B)' % (line, file_size, self.__env['outputlimit']))
                            except:
                                pass
                    if not findFlag and file_name != j.logFile:
    #                if not findFlag and not ".log." in file_name:
                        pUtil.tolog("Could not access file %s: %s" % (file_name, out))
    
        return rc, pilotErrorDiag, job_index
            
    def __checkOutputFileSizes(self):
        """ check that the output file sizes are within the limit """
        # return True for too large files, to skip looping test and normal server update
        
        # verify the file sizes
        rc, pilotErrorDiag, job_index = self.__verifyOutputFileSizes()
        if rc == 0:
            pUtil.tolog("Completed output file size verification")
        else:
            # found too large output file, stop the job(s)
            for j in self.__env['jobDic']['prod'][1]:
                j.result[0] = "failed"
                j.currentState = j.result[0]
                j.result[2] = rc
                j.pilotErrorDiag = pilotErrorDiag
                self.__skip = True
            
    def __verify_output_sizes(self):
        # verify output file sizes every ten minutes
        if (int(time.time()) - self.__env['curtime_of']) > self.__env['update_freq_space']:
            # check the output file sizes
            self.__skip = self.__checkOutputFileSizes()

            # update the time for checking output file sizes
            self.__env['curtime_of'] = int(time.time())

# FOR TESTING ONLY
#    def __verify_memory_limits(self):
#        # verify output file sizes every five minutes
#        if (int(time.time()) - self.__env['curtime_mem']) > 1*60: #self.__env['update_freq_mem']:
#            # check the CGROUPS memory
#            max_memory = getMaxMemoryUsageFromCGroups()
#            if max_memory:
#                pUtil.tolog("cgroups max_memory = %s" % (max_memory))
#            else:
#                pUtil.tolog("cgroups max_memory not defined")
#
#            # update the time for checking memory
#            self.__env['curtime_mem'] = int(time.time())
            
    def __killLoopingJob(self, job, pid, setStageout=False):
        """ kill the looping job """
    
        # the child process is looping, kill it
        pilotErrorDiag = "Pilot has decided to kill looping job %s at %s" %\
                         (job.jobId, pUtil.timeStamp())
        pUtil.tolog("!!FAILED!!1999!! %s" % (pilotErrorDiag))
    
        cmd = 'ps -fwu %s' % (commands.getoutput("whoami"))
        pUtil.tolog("%s: %s" % (cmd + '\n', commands.getoutput(cmd)))
        cmd = 'ls -ltr %s' % (job.workdir)
        pUtil.tolog("%s: %s" % (cmd + '\n', commands.getoutput(cmd)))
        cmd = 'ps -o pid,ppid,sid,pgid,tpgid,stat,comm -u %s' % (commands.getoutput("whoami"))
        pUtil.tolog("%s: %s" % (cmd + '\n', commands.getoutput(cmd)))
    
        killProcesses(pid, job.pgrp)

        if self.__env['stagein']:
            pilotErrorDiag += " (Job stuck in stage-in state)"
            pUtil.tolog("!!FAILED!!1999!! Job stuck in stage-in state: file copy time-out")
            job.result[2] = self.__error.ERR_GETTIMEOUT
        elif setStageout:
            pilotErrorDiag += " (Job stuck in stage-out state)"
            pUtil.tolog("!!FAILED!!1999!! Job stuck in stage-out state: file copy time-out")
            job.result[2] = self.__error.ERR_PUTTIMEOUT
        else:
            job.result[2] = self.__error.ERR_LOOPINGJOB
            job.result[0] = "failed"
            job.currentState = job.result[0]
            job.pilotErrorDiag = pilotErrorDiag
    
        # remove any lingering input files from the work dir
        if job.inFiles:
            if len(job.inFiles) > 0:
                ec = pUtil.removeFiles(job.workdir, job.inFiles)
    
        return job
        
    def __updateJobs(self):
        """ Make final server update for all jobs"""
    
        # get the stdout tails
        
        #fake_dict = {"prod":[0, self.__env['jobDic']['prod'][1][0] ,0]}

        stdout_dictionary = pUtil.getStdoutDictionary(self.__env['jobDic'])
        num_of_job = len(self.__env['jobDic']['prod'][1])
        pUtil.tolog("About to update PanDA server for [%s] jobs" % num_of_job)                          
        # loop over all parallel jobs, update server, kill job if necessary
        # (after multitasking was removed from the pilot, there is actually only one job)
        t0_serverupdate = os.times()
        for j in self.__env['jobDic']['prod'][1]:
            tmp = j.result[0]
            pUtil.tolog("Update job [%s] with state [%s] " % (j.jobId, tmp))  
            if not tmp in ["finished", "failed", "holding"]:
    
                # get the tail if possible
                try:
                    self.__env['stdout_tail'] = stdout_dictionary[j.jobId]
                    index = "path-%s" % (j.jobId)
                    self.__env['stdout_path'] = stdout_dictionary[index]
                    pUtil.tolog("stdout_path=%s at index=%s" % (self.__env['stdout_path'], index))
                except Exception, e:
                    self.__env['stdout_tail'] = "(stdout tail not available)"
                    self.__env['stdout_path'] = ""
                    pUtil.tolog("no stdout_path: %s" % (e))

                # update the panda server
                ret, retNode = pUtil.updatePandaServer(j, stdout_tail = self.__env['stdout_tail'], stdout_path = self.__env['stdout_path'])
                if ret == 0:
                    pUtil.tolog("Successfully updated PanDA server at %s [PanDA ID %s]" % (pUtil.timeStamp(), j.jobId))
                else:
                    pUtil.tolog("!!WARNING!!1999!! updatePandaServer returned a %d" % (ret))
    
                # kill a job if signaled from panda server
                #if "tobekilled" in self.__env['jobDic'][k][1].action:
                #    pilotErrorDiag = "Pilot received a panda server signal to kill job %s at %s" %\
                #                     (self.__env['jobDic'][k][1].jobId, pUtil.timeStamp())
                #    pUtil.tolog("!!FAILED!!1999!! %s" % (pilotErrorDiag))
                #    if self.__env['jobrec']:
                #        self.__env['jobrec'] = False
                #        pUtil.tolog("Switching off job recovery")
                #    # kill the real job process(es)
                #    #pUtil.tolog("Going to kill pid %d" %lineno())
                #    killProcesses(self.__env['jobDic'][k][0], self.__env['jobDic'][k][1].pgrp)
                #    self.__env['jobDic'][k][1].result[0] = "failed"
                #    self.__env['jobDic'][k][1].currentState = self.__env['jobDic'][k][1].result[0]
                #    self.__env['jobDic'][k][1].result[2] = self.__error.ERR_PANDAKILL
                #    self.__env['jobDic'][k][1].pilotErrorDiag = pilotErrorDiag
    
                # did we receive a command to turn on debug mode?
                #if "debug" in self.__env['jobDic'][k][1].action.lower():
                #    pUtil.tolog("Pilot received a command to turn on debug mode from the server")
#                    self.__env['update_freq_server'] = 2*60
                #    self.__env['update_freq_server'] = 5*60
                #    pUtil.tolog("Server update frequency lowered to %d s" % (self.__env['update_freq_server']))
                #    self.__env['jobDic'][k][1].debug = "True"
    
                # did we receive a command to turn off debug mode?
                #if "debugoff" in self.__env['jobDic'][k][1].action.lower():
                #    pUtil.tolog("Pilot received a command to turn off debug mode from the server")
                #    self.__env['update_freq_server'] = 30*60
                #    pUtil.tolog("Server update frequency increased to %d s" % (self.__env['update_freq_server']))
                #    self.__env['jobDic'][k][1].debug = "False"
        t1_serverupdate = os.times()
        t_serverupdate = t1_serverupdate[4] - t0_serverupdate[4]
        pUtil.tolog("PanDA server update for [%s] jobs took: %s sec" % (len(self.__env['jobDic']['prod'][1]), t_serverupdate))
            
    def __loopingJobKiller(self):
        """ Look for looping job """
    
        pUtil.tolog("Checking for looping job")
        for j in self.__env['jobDic']['prod'][1]:
    
            # if current run state is "stageout", just make sure that the stage out copy command is not hanging
            if self.__env['stageout']:
                pUtil.tolog("Making sure that the stage out copy command is not hanging")
    
                if not self.__env['stageoutStartTime']:
                    pUtil.tolog("!!WARNING!!1700!! Stage-out start time not set")
                else:
                    # how much time has passed since stage-out began?
                    time_passed = int(time.time()) - self.__env['stageoutStartTime']
    
                    # find out the timeout limit for the relevant site mover
                    from SiteMoverFarm import getSiteMover
                    sitemover = getSiteMover(pUtil.readpar('copytool'), "")
                    timeout = sitemover.get_timeout()
    
                    pUtil.tolog("Stage-out with %s site mover began at %s (%d s ago, site mover time-out: %d s)" 
                                %(pUtil.readpar('copytool'), time.strftime("%H:%M:%S", time.gmtime(self.__env['stageoutStartTime'])), time_passed, timeout))
    
                    grace_time = 5*60
                    if time_passed > timeout:
                        pUtil.tolog("Adding a grace time of %d s in case copy command has been aborted but file removal is not complete" % (grace_time))
                    if time_passed > timeout + grace_time:
                        j = self.__killLoopingJob(j, self.__env['jobDic']['prod'][0], setStageout = True)
                        break
                    
            elif len(j.outFiles) > 0: # only check for looping job for jobs with output files
    
                # loop over all job output files and find the one with the latest modification time
                # note that some files might not have been created at this point (e.g. RDO built from HITS)
    
                # locate all files that were modified the last N minutes
                cmd = "find %s -mmin -%d" % (j.workdir, int(self.__env['loopingLimit']/60))
                pUtil.tolog("Executing command: %s" % (cmd))
                out = commands.getoutput(cmd)
                if out != "":
                    files = out.split("\n")
                    if len(files) > 0:
                        # remove unwanted list items (*.py, *.pyc, workdir, ...)
                        _files = []
                        for _file in files:
                            if not (j.workdir == _file or
                                    ".lib.tgz" in _file or
                                    ".py" in _file or
                                    "PoolFileCatalog" in _file or
                                    "setup.sh" in _file or
                                    "jobState" in _file or
                                    "pandaJob" in _file or
                                    "runjob" in _file or
                                    "matched_replicas" in _file or
                                    "DBRelease-" in _file):
                                _files.append(_file)
    #                        else:
    #                            pUtil.tolog("Ignored file: %s" % (_file))
                        if _files != []:
                            pUtil.tolog("Found %d files that were recently updated (e.g. file %s)" % (len(_files), _files[0]))
    #                        s = ""
    #                        for _file in _files:
    #                            s += _file + ", "
    #                        pUtil.tolog(s)
                            # get the current system time
                            self.__env['lastTimeFilesWereModified']['prod'] = int(time.time())
                        else:
                            pUtil.tolog("WARNING: found no recently updated files!")
                    else:
                        pUtil.tolog("WARNING: found no recently updated files")
                else:
                    pUtil.tolog("WARNING: Found no recently updated files")
    
                # check if the last modification time happened long ago
                # (process is considered to be looping if it's files have not been modified within loopingLimit time)
                pUtil.tolog("int(time.time())=%d"%int(time.time()))
                pUtil.tolog("lastTimeFilesWereModified=%d"%self.__env['lastTimeFilesWereModified']['prod'])
                pUtil.tolog("loopingLimit=%d"%self.__env['loopingLimit'])
                if (int(time.time()) - self.__env['lastTimeFilesWereModified']['prod']) > self.__env['loopingLimit']:
                    j = self.__killLoopingJob(j, self.__env['jobDic']['prod'][0])
                    break
                
    def __check_looping_jobs(self):             
        # every 30 minutes, look for looping jobs
        if (int(time.time()) - self.__env['curtime']) > self.__env['update_freq_server'] and not self.__skip: # 30 minutes
            # check when the workdir files were last updated or that the stageout command is not hanging
            #if self.__allowLoopingJobKiller():
            #    self.__loopingJobKiller()

            # make final server update for all ended jobs
            self.__updateJobs()

            # update the time for checking looping jobs
            self.__env['curtime'] = int(time.time())
                        
    def __set_outputs(self):
        # all output will be written to the pilot log as well as to stdout [with the pUtil.tolog() function]
        pUtil.setPilotlogFilename("%s/pilotlog.txt" % (self.__env['thisSite'].workdir))
    
        # redirect stderr
        pUtil.setPilotstderrFilename("%s/pilot.stderr" % (self.__env['thisSite'].workdir))
        sys.stderr = open(pUtil.getPilotstderrFilename(), 'w')
        
    def __verify_permissions(self):
        # verify permissions
        cmd = "stat %s" % (self.__env['thisSite'].workdir)
        pUtil.tolog("(1b) Executing command: %s" % (cmd))
        rc, rs = commands.getstatusoutput(cmd)
        pUtil.tolog("\n%s" % (rs))    
        
    def __getsetWNMem(self):
        """ Get the memory limit from queuedata or from the -k pilot option and set it """

        wn_mem = 0

        # Get the memory limit primarily from queuedata
        # Note: memory will soon be changed to maxmemory
        _maxmemory = pUtil.readpar('maxmemory')
        if _maxmemory == "":
            _maxmemory = pUtil.readpar('memory')

        if _maxmemory != "":
            try:
                maxmemory = int(_maxmemory) # Should already be an int
            except Exception, e:
                pUtil.tolog("Could not convert maxmemory to an int: %s" % (e))
                maxmemory = -1
            else:
                pUtil.tolog("Got max memory limit: %d MB (from queuedata)" % (maxmemory))
        else:
            maxmemory = -1

        # Get the max memory limit from the -k pilot option if specified
        if maxmemory == -1 and self.__env['memory']:
            try:
                maxmemory = int(self.__env['memory'])
            except Exception, e:
                pUtil.tolog("Could not convert memory to an int: %s" % (e))
                maxmemory = -1
            else:
                pUtil.tolog("Got max memory limit: %d MB (from pilot option -k)" % (maxmemory))

        # Set the memory limit
        if maxmemory > 0:
        
            # Convert MB to Bytes for the setrlimit function
            _maxmemory = maxmemory*1024**2

            max_memory = getMaxMemoryUsageFromCGroups()
            if max_memory:
                pUtil.tolog("cgroups max_memory = %s" % (max_memory))
            else:
                pUtil.tolog("cgroups max_memory not defined")

            # Only proceed if not a CGROUPS site
            if not isCGROUPSSite():
                pUtil.tolog("Not a CGROUPS site, proceeding with setting the memory limit")
                try:
                    import resource
                    resource.setrlimit(resource.RLIMIT_AS, [_maxmemory, _maxmemory])
                except Exception, e:
                    pUtil.tolog("!!WARNING!!3333!! resource.setrlimit failed: %s" % (e))
                else:
                    pUtil.tolog("Max memory limit set to: %d B" % (_maxmemory))
            else:
                pUtil.tolog("Detected a CGROUPS site, will not set the memory limit")

            cmd = "ulimit -a"
            pUtil.tolog("Executing command: %s" % (cmd))
            out = commands.getoutput(cmd)
            pUtil.tolog("\n%s" % (out))
        else:
            pUtil.tolog("Max memory will not be set")

        return maxmemory

    def __checkLocalDiskSpace(self, disk):
        """ Do we have enough local disk space left to run the job? """
    
        ec = 0
    
        # convert local space to B and compare with the space limit
        spaceleft = int(disk)*1024**2 # B (node.disk is in MB)
        _localspacelimit = self.__env['localspacelimit0'] * 1024 # B
        pUtil.tolog("Local space limit: %d B" % (_localspacelimit))
        if spaceleft < _localspacelimit:
            pUtil.tolog("!!FAILED!!1999!! Too little space left on local disk to run job: %d B (need > %d B)" % (spaceleft, _localspacelimit))
            ec = self.__error.ERR_NOLOCALSPACE
        else:
            pUtil.tolog("Remaining local disk space: %d B" % (spaceleft))
    
        return ec

    def __getLoopingLimit(self, maxCpuCount, jobPars, sitename):
        """ Get the looping time limit for the current job (in seconds) """
    
        # start with the default looping time limit, use maxCpuCount if necessary
        if "ANALY_" in sitename:
            loopingLimit = self.__env['loopingLimitDefaultUser']
        else:
            loopingLimit = self.__env['loopingLimitDefaultProd']
        if maxCpuCount >= self.__env['loopingLimitMinDefault']:
            _loopingLimit = max(self.__env['loopingLimitMinDefault'], maxCpuCount)
        else:
            _loopingLimit = max(loopingLimit, maxCpuCount)
        if _loopingLimit != loopingLimit:
            pUtil.tolog("Task request: Updated looping job limit from %d s to %d s using maxCpuCount" % \
                  (loopingLimit, _loopingLimit))
            loopingLimit = _loopingLimit
        else:
            # increase the looping job limit for multi-trf jobs
            if jobPars.find("\n") >= 0:
                pUtil.tolog("Multi-trf job encountered: will double the looping job limit")
                loopingLimit = 2*loopingLimit
            #pUtil.tolog("Using looping job limit: %d s" % (loopingLimit))
            #pUtil.tolog("maxCpuCount: %d s" % (maxCpuCount))
    
        return loopingLimit

    def __storePilotInitdir(self, targetdir, pilot_initdir):
        """ Store the pilot launch directory in a file used by environment.py """

        # This function is used to store the location of the init directory in the init directory itself as well as in the
        # site work directory. The location file is used by environment.py to set the global env['pilot_initdir'] used
        # by the pilot and the Monitor

        # This function must be called before the global env variable is instantiated in the pilot

        path = os.path.join(targetdir, "PILOT_INITDIR")
        pUtil.tolog("Creating file %s with content %s" % (path, pilot_initdir))
        pUtil.writeToFile(path, pilot_initdir)

#    def __copyPilotVersion(self):
#        """ Copy the PILOTVERSION file into the jobs' work directory """

    def __createJobWorkdir(self, job, stderr):
        """ Attempt to create the job workdir """
    
        ec, errorText = job.mkJobWorkdir(self.__env['thisSite'].workdir)
        if ec != 0:
            job.setState(["failed", 0, self.__error.ERR_MKDIRWORKDIR])
            ret, retNode = pUtil.updatePandaServer(job)
            if ret == 0:
                pUtil.tolog("Successfully updated panda server at %s" % pUtil.timeStamp())
            else:
                pUtil.tolog("!!WARNING!!1999!! updatePandaServer returned a %d" % (ret))
            # send to stderr
            print >> stderr, errorText
            # remove the site workdir before exiting
            pUtil.writeToFile(os.path.join(self.__env['thisSite'].workdir, "EXITCODE"), str(self.__error.ERR_GENERALERROR))

        else:
            pUtil.tolog("Created job workdir at %s" % (job.workdir))
            # copy the job def file into job workdir
            pUtil.tolog(os.getcwd())
            copy("%s/Job_%s.py" % (os.getcwd(), job.jobId), "%s/newJobDef.py" % job.workdir)

            self.__storePilotInitdir(self.__env['job'].workdir, self.__env['pilot_initdir'])
    
        return ec, job
    
    def __throttleJob(self):
        """ short delay requested by the server """
    
        # dictionary for throttling job startup (setting running state)
        throttleDic = {'CA':6, 'CERN':1.33, 'DE':6, 'ES':6, 'FR':6, 'IT':6, 'ND':0, 'NDGF':0, 'NL':6, 'TW':6, 'UK':6, 'US':6}
        if self.__env['nSent'] > 0:
            try:
                _M = throttleDic[pUtil.readpar('cloud')]
            except Exception, e:
                pUtil.tolog("Warning: %s (using default value 6 as multiplier for throttling)" % str(e))
                _M = 6
            _t = (self.__env['nSent'] + 1)*_M
            # protect for potential unreasonably high sleeping times
            max_sleep = 60
            if _t > max_sleep:
                _t = max_sleep
                pUtil.tolog("!!WARNING!!1111!! Throttle time out of bounds, reset to %d s (nSent = %d)" % (max_sleep, self.__env['nSent']))
            pUtil.tolog("Throttle sleep: %d s" % (_t))
            time.sleep(_t)

    def __backupJobDef(self):
        """ Backup job definition """
    
        # note: the log messages here only appears in pilotlog.txt and not in the batch log since they are
        # written by the forked child process
    
        if os.path.exists(self.__env['pandaJobDataFileName']):
            pUtil.tolog("Copying job definition (%s) to %s" % (self.__env['pandaJobDataFileName'], self.__env['jobDic']["prod"][1][0].workdir))
            try:
                copy2(self.__env['pandaJobDataFileName'], self.__env['jobDic']["prod"][1][0].workdir)
            except Exception, e:
                pUtil.tolog("!!WARNING!!1999!! Could not backup job definition: %s" % str(e))
            else:
                pandaJobDataFileName_i = self.__env['pandaJobDataFileName'].replace(".out", "_%d.out" % (self.__env['number_of_jobs']))
                _path = os.path.join(self.__env['pilot_initdir'], pandaJobDataFileName_i)
                pUtil.tolog("Copying job definition (%s) to %s" % (self.__env['pandaJobDataFileName'], _path))
                try:
                    copy2(self.__env['pandaJobDataFileName'], _path)
                except Exception, e:
                    pUtil.tolog("!!WARNING!!1999!! Could not backup job definition: %s" % str(e))
        else:
            pUtil.tolog("!!WARNING!!1999!! Could not backup job definition since file %s does not exist" % (self.__env['pandaJobDataFileName']))
    
    def __updateTerminatedJobs(self):
        """ For multiple jobs, pilot may took long time collect logs. We need to heartbeat for these jobs. """
        for j  in self.__env['jobDic']['prod'][1]:
            tmp = j.result[0]
            if tmp in ["finished", "failed", "holding"]:
                jobResult = j.result[0]
                try:
                    j.result[0] = 'transferring'
                    # update the panda server
                    ret, retNode = pUtil.updatePandaServer(j, stdout_tail = '', stdout_path = '')
                    if ret == 0:
                        pUtil.tolog("Successfully updated panda server for job %s at %s" % (j.jobId, pUtil.timeStamp()))
                    else:
                        pUtil.tolog("!!WARNING!!1999!! updatePandaServer returned a %d" % (ret))
                except:
                    pUtil.tolog("!!WARNING!!1999!! updatePandaServer failed: %s" % (traceback.format_exc()))
                finally:
                    j.result[0] = jobResult
                    pUtil.tolog('Internal state for job %s back to [%s]' % (j.jobId, j.result[0]))
                return True

    def __cleanUpEndedJobs(self):
        """ clean up the ended jobs (if there are any) """
    
        # after multitasking was removed from the pilot, there is actually only one job
        
        #perr = self.__env['jobDic']['prod'][1].result[2]
        #terr = self.__env['jobDic']['prod'][1].result[1]
        #allJobsDone = True
        
        update_time = self.__env['curtime']
        
        for j in self.__env['jobDic']['prod'][1][:]:  
            if j.result[0] in ["finished", "failed", "holding"]:
                pUtil.tolog("Clean up the ended job: %s (%s)" % (j.jobId, j.result[0]))

                # do not put the getStdoutDictionary() call outside the loop since cleanUpEndedJobs() is called every minute
                # only call getStdoutDictionary() once
            
                # get the stdout tails
                pUtil.tolog("Refreshing tail stdout dictinary prior to finishing the job %s" % j.jobId)
                stdout_dictionary = pUtil.getStdoutDictionary(self.__env['jobDic'])
            
                # refresh the stdout tail if necessary
                # get the tail if possible
                try:
                    self.__env['stdout_tail'] = stdout_dictionary[j.jobId]
                    index = "path-%s" % (j.jobId)
                    self.__env['stdout_path'] = stdout_dictionary[index]
                except:
                    self.__env['stdout_tail'] = "(stdout tail not available)"
                    self.__env['stdout_path'] = ""
    
                # cleanup the job workdir, save/send the job tarball to DDM, and update
                # panda server with the final job state
            
                pUtil.postJobTask(j, self.__env['thisSite'], self.__env['workerNode'], 
                                  self.__env['experiment'], jr = False, stdout_tail = self.__env['stdout_tail'], stdout_path = self.__env['stdout_path'])

                # for NG write the error code, if any
                #if pUtil.readpar('region') == "Nordugrid" and (perr != 0 or terr != 0):
                #    if perr != 0:
                #        ec = perr
                #    else:
                #        ec = terr
                #    pUtil.writeToFile(os.path.join(self.__env['thisSite'].workdir, "EXITCODE"), str(ec))

                # move this job from env['jobDic'] to zombieJobList for later collection
                self.__env['jobDic']['prod'][1].remove(j)
        
            # let pilot do heartbeat
            if (int(time.time()) - update_time) > self.__env['update_freq_server'] and not self.__skip:
                tolog("Send heartbeat for not transfered jobs")
                self.__updateTerminatedJobs()
                update_time = int(time.time())
                break
        
        if not self.__env['jobDic']['prod'][1]:
            
            self.__env['zombieJobList'].append(self.__env['jobDic']['prod'][0]) # only needs pid of this job for cleanup

            # athena processes can loop indefinately (e.g. pool utils), so kill all subprocesses just in case
            pUtil.tolog("Killing remaining subprocesses (if any)")
            
            for j in self.__env['jobDic']['prod'][1]:
                if j.result[2] == self.__error.ERR_OUTPUTFILETOOLARGE:
                    killOrphans()
                #pUtil.tolog("Going to kill pid %d" %lineno()) 
            
            killProcesses(self.__env['jobDic']['prod'][0], self.__env['jobDic']['prod'][2])
            #if self.__env['jobDic']['prod'][1].result[2] == self.__error.ERR_OUTPUTFILETOOLARGE:
            #    killOrphans()
            # remove the process id file to prevent cleanup from trying to kill the remaining processes another time
            # (should only be necessary for jobs killed by the batch system)
            if os.path.exists(os.path.join(self.__env['thisSite'].workdir, "PROCESSID")):
                try:
                    os.remove(os.path.join(self.__env['thisSite'].workdir, "PROCESSID"))
                except Exception, e:
                    pUtil.tolog("!!WARNING!!2999!! Could not remove process id file: %s" % str(e))
                else:
                    pUtil.tolog("Process id file removed")
                
            # ready with this object, delete it
            del self.__env['jobDic']['prod']
        
        
    def __spaceReport(self, job):
        
        if self.__env['loggingMode'] == None:
            ret, retNode = pUtil.updatePandaServer(job, spaceReport = True)
        else:
            ret, retNode = pUtil.updatePandaServer(job, spaceReport = False)
        if ret == 0:
            pUtil.tolog("Successfully updated panda server at %s" % pUtil.timeStamp())
            self.__env['isServerUpdated'] = True
        else:
            pUtil.tolog("!!WARNING!!1999!! updatePandaServer returned a %d" % (ret))
            self.__env['isServerUpdated'] = False
        
        return True
                
    def monitor_job(self):
        """ Main monitoring loop launched from the pilot module """

        try:
            # multi-job variables
            maxFailedMultiJobs = 3
            multiJobTimeDelays = range(2, maxFailedMultiJobs+2) # [2,3,4]
            shuffle(multiJobTimeDelays)
            number_of_failed_jobs = 0
            
            self.__set_outputs()            
            self.__verify_permissions()


            # PN
            #pUtil.tolog("Faking bad TCP server")
            #pUtil.tolog("!!WARNING!!1234!! Failed to open TCP connection to localhost (worker node network problem), cannot continue")
            #pUtil.fastCleanup(self.__env['thisSite'].workdir, self.__env['pilot_initdir'], self.__env['rmwkdir'])
            #self.__env['return'] = self.__error.ERR_NOTCPCONNECTION
            #return

            
            # start the monitor and watchdog process
            monthread = PilotTCPServer(UpdateHandler)
            if not monthread.port:
                pUtil.tolog("!!WARNING!!1234!! Failed to open TCP connection to localhost (worker node network problem), cannot continue")
                pUtil.fastCleanup(self.__env['thisSite'].workdir, self.__env['pilot_initdir'], self.__env['rmwkdir'])
                self.__env['return'] = self.__error.ERR_NOTCPCONNECTION
                return
            else:
                pUtil.tolog("Pilot TCP server will use port: %d" % (monthread.port))
            monthread.start()

            # dump some pilot info, version id, etc (to the log file this time)
            pUtil.tolog("\n\nEntered multi-job loop. Current work dir: %s\n" % (os.getcwd()))
            pUtil.dumpPilotInfo(self.__env['version'], self.__env['pilot_version_tag'], self.__env['pilotId'], 
                                self.__env['jobSchedulerId'], self.__env['pilot_initdir'], tofile = True)
            
            if self.__env['timefloor'] != 0:
                pUtil.tolog("Entering main pilot loop: multi job enabled (number of processed jobs: %d)" % (self.__env['number_of_jobs']))
                self.__env['hasMultiJob'] = True
            else:
                pUtil.tolog("Entering main pilot loop: multi job disabled")
                # do not reset hasMultiJob
        
            # local checks begin here..................................................................................
        
            # collect WN info again to avoid getting wrong disk info from gram dir which might differ from the payload workdir
            pUtil.tolog("Collecting WN info from: %s (again)" % (self.__env['thisSite'].workdir))
            self.__env['workerNode'].collectWNInfo(self.__env['thisSite'].workdir)
        
            # overwrite mem since this should come from either pilot argument or queuedata
            self.__env['workerNode'].mem = self.__getsetWNMem()

            # update the globals used in the exception handler
            globalSite = self.__env['thisSite']
            globalWorkNode = self.__env['workerNode']

            # get the experiment object
            thisExperiment = pUtil.getExperiment(self.__env['experiment'])

            # do we have a valid proxy?
            if self.__env['proxycheckFlag']:
                ec, pilotErrorDiag = thisExperiment.verifyProxy(envsetup="")
                if ec != 0:
                    self.__env['return'] = ec
                    return

            # do we have enough local disk space to run the job?
            ec = self.__checkLocalDiskSpace(self.__env['workerNode'].disk)
            if ec != 0:
                pUtil.tolog("Pilot was executed on host: %s" % (self.__env['workerNode'].nodename))
                pUtil.fastCleanup(self.__env['thisSite'].workdir, self.__env['pilot_initdir'], self.__env['rmwkdir'])
                self.__env['return'] = ec
                return

            # make sure the pilot TCP server is still running
            pUtil.tolog("Verifying that pilot TCP server is still alive...")
            if pUtil.isPilotTCPServerAlive('localhost', monthread.port):
                pUtil.tolog("...Pilot TCP server is still running")
            else:
                pUtil.tolog("!!WARNING!!1231!! Pilot TCP server is down - aborting pilot (payload cannot be started)")
                pUtil.fastCleanup(self.__env['thisSite'].workdir)
                self.__env['return'] = self.__error.ERR_NOPILOTTCPSERVER
                return

            # prod job start time counter
            #tp_0 = os.times()
        
            # reset stageout start time (used by looping job killer)
            self.__env['stageoutStartTime'] = None
            self.__env['stagein'] = False
            self.__env['stageout'] = False
                
            #tp_1 = os.times()
    
            # self.__env['job'].timeGetJob = int(round(tp_1[4] - tp_0[4])) ?
        
            # update the global used in the exception handler
            globalJob = self.__env['job']
        
            # update job id list
            for j in self.__env['job']:
                self.__env['jobIds'].append(j.jobId)
                j.jobState = "startup"
            
            # self.__env['job'].jobState = "startup"
            
            # does the application directory exist?
            '''
            ec, self.__env['thisSite'].appdir = self.__env['si'].extractAppdir(pUtil.readpar('appdir'),
                                                                               self.__env['job'].processingType, 
                                                                               self.__env['job'].homePackage)

#            ec, self.__env['thisSite'].appdir = self.__env['si'].extractAppdir(self.__env['thisSite'].appdir, 
#                                                                               self.__env['job'].processingType, 
#                                                                               self.__env['job'].homePackage)
            if ec != 0:
                self.__env['job'].result[0] = 'failed'
                self.__env['job'].currentState = self.__env['job'].result[0]
                self.__env['job'].result[2] = ec
                ret, retNode = pUtil.updatePandaServer(self.__env['job'])
                pUtil.fastCleanup(self.__env['thisSite'].workdir, self.__env['pilot_initdir'], self.__env['rmwkdir'])
                self.__env['return'] = ec
                return
            '''
            # update the job state file
            # JR = JobRecovery()
            
            # self.__env['job'].jobState = "startup"
            # _retjs = JR.updateJobStateTest(self.__env['job'], self.__env['thisSite'], self.__env['workerNode'], mode="test")
            # if not _retjs:
            #    pUtil.tolog("Could not update job state test file: %s" % str(_retjs))
        
            # getJob() ends here.....................................................................................
        
            # copy some supporting modules to the workdir for pilot job to run
            ec = pUtil.stageInPyModules(self.__env['pilot_initdir'], self.__env['thisSite'].workdir)
            if ec != 0:
                pUtil.tolog("Pilot cannot continue since not all modules could be copied to work directory")
                pUtil.fastCleanup(self.__env['thisSite'].workdir, self.__env['pilot_initdir'], self.__env['rmwkdir'])
                self.__env['return'] = ec
                return
        
            pUtil.tolog("Current time :%s" % (pUtil.timeStamp()))
            pUtil.tolog("The site this pilot runs on: %s" % (self.__env['thisSite'].sitename))
            pUtil.tolog("Pilot executing on host: %s" % (self.__env['workerNode'].nodename))
            pUtil.tolog("The workdir this pilot runs on:%s" % (self.__env['thisSite'].workdir))
            pUtil.tolog("The dq2url for this site is: %s" % (self.__env['thisSite'].dq2url))
            pUtil.tolog("The DQ2 SE has %d GB space left (NB: dCache is defaulted to 999999)" % (self.__env['thisSite'].dq2space))
            pUtil.tolog("New jobs has prodSourceLabels: [%s]" % (list(set([job.prodSourceLabel for job in self.__env['job']]))))
        
            # update the globals used in the exception handler
            globalSite = self.__env['thisSite']
            globalWorkNode = self.__env['workerNode']
        
            # does the looping job limit need to be updated?
            loopingLimits = []
            for j in self.__env['job']:
                #pUtil.tolog("JobID: %s env = %s" % (j.jobId, str(j.jobPars)))
                loopingLimits.append(self.__getLoopingLimit(j.maxCpuCount, j.jobPars, self.__env['thisSite'].sitename))

                # figure out and set payload file names
                j.setPayloadName(thisExperiment.getPayloadName(j))
            
            self.__env['loopingLimit'] = max(loopingLimits)
            
            #self.__env['loopingLimit'] = self.__getLoopingLimit(self.__env['job'].maxCpuCount, self.__env['job'].jobPars, self.__env['thisSite'].sitename)

            # get the experiment object
            thisExperiment = pUtil.getExperiment(self.__env['experiment'])

        
            # update the global used in the exception handler
            globalJob = self.__env['job']
        
            prodUsers = list(set([job.prodUserID for job in self.__env['job']]))
            if prodUsers:
                pUtil.tolog("Pilot executing job(s) for user(s): %s" % (prodUsers))
                
            #if self.__env['job'].prodUserID != "":
            #    pUtil.tolog("Pilot executing job for user: %s" % (self.__env['job'].prodUserID))
        
            # update job status and id 
            
            for j in self.__env['job']:
                j.result[0] = "starting"
                j.currentState = j.result[0]
                j.workdir = "%s/PandaJob_%s" % (self.__env['thisSite'].workdir, j.jobId)
                 
            # os.environ['PandaID'] = self.__env['job'].jobId

            # if desired, create the job setup script (used to recreate the job locally if needed)
            # note: this step only creates the file with the script header (bash info)
            #thisExperiment.updateJobSetupScript(self.__env['job'].workdir, create=True, to_script=None)
            thisExperiment.updateJobSetupScript(self.__env['thisSite'].workdir, create=True, to_script=None)

            # create the initial file state dictionary
            for j in self.__env['job']:
                createFileStates(self.__env['thisSite'].workdir, 
                                 j.jobId, 
                                 outFiles = j.outFiles, 
                                 logFile = j.logFile, 
                                 type="output")
                
                dumpFileStates(self.__env['thisSite'].workdir, 
                               j.jobId, 
                               type = "output")
                
                if j.inFiles != ['']:
                    
                    createFileStates(self.__env['thisSite'].workdir, 
                                     j.jobId, 
                                     inFiles = j.inFiles, 
                                     type="input")
                    
                    dumpFileStates(self.__env['thisSite'].workdir, 
                                   j.jobId, 
                                   type="input")
        
                    # are the output files within the allowed limit?
                    # (keep the LFN verification at this point since the wrkdir is now created, needed for creating the log in case of failure)
                    
                    ec, j.pilotErrorDiag = pUtil.verifyLFNLength(j.outFiles)
                    if ec != 0:
                        pUtil.tolog("Updating PanDA server for the failed job (error code %d)" % (ec))
                        j.result[0] = 'failed'
                        j.currentState = j.result[0]
                        j.result[2] = ec
                        pUtil.postJobTask(j, self.__env['thisSite'], 
                                          self.__env['workerNode'], self.__env['experiment'], 
                                          jr=False)
                        pUtil.fastCleanup(self.__env['thisSite'].workdir, self.__env['pilot_initdir'], self.__env['rmwkdir'])
                        self.__env['return'] = ec 
                        return
                    else:
                        pUtil.tolog("LFN length(s) verified, within allowed limit")
        
                        # print job info
                        #j.displayJob()
            
            self.__env['jobDic']["prod"] = [None, self.__env['job'], None] # pid, job, os.getpgrp()
        
            # send space report now, at the beginning of the job
            '''
            t_start = time.time()
            
            for j in self.__env['jobDic']["prod"][1]:
                self.__spaceReport(j)
            
            t_finish = time.time()
            update_space_time = t_finish - t_start
            pUtil.tolog("Space report update for %s jobs took: %s sec." % (len(self.__env['jobDic']["prod"][1]), update_space_time))
            '''
            # short delay requested by the server
            # self.__throttleJob()
        
            # maximum number of found processes
            self.__env['maxNProc'] = 0
        
            # fork into two processes, one for the pilot main control loop, and one for RunJob
            pid_1 = os.fork()
            if pid_1: # parent process
                # store the process id in case cleanup need to kill lingering processes
                pUtil.writeToFile(os.path.join(self.__env['thisSite'].workdir, "PROCESSID"), str(pid_1))
                self.__env['jobDic']["prod"][0] = pid_1
                self.__env['jobDic']["prod"][2] = os.getpgrp()

                t_start = time.time()
                for j in self.__env['jobDic']["prod"][1]:
                    j.result[0] = "starting"
                    pUtil.tolog("Parent process %s has set job state: %s" % (pid_1, j.result[0]))

                    # do not set self.__jobDic["prod"][1].currentState = "running" here (the state is at this point only needed for the server)
                    ret, retNode = pUtil.updatePandaServer(j)
                    if ret == 0:
                        pUtil.tolog("Successfully updated panda server at %s" % pUtil.timeStamp())
                    else:
                        pUtil.tolog("!!WARNING!!1999!! updatePandaServer returned a %d" % (ret))
                t_finish = time.time()
                update_space_time = t_finish - t_start
                pUtil.tolog("'Starting' state for %s jobs took: %s sec." % (len(self.__env['jobDic']["prod"][1]), update_space_time))

            else: # child job
                pUtil.tolog("Starting child process in dir: %s" % self.__env['thisSite'].workdir)

                # Decide which subprocess is to be launched (using info stored in the job object)
                subprocessName = thisExperiment.getSubprocessName(False)

                pUtil.tolog("About to launch child process: %s" % (subprocessName))
                # Get the arguments needed to launch the subprocess (list)
                jobargs = thisExperiment.getSubprocessArguments(self.__env, monthread.port, subprocessName=subprocessName)
                pUtil.tolog("jobargs=%s" % (jobargs))
                if not jobargs:
                    pUtil.tolog("!!WARNING!!1998!! Subprocess arguments are not known - cannot continue")
                else:
                    # copy all python files to workdir
                    #pUtil.stageInPyModules(self.__env['thisSite'].workdir, workdir)

                    # backup job definition
                    #self.__backupJobDef()

                    # start the RunJob* subprocess
                    # pUtil.chdir(workdir)
                    sys.path.insert(1,".")
                    os.execvpe(self.__env['pyexe'], jobargs, os.environ)

            # Control variables for looping jobs
            self.__env['lastTimeFilesWereModified'] = {}
            for k in self.__env['jobDic'].keys(): # loop over production and possible analysis job
                self.__env['lastTimeFilesWereModified'][k] = int(time.time())
        
            # main monitoring loop
            iteration = 1
            self.__env['curtime'] = int(time.time())
            self.__env['curtime_sp'] = self.__env['curtime']
            self.__env['curtime_of'] = self.__env['curtime']
            self.__env['curtime_proc'] = self.__env['curtime']
            #self.__env['curtime_mem'] = self.__env['curtime']
            self.__env['create_softlink'] = False # Will not work for MPI
            while True:

                #pUtil.tolog("--- Main pilot monitoring loop -----")
                #n = 0
                #for j in self.__env['job']:
                #    pUtil.tolog("(job id %s, state:%s (%s), iteration %d)" 
                #                   % (j.jobId, j.currentState, self.__env['jobDic']["prod"][1][n].result[0], iteration))
                #    n += 1
                self.__check_remaining_space()        
                self.create_softlink()
                self.__monitor_processes()
                self.__verify_output_sizes()              
                #self.__verify_memory_limits()
                self.__check_looping_jobs()

                # check if any jobs are done by scanning the process list
                # some jobs might have sent updates to the monitor thread about their final states at other times
                # Disabled for multijob
                self.__wdog.pollChildren()
        
                # clean up the ended jobs (if there are any)
                self.__cleanUpEndedJobs()
        
                # collect all the zombie processes
                self.__wdog.collectZombieJob(tn=10)
        
                # is there still a job in the self.__?
                if len(self.__env['jobDic']) == 0: # no child jobs in self.__
                    pUtil.tolog("The job has finished")
                    break
                else:
                    iteration += 1

                # rest a minute before next iteration
                time.sleep(60)
        
            # do not bother with saving the log file if it has already been transferred and registered
            try:
                for j in self.__env['job']:
                    state = getFileState(j.logFile, self.__env['thisSite'].workdir, j.jobId, type="output")
                    pUtil.tolog("Current log file state: %s" % str(state))
                    if os.path.exists(os.path.join(self.__env['thisSite'].workdir, j.logFile)) and state[0] == "transferred" and state[1] == "registered":
                        pUtil.tolog("Safe to remove the log file")
                        ec = pUtil.removeFiles(self.__env['thisSite'].workdir, [j.logFile])
                    else:
                        pUtil.tolog("Will not remove log file at this point (possibly already removed)")
            except Exception, e:
                pUtil.tolog("!!WARNING!!1111!! %s" % (e))
        
            pUtil.tolog("--------------------------------------------------------------------------------")
            pUtil.tolog("Number of processed jobs              : %d" % (self.__env['number_of_jobs']))
            pUtil.tolog("Maximum number of monitored processes : %d" % (self.__env['maxNProc']))
            pUtil.tolog("Pilot executed last job in directory  : %s" % (self.__env['thisSite'].workdir))
            pUtil.tolog("Current time                          : %s" % (pUtil.timeStamp()))
            pUtil.tolog("--------------------------------------------------------------------------------")                
    
            # a bit more cleanup
            self.__wdog.collectZombieJob()
    
            # call the cleanup function (only needed for multi-jobs)
            if self.__env['hasMultiJob']:
                pUtil.cleanup(self.__wdog, self.__env['pilot_initdir'], True, self.__env['rmwkdir'])
    
            # is there still time to run another job?
            # if comment this part, pilot will work continusely, while joobs will be in queue! 
            
            if os.path.exists(os.path.join(globalSite.workdir, "KILLED")):
                pUtil.tolog("Aborting multi-job loop since a KILLED file was found")
                self.__env['return'] = 'break'
                print 'break'
                return

            elif self.__env['timefloor'] == 0:
                pUtil.tolog("No time floor set, no time to run another job")
                self.__env['return'] = 'break'
                print 'break'
                return
            
            else:
                time_since_multijob_startup = int(time.time()) - self.__env['multijob_startup']
                pUtil.tolog("Time since multi-job startup: %d s" % (time_since_multijob_startup))
                if self.__env['timefloor'] > time_since_multijob_startup:
                    # do not run too many failed multi-jobs, abort if necessary
                    if self.__env['job'].result[2] != 0:
                        number_of_failed_jobs += 1
                    if number_of_failed_jobs >= maxFailedMultiJobs:
                        pUtil.tolog("Passed max number of failed multi-jobs (%d), aborting multi-job mode" % (maxFailedMultiJobs))
                        self.__env['return'] = 'break' 
                        print 'break' #TODO: this is not in a loop. We shoould probably do a "return ERROR" or similar
                        return
    
                    pUtil.tolog("Since time floor is set to %d s, there is time to run another job" % (self.__env['timefloor']))
    
                    # need to re-download the queuedata since the previous job might have modified it
                    ec, self.__env['thisSite'], self.__env['jobrec'], self.__env['hasQueuedata'] = pUtil.handleQueuedata(self.__env['queuename'], 
                        self.__env['schedconfigURL'], self.__error, self.__env['thisSite'], self.__env['jobrec'], self.__env['experiment'], forceDownload = True, 
                        forceDevpilot = self.__env['force_devpilot'])
                    if ec != 0:
                        self.__env['return'] = 'break'
                        print 'break' #TODO: this is not in a loop. We shoould probably do a "return ERROR" or similar
                        return
    
                    # do not continue immediately if the previous job failed due to an SE problem
                    if self.__env['job'].result[2] != 0:
                        _delay = 60 * multiJobTimeDelays[number_of_failed_jobs - 1]
                        if self.__error.isGetErrorCode(self.__env['job'].result[2]):
                            pUtil.tolog("Taking a nap for %d s since the previous job failed during stage-in" % (_delay))
                            time.sleep(_delay)
                        elif self.__error.isPutErrorCode(self.__env['job'].result[2]):
                            pUtil.tolog("Taking a nap for %d s since the previous job failed during stage-out" % (_delay))
                            time.sleep(_delay)
                        else:
                            pUtil.tolog("Will not take a nap since previous job failed with a non stage-in/out error")
    
                    self.__env['return'] = 'continue'
                    print 'continue' #TODO: this is not in a loop. We shoould probably do a "return ERROR" or similar
                    return
                else:
                    pUtil.tolog("Since time floor is set to %d s, there is no time to run another job" % (self.__env['timefloor']))
                    self.__env['return'] = 'break'
                    print 'break' #TODO: this is not in a loop. We shoould probably do a "return ERROR" or similar
                    return 
            
            # flush buffers
            sys.stdout.flush()
            sys.stderr.flush()
    
            # multi-job loop ends here ..............................................................................
            
        # catch any uncaught pilot exceptions
        except Exception, errorMsg:
    
            error = PilotErrors()
            # can globalJob be added here?
    
            if len(str(errorMsg)) == 0:
                errorMsg = "(empty error string)"
                                                                    
            if 'format_exc' in traceback.__all__:
                pilotErrorDiag = "Exception caught: %s, %s" % (str(errorMsg), traceback.format_exc())
            else:
                pUtil.tolog("traceback.format_exc() not available in this python version")
                pilotErrorDiag = "Exception caught: %s" % (str(errorMsg))
            pUtil.tolog("!!FAILED!!1999!! %s" % (pilotErrorDiag))
    
            if self.__env['isJobDownloaded']:
                if self.__env['isServerUpdated']:
                    pUtil.tolog("Do a full cleanup since job was downloaded and server updated")
    
                    # was the process id added to env['jobDic']?
                    bPID = False
                    try:
                        for k in self.__env['jobDic'].keys():
                            pUtil.tolog("Found process id in env['jobDic']: %d" % (self.__env['jobDic'][k][0]))
                    except:
                        pUtil.tolog("Process id not added to env['jobDic']")
                    else:
                        bPID = True
    
                    if bPID:
                        pUtil.tolog("Cleanup using env['jobDic']")
                        for j in self.__env['jobDic']['prod'][1]:
                            j.result[0] = "failed"
                            j.currentState = j.result[0]
                            if j.result[2] == 0:
                                j.result[2] = error.ERR_PILOTEXC
                            if j.pilotErrorDiag == "":
                                j.pilotErrorDiag = pilotErrorDiag
                        #--- only for test with params from first job --#
                            if globalSite:
                                pUtil.postJobTask(j, globalSite, globalWorkNode, 
                                                  self.__env['experiment'], jr=False)
                                self.__env['logTransferred'] = True
                        
                        pUtil.tolog("Killing process: %d from line %d" % (self.__env['jobDic']['prod'][0], lineno()))
                        killProcesses(self.__env['jobDic']['prod'][0], self.__env['jobDic']['prod'][2])
                        # move this job from env['jobDic'] to zombieJobList for later collection
                        self.__env['zombieJobList'].append(self.__env['jobDic'][k][0]) # only needs pid of this job for cleanup
                        del self.__env['jobDic']['prod']
    
                        # collect all the zombie processes
                        self.__wdog.collectZombieJob(tn=10)
                    else:
                        pUtil.tolog("Cleanup using globalJob")
                        for j in globalJob: 
                            j.result[0] = "failed"
                            j.currentState = j.result[0]
                            j.result[2] = error.ERR_PILOTEXC
                            j.pilotErrorDiag = pilotErrorDiag
                            if globalSite:
                                pUtil.postJobTask(j, globalSite, globalWorkNode, self.__env['experiment'], jr=False)
                else:
                    if globalSite:
                        pUtil.tolog("Do a fast cleanup since server was not updated after job was downloaded (no log)")
                        pUtil.fastCleanup(globalSite.workdir, self.__env['pilot_initdir'], self.__env['rmwkdir'])
            else:
                if globalSite:
                    pUtil.tolog("Do a fast cleanup since job was not downloaded (no log)")
                    pUtil.fastCleanup(globalSite.workdir, self.__env['pilot_initdir'], self.__env['rmwkdir'])
            self.__env['return'] = error.ERR_PILOTEXC 
            return
    
        # end of the pilot
        else:
            self.__env['return'] = 0
            return
