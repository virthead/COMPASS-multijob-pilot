import os
import time
import pUtil
from PilotErrors import PilotErrors
from Configuration import Configuration

class WatchDog:
    
    def __init__(self):
        self.__env = Configuration()
    
    def pollChildren(self):
        """
        check children processes, collect zombie jobs, and update jobDic status
        """
        
        pUtil.tolog("Watchdog to check children processes")
        error = PilotErrors()

        try:
            _id, rc = os.waitpid(self.__env['jobDic']['prod'][0], os.WNOHANG)
        except OSError, e:
            pUtil.tolog("Harmless exception when checking child process %s, %s" % (self.__env['jobDic']['prod'][0], e))
            if str(e).rstrip() == "[Errno 10] No child processes":
                pilotErrorDiag = "Exception caught by pilot watchdog: %s" % str(e)
                for j in self.__env['jobDic']['prod'][1]:
                    pUtil.tolog("Watchdog. JobID: %s, status [%s]" % (j.jobId, j.result[0])) 
                    if j.result[0] in ["finished", "failed", "holding", "transferring"]:
                        pUtil.tolog("Job: %s already %s" % (j.jobId, j.result[0]))
                    else:    
                        pUtil.tolog("!!FAILED!!1000!! Pilot setting state to failed since there are no child processes")
                        pUtil.tolog("!!FAILED!!1000!! %s" % (pilotErrorDiag))
                        pUtil.tolog("Watchdog will fail JobID: %s  status: [%s]" % (j.jobId, j.result[0]))
                        j.result[0] = "failed"
                        j.currentState = j.result[0]
                        if j.result[2] == 0:
                            j.result[2] = error.ERR_NOCHILDPROCESSES
                            if j.pilotErrorDiag == "":
                                j.pilotErrorDiag = pilotErrorDiag
            else:
                pass
        else:
            if _id: # finished
                rc = rc%255 # exit code
                self.__prodJobDone = True
                pUtil.tolog("Production job(s) is done")
                for j in self.__env['jobDic']['prod'][1]:
                    if j.result[0] != "finished" and j.result[0] != "failed" and j.result[0] != "holding":
                        if not rc: # rc=0, ok job
                            if not j.result[1]:
                                j.result[1] = rc # transExitCode (because pilotExitCode is reported back by child job)
                        else: # rc != 0, failed job
                            j.result[1] = rc # transExitCode

    def collectZombieJob(self, tn=None):
        """
        collect zombie child processes, tn is the max number of loops, plus 1,
        to avoid infinite looping even if some child proceses really get wedged;
        tn=None means it will keep going till all children zombies collected.        
        """
        time.sleep(1)
        if self.__env['zombieJobList'] and tn > 1:
            pUtil.tolog("--- collectZombieJob: --- %d, %s" % (tn, str(self.__env['zombieJobList'])))
            tn -= 1
            for x in self.__env['zombieJobList']:
                try:
                    pUtil.tolog("Zombie collector trying to kill pid %s" % str(x))
                    _id, rc = os.waitpid(x, os.WNOHANG)
                except OSError,e:
                    pUtil.tolog("Harmless exception when collecting zombie jobs, %s" % str(e))
                    self.__env['zombieJobList'].remove(x)
                else:
                    if _id: # finished
                        self.__env['zombieJobList'].remove(x)
                self.collectZombieJob(tn=tn) # recursion

        if self.__env['zombieJobList'] and not tn: # for the infinite waiting case, we have to
            # use blocked waiting, otherwise it throws
            # RuntimeError: maximum recursion depth exceeded
            for x in self.__env['zombieJobList']:
                try:
                    _id, rc = os.waitpid(x, 0)
                except OSError,e:
                    pUtil.tolog("Harmless exception when collecting zombie jobs, %s" % str(e))
                    self.__env['zombieJobList'].remove(x)
                else:
                    if _id: # finished
                        self.__env['zombieJobList'].remove(x)
                self.collectZombieJob(tn=tn) # recursion

