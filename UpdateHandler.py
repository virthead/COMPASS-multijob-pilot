import os, sys
import time
import pUtil
from SocketServer import BaseRequestHandler 
from Configuration import Configuration

class UpdateHandler(BaseRequestHandler):
    """ update self.__env['jobDic'] status with the messages sent from child via socket, do nothing else """
    
    def __init__(self, request, client_address, server):
        self.__env = Configuration()
        BaseRequestHandler.__init__(self, request, client_address, server)

    def handle(self):
        try:
            pUtil.tolog("Connected from %s" % str(self.client_address))
            data = self.request.recv(4096)
            jobmsg = data.split(";")
            jobinfo = {}
            for i in jobmsg:
                if not i:
                    continue # skip empty line
                try:
                    jobinfo[i.split("=")[0]] = i.split("=")[1]
                except Exception, e:
                    pUtil.tolog("!!WARNING!!1999!! Exception caught: %s" % (e))

            for j in self.__env['jobDic']['prod'][1]:
                if j.jobId == jobinfo["jobid"]: # job id matches
#                if self.__env['jobDic'][k][2] == int(jobinfo["pgrp"]) and self.__env['jobDic'][k][1].jobId == int(jobinfo["jobid"]): # job pid matches
                    # protect with try statement in case the pilot server goes down (jobinfo will be corrupted)
                    try:
                        j.currentState = jobinfo["status"]
                        if jobinfo["status"] == "stagein":
                            self.__env['stagein'] = True
                            self.__env['stageout'] = False
                            j.result[0] = "starting"
                        elif jobinfo["status"] == "stageout":
                            self.__env['stagein'] = False
                            self.__env['stageout'] = True
                            j.result[0] = "transferring"
                            self.__env['stageoutStartTime'] = int(time.time())
                        else:
                            self.__env['stagein'] = False
                            self.__env['stageout'] = False
                            j.result[0] = jobinfo["status"]
                        j.result[1] = int(jobinfo["transecode"]) # transExitCode
                        j.result[2] = int(jobinfo["pilotecode"]) # pilotExitCode
                        j.timeStageIn = jobinfo["timeStageIn"]
                        j.timeStageOut = jobinfo["timeStageOut"]
                        j.timeSetup = jobinfo["timeSetup"]
                        j.timeExe = jobinfo["timeExe"]
                        j.cpuConsumptionTime = jobinfo["cpuTime"]
                        j.cpuConsumptionUnit = jobinfo["cpuUnit"]
                        j.cpuConversionFactor = jobinfo["cpuConversionFactor"]
                        j.jobState = jobinfo["jobState"]
                        j.vmPeakMax = int(jobinfo["vmPeakMax"])
                        j.vmPeakMean = int(jobinfo["vmPeakMean"])
                        j.RSSMean = int(jobinfo["RSSMean"])
                        j.JEM = jobinfo["JEM"]
                        j.cmtconfig = jobinfo["cmtconfig"]

                        try:
                            j.pgrp = int(jobinfo["pgrp"])
                        except:
                            pUtil.tolog("!!WARNING!!2222!! Failed to convert pgrp value to int: %s" % (e))
                        else:
                            pUtil.tolog("Process groups: %d (pilot), %d (sub process)" % (os.getpgrp(), j.pgrp))

                        tmp = j.result[0]
                        if (tmp == "failed" or tmp == "holding" or tmp == "finished") and jobinfo.has_key("logfile"):
                            j.logMsgFiles.append(jobinfo["logfile"])

                        if jobinfo.has_key('startTime'):
                            j.startTime = jobinfo["startTime"]

                        if jobinfo.has_key('endTime'):
                            j.endTime = jobinfo["endTime"]

                        if jobinfo.has_key("pilotErrorDiag"):
                            j.pilotErrorDiag = pUtil.decode_string(jobinfo["pilotErrorDiag"])

                        if jobinfo.has_key("exeErrorDiag"):
                            j.exeErrorDiag = pUtil.decode_string(jobinfo["exeErrorDiag"])

                        if jobinfo.has_key("exeErrorCode"):
                            j.exeErrorCode = int(jobinfo["exeErrorCode"])

                        if jobinfo.has_key("filesWithFAX"):
                            j.filesWithFAX = int(jobinfo["filesWithFAX"])
    
                        if jobinfo.has_key("filesWithoutFAX"):
                            j.filesWithoutFAX = int(jobinfo["filesWithoutFAX"])

                        if jobinfo.has_key("bytesWithFAX"):
                            j.bytesWithFAX = int(jobinfo["bytesWithFAX"])

                        if jobinfo.has_key("bytesWithoutFAX"):
                            j.bytesWithoutFAX = int(jobinfo["bytesWithoutFAX"])

                        if jobinfo.has_key("filesAltStageOut"):
                            j.filesAltStageOut = int(jobinfo["filesAltStageOut"])

                        if jobinfo.has_key("filesNormalStageOut"):
                            j.filesNormalStageOut = int(jobinfo["filesNormalStageOut"])

                        if jobinfo.has_key("nEvents"):
                            try:
                                j.nEvents = int(jobinfo["nEvents"])
                            except Exception, e:
                                pUtil.tolog("!!WARNING!!2999!! jobinfo did not return an int as expected: %s" % str(e))
                                j.nEvents = 0
                        if jobinfo.has_key("nEventsW"):
                            try:
                                j.nEventsW = int(jobinfo["nEventsW"])
                            except Exception, e:
                                pUtil.tolog("!!WARNING!!2999!! jobinfo did not return an int as expected: %s" % str(e))
                                j.nEventsW = 0
    
                        if jobinfo.has_key("finalstate"):
                            j.finalstate = jobinfo["finalstate"]
                        if jobinfo.has_key("spsetup"):
                            j.spsetup = jobinfo["spsetup"]
                            # restore the = and ;-signs
                            j.spsetup = j.spsetup.replace("^", ";").replace("!", "=")
                            pUtil.tolog("Handler received special setup command: %s" % (j.spsetup))

                        if jobinfo.has_key("output_latereg"):
                            pUtil.tolog("Got output_latereg=%s" % (jobinfo["output_latereg"]))
                            j.output_latereg = jobinfo["output_latereg"]
    
                        if jobinfo.has_key("output_fields"):
                            j.output_fields = pUtil.stringToFields(jobinfo["output_fields"])
                            pUtil.tolog("Got output_fields=%s" % str(j.output_fields))
                            pUtil.tolog("Converted from output_fields=%s" % str(jobinfo["output_fields"]))

                        # hpc status
                        if jobinfo.has_key("mode"):
                            j.mode = jobinfo['mode']
                        if jobinfo.has_key("hpcStatus"):
                            j.hpcStatus = jobinfo['hpcStatus']
                        if jobinfo.has_key("refreshNow"):
                            j.refreshNow = jobinfo['refreshNow']
                        if jobinfo.has_key("coreCount"):
                            j.coreCount = jobinfo['coreCount']
                        if jobinfo.has_key('HPCJobId'):
                            j.HPCJobId = jobinfo['HPCJobId']

                    except Exception, e:
                        pUtil.tolog("!!WARNING!!1998!! 1 Caught exception. Pilot server down? %s" % str(e))
                        try:
                            pUtil.tolog("Received jobinfo: %s" % str(jobinfo))
                        except:
                            pass

        except Exception, e:
            pUtil.tolog("!!WARNING!!1998!! O Caught exception. Pilot server down? %s" % str(e))
            
        self.request.send("OK")
