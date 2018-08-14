# Class definition:
#   RunJobTitan
#   [Add description here]
#   Instances are generated with RunJobFactory via pUtil::getRunJob()
#   Implemented as a singleton class
#   http://stackoverflow.com/questions/42558/python-and-the-singleton-pattern

# Import relevant python/pilot modules

from RunJobHPC import RunJobHPC                  # Parent RunJob class
import os, sys, commands, time
import traceback
import atexit, signal
import saga

# Pilot modules
import Mover as mover
import Site, pUtil, Job, Node, RunJobUtilities
from pUtil import tolog, isAnalysisJob, readpar, createLockFile,  getChecksumCommand,\
     tailPilotErrorDiag, getFileAccessInfo, getCmtconfig, getExperiment 
from FileStateClient import updateFileStates, dumpFileStates
from ErrorDiagnosis import ErrorDiagnosis # import here to avoid issues seen at BU with missing module
from PilotErrors import PilotErrors
from datetime import datetime
from shutil import copy2
from glob import glob
from time import mktime, gmtime, strftime


class RunJobTitan(RunJobHPC):

    # private data members
    __runjob = "RunJobTitan"                    # String defining the sub class
    __instance = None                           # Boolean used by subclasses to become a Singleton
    #__error = PilotErrors()                    # PilotErrors object

    # Required methods

    def __init__(self):
        """ Default initialization """

        # e.g. self.__errorLabel = errorLabel
        pass

    def __new__(cls, *args, **kwargs):
        """ Override the __new__ method to make the class a singleton """

        if not cls.__instance:
            cls.__instance = super(RunJobHPC, cls).__new__(cls, *args, **kwargs)

        return cls.__instance

    def getRunJob(self):
        """ Return a string with the execution class name """

        return self.__runjob

    def getRunJobFileName(self):
        """ Return the filename of the module """

        return super(RunJobTitan, self).getRunJobFileName()

    # def argumentParser(self):  <-- see example in RunJob.py

    def allowLoopingJobKiller(self):
        """ Should the pilot search for looping jobs? """

        # The pilot has the ability to monitor the payload work directory. If there are no updated files within a certain
        # time limit, the pilot will consider the as stuck (looping) and will kill it. The looping time limits are set
        # in environment.py (see e.g. loopingLimitDefaultProd)

        return False
    
    def createFileMetadata(self, outFiles, job, outsDict, dsname, datasetDict, sitename, analysisJob=False):
        """ create the metadata for the output + log files """
        
        ec = 0

        # get/assign guids to the output files
        if outFiles:
            if not pUtil.isBuildJob(outFiles):
                ec, job.pilotErrorDiag, job.outFilesGuids = RunJobUtilities.getOutFilesGuids(job.outFiles, job.workdir, job.experiment)
                if ec:
                    tolog("missing PoolFileCatalog (only error code from getOutFilesGuids)")
                    #return ec, job, None
            else:
                tolog("Build job - do not use PoolFileCatalog to get guid (generated)")
        else:
            tolog("This job has no output files")

        # get the file sizes and checksums for the local output files
        # WARNING: any errors are lost if occur in getOutputFileInfo()
        for i  in range(len(outFiles)):
            outFiles[i] = job.workdir + "/" + outFiles[i]
        
        ec, pilotErrorDiag, fsize, checksum = pUtil.getOutputFileInfo(list(outFiles), getChecksumCommand(), skiplog=True, logFile=job.logFile)
        if ec != 0:
            tolog("!!FAILED!!2999!! %s" % (pilotErrorDiag))
            self.failJob(job.result[1], ec, job, pilotErrorDiag=pilotErrorDiag)

        #if self.__logguid:
        #    guid = self.__logguid
        #else:
        guid = job.tarFileGuid

        # create preliminary metadata (no metadata yet about log file - added later in pilot.py)
        _fname = "%s/metadata-%s.xml" % (job.workdir, job.jobId)
        try:
            _status = pUtil.PFCxml(job.experiment, _fname, list(job.outFiles), fguids=job.outFilesGuids, fntag="lfn", alog=job.logFile, alogguid=guid,\
                                   fsize=fsize, checksum=checksum, analJob=analysisJob)
        except Exception, e:
            pilotErrorDiag = "PFCxml failed due to problematic XML: %s" % (e)
            tolog("!!WARNING!!1113!! %s" % (pilotErrorDiag)) 
            self.failJob(job.result[1], error.ERR_MISSINGGUID, job, pilotErrorDiag=pilotErrorDiag)
        else:
            if not _status:
                pilotErrorDiag = "Missing guid(s) for output file(s) in metadata"
                tolog("!!FAILED!!2999!! %s" % (pilotErrorDiag))
                self.failJob(job.result[1], error.ERR_MISSINGGUID, job, pilotErrorDiag=pilotErrorDiag)

        tolog("..............................................................................................................")
        tolog("Created %s with:" % (_fname))
        tolog(".. log            : %s (to be transferred)" % (job.logFile))
        tolog(".. log guid       : %s" % (guid))
        tolog(".. out files      : %s" % str(job.outFiles))
        tolog(".. out file guids : %s" % str(job.outFilesGuids))
        tolog(".. fsize          : %s" % str(fsize))
        tolog(".. checksum       : %s" % str(checksum))
        tolog("..............................................................................................................")

        # convert the preliminary metadata-<jobId>.xml file to OutputFiles-<jobId>.xml for NG and for CERNVM
        # note: for CERNVM this is only really needed when CoPilot is used
        #region = readpar("region")
        #if region == 'Nordugrid' or sitename == 'CERNVM':
        #    if RunJobUtilities.convertMetadata4NG(os.path.join(job.workdir, job.outputFilesXML), _fname, outsDict, dsname, datasetDict):
        #        tolog("Metadata has been converted to NG/CERNVM format")
        #    else:
        #        job.pilotErrorDiag = "Could not convert metadata to NG/CERNVM format"
        #        tolog("!!WARNING!!1999!! %s" % (job.pilotErrorDiag))

        # try to build a file size and checksum dictionary for the output files
        # outputFileInfo: {'a.dat': (fsize, checksum), ...}
        # e.g.: file size for file a.dat: outputFileInfo['a.dat'][0]
        # checksum for file a.dat: outputFileInfo['a.dat'][1]
        try:
            # remove the log entries
            _fsize = fsize[1:]
            _checksum = checksum[1:]
            outputFileInfo = dict(zip(job.outFiles, zip(_fsize, _checksum)))
        except Exception, e:
            tolog("!!WARNING!!2993!! Could not create output file info dictionary: %s" % str(e))
            outputFileInfo = {}
        else:
            tolog("Output file info dictionary created: %s" % str(outputFileInfo))

        return ec, job, outputFileInfo
    
    def setup(self, job, jobSite, thisExperiment, _checkCMTCONFIG):
        """ prepare the setup and get the run command list """

        # start setup time counter
        t0 = time.time()
        ec = 0
        runCommandList = []

        # split up the job parameters to be able to loop over the tasks
        jobParameterList = job.jobPars.split("\n")
        jobHomePackageList = job.homePackage.split("\n")
        jobTrfList = job.trf.split("\n")
        job.release = thisExperiment.formatReleaseString(job.release)
        releaseList = thisExperiment.getRelease(job.release)

        tolog("Number of transformations to process: %s" % len(jobParameterList))
        if len(jobParameterList) > 1:
            multi_trf = True
        else:
            multi_trf = False

        # verify that the multi-trf job is setup properly
        ec, job.pilotErrorDiag, releaseList = RunJobUtilities.verifyMultiTrf(jobParameterList, jobHomePackageList, jobTrfList, releaseList)
        if ec > 0:
            return ec, runCommandList, job, multi_trf

        os.chdir(jobSite.workdir)
        tolog("Current job workdir is %s" % os.getcwd())

        # setup the trf(s)
        _i = 0
        _stdout = job.stdout
        _stderr = job.stderr
        _first = True
        for (_jobPars, _homepackage, _trf, _swRelease) in map(None, jobParameterList, jobHomePackageList, jobTrfList, releaseList):
            tolog("Preparing setup %d/%d" % (_i + 1, len(jobParameterList)))

            # reset variables
            job.jobPars = _jobPars
            job.homePackage = _homepackage
            job.trf = _trf
            job.release = _swRelease
            if multi_trf:
                job.stdout = _stdout.replace(".txt", "_%d.txt" % (_i + 1))
                job.stderr = _stderr.replace(".txt", "_%d.txt" % (_i + 1))

            # post process copysetup variable in case of directIn/useFileStager
            _copysetup = readpar('copysetup')
            _copysetupin = readpar('copysetupin')
            if "--directIn" in job.jobPars or "--useFileStager" in job.jobPars or _copysetup.count('^') == 5 or _copysetupin.count('^') == 5:
                # only need to update the queuedata file once
                if _first:
                    RunJobUtilities.updateCopysetups(job.jobPars)
                    _first = False

            # setup the trf
            ec, job.pilotErrorDiag, cmd, job.spsetup, job.JEM, job.cmtconfig = thisExperiment.getJobExecutionCommand(job, jobSite, self.getPilotInitDir(), _checkCMTCONFIG)
            if ec > 0:
                # setup failed
                break

            # add the setup command to the command list
            runCommandList.append(cmd)
            _i += 1

        job.stdout = _stdout
        job.stderr = _stderr
        job.timeSetup = int(time.time() - t0)
        tolog("Total setup time: %d s" % (job.timeSetup))

        return ec, runCommandList, job, multi_trf
    

    def stageIn(self, job, jobSite, analysisJob, pfc_name="PoolFileCatalog.xml"):
        """ Perform the stage-in """

        ec = 0
        statusPFCTurl = None
        usedFAXandDirectIO = False

        # Prepare the input files (remove non-valid names) if there are any
        ins, job.filesizeIn, job.checksumIn = RunJobUtilities.prepareInFiles(job.inFiles, job.filesizeIn, job.checksumIn)
        if ins:
            tolog("Preparing for get command")

            # Get the file access info (only useCT is needed here)
            useCT, oldPrefix, newPrefix = getFileAccessInfo()

            # Transfer input files
            tin_0 = os.times()
            ec, job.pilotErrorDiag, statusPFCTurl, FAX_dictionary = \
                mover.get_data(job, jobSite, ins, self.getStageInRetry(), analysisJob=analysisJob, usect=useCT,\
                               pinitdir=self.getPilotInitDir(), proxycheck=False, inputDir=self.getInputDir(), workDir=self.getParentWorkDir(), pfc_name=pfc_name)
            if ec != 0:
                job.result[2] = ec
            tin_1 = os.times()
            job.timeStageIn = int(round(tin_1[4] - tin_0[4]))
            tolog("Stage in for %s took %s s." % (str(ins), job.timeStageIn))
            # Extract any FAX info from the dictionary
            if FAX_dictionary.has_key('N_filesWithoutFAX'):
                job.filesWithoutFAX = FAX_dictionary['N_filesWithoutFAX']
            if FAX_dictionary.has_key('N_filesWithFAX'):
                job.filesWithFAX = FAX_dictionary['N_filesWithFAX']
            if FAX_dictionary.has_key('bytesWithoutFAX'):
                job.bytesWithoutFAX = FAX_dictionary['bytesWithoutFAX']
            if FAX_dictionary.has_key('bytesWithFAX'):
                job.bytesWithFAX = FAX_dictionary['bytesWithFAX']
            if FAX_dictionary.has_key('usedFAXandDirectIO'):
                usedFAXandDirectIO = FAX_dictionary['usedFAXandDirectIO']

        return job, ins, statusPFCTurl, usedFAXandDirectIO


    def stageOut(self, job, jobSite, outs, analysisJob, dsname, datasetDict, outputFileInfo):
        """ perform the stage-out """

        error = PilotErrors()
        pilotErrorDiag = ""
        rc = 0
        latereg = False
        rf = None

        # generate the xml for the output files and the site mover
        pfnFile = "%s/OutPutFileCatalog.xml" % job.workdir
        #outs_full = [] #full path needed, because data in sub dirs
        #for f in outs:
        #    f = job.workdir+"/"+f
        #    outs_full.append(f)
        for i in range(len(outs)):
            outs[i] = job.workdir+"/"+outs[i]
            
        try:
            _status = pUtil.PFCxml(job.experiment, pfnFile, outs, fguids=job.outFilesGuids, fntag="pfn")
        except Exception, e:
            job.pilotErrorDiag = "PFCxml failed due to problematic XML: %s" % (e)
            tolog("!!WARNING!!1113!! %s" % (job.pilotErrorDiag)) 
            return error.ERR_MISSINGGUID, job, rf, latereg
        else:
            if not _status:
                job.pilotErrorDiag = "Metadata contains missing guid(s) for output file(s)"
                tolog("!!WARNING!!2999!! %s" % (job.pilotErrorDiag))
                return error.ERR_MISSINGGUID, job, rf, latereg

        tolog("Using the newly-generated %s/%s for put operation" % (job.workdir, pfnFile))

        # the cmtconfig is needed by at least the xrdcp site mover
        cmtconfig = getCmtconfig(job.cmtconfig)

        rs = "" # return string from put_data with filename in case of transfer error
        tin_0 = os.times()
        try:
            rc, job.pilotErrorDiag, rf, rs, job.filesNormalStageOut, job.filesAltStageOut = mover.mover_put_data("xmlcatalog_file:%s" % (pfnFile), dsname, jobSite.sitename,\
                                             jobSite.computingElement, ub=jobSite.dq2url, analysisJob=analysisJob, pinitdir=self.getPilotInitDir(), scopeOut=job.scopeOut,\
                                             proxycheck=self.getProxyCheckFlag(), spsetup=job.spsetup, token=job.destinationDBlockToken,\
                                             userid=job.prodUserID, datasetDict=datasetDict, prodSourceLabel=job.prodSourceLabel,\
                                             outputDir=self.getOutputDir(), jobId=job.jobId, jobWorkDir=job.workdir, DN=job.prodUserID,\
                                             dispatchDBlockTokenForOut=job.dispatchDBlockTokenForOut, outputFileInfo=outputFileInfo,\
                                             jobDefId=job.jobDefinitionID, jobCloud=job.cloud, logFile=job.logFile,\
                                             stageoutTries=self.getStageOutRetry(), cmtconfig=cmtconfig, experiment=self.getExperiment(), fileDestinationSE=job.fileDestinationSE, job=job)
    
            tin_1 = os.times()
            job.timeStageOut = int(round(tin_1[4] - tin_0[4]))
        except Exception, e:
            tin_1 = os.times()
            job.timeStageOut = int(round(tin_1[4] - tin_0[4]))

            if 'format_exc' in traceback.__all__:
                trace = traceback.format_exc()
                pilotErrorDiag = "Put function can not be called for staging out: %s, %s" % (str(e), trace)
            else:
                tolog("traceback.format_exc() not available in this python version")
                pilotErrorDiag = "Put function can not be called for staging out: %s" % (str(e))
            tolog("!!WARNING!!3000!! %s" % (pilotErrorDiag))

            rc = error.ERR_PUTFUNCNOCALL
            job.setState(["holding", job.result[1], rc])
        else:
            if job.pilotErrorDiag != "":
                if job.pilotErrorDiag.startswith("Put error:"):
                    pre = ""
                else:
                    pre = "Put error: "
                job.pilotErrorDiag = pre + tailPilotErrorDiag(job.pilotErrorDiag, size=256-len("pilot: Put error: "))

            tolog("Put function returned code: %d" % (rc))
            if rc != 0:
                # remove any trailing "\r" or "\n" (there can be two of them)
                if rs != None:
                    rs = rs.rstrip()
                    tolog("Error string: %s" % (rs))

                # is the job recoverable?
                if error.isRecoverableErrorCode(rc):
                    _state = "holding"
                    _msg = "WARNING"
                else:
                    _state = "failed"
                    _msg = "FAILED"

                # look for special error in the error string
                if rs == "Error: string Limit exceeded 250":
                    tolog("!!%s!!3000!! Put error: file name string limit exceeded 250" % (_msg))
                    job.setState([_state, job.result[1], error.ERR_LRCREGSTRSIZE])
                else:
                    job.setState([_state, job.result[1], rc])

                tolog("!!%s!!1212!! %s" % (_msg, error.getErrorStr(rc)))
            else:
                # set preliminary finished (may be overwritten below in the LRC registration)
                job.setState(["finished", 0, 0])

                # create a weak lockfile meaning that file transfer worked
                # (useful for job recovery if activated) in the job workdir
                createLockFile(True, jobSite.workdir, lockfile=("ALLFILESTRANSFERRED_%s" % job.jobId))
                # create another lockfile in the site workdir since a transfer failure can still occur during the log transfer
                # and a later recovery attempt will fail (job workdir will not exist at that time)
                createLockFile(True, self.getParentWorkDir(), lockfile=("ALLFILESTRANSFERRED_%s"  % job.jobId))

            if job.result[0] == "holding" and '(unrecoverable)' in job.pilotErrorDiag:
                job.result[0] = "failed"
                tolog("!!WARNING!!2999!! HOLDING state changed to FAILED since error is unrecoverable")

        return rc, job, rf, latereg
    
    def cleanup(self, job, rf=None):
        """ Cleanup function """
        # 'rf' is a list that will contain the names of the files that could be transferred
        # In case of transfer problems, all remaining files will be found and moved
        # to the data directory for later recovery.

        tolog("********************************************************")
        tolog(" This job ended with (trf,pilot) exit code of (%d,%d)" % (job.result[1], job.result[2]))
        tolog("********************************************************")

        # clean up the pilot wrapper modules
        # pUtil.removePyModules(job.workdir)

        if os.path.isdir(job.workdir):
            os.chdir(job.workdir)

            # remove input files from the job workdir
            remFiles = job.inFiles
            for inf in remFiles:
                if inf and inf != 'NULL' and os.path.isfile("%s/%s" % (job.workdir, inf)): # non-empty string and not NULL
                    try:
                        os.remove("%s/%s" % (job.workdir, inf))
                    except Exception,e:
                        tolog("!!WARNING!!3000!! Ignore this Exception when deleting file %s: %s" % (inf, str(e)))
                        pass

            # only remove output files if status is not 'holding'
            # in which case the files should be saved for the job recovery.
            # the job itself must also have finished with a zero trf error code
            # (data will be moved to another directory to keep it out of the log file)

            # always copy the metadata-<jobId>.xml to the site work dir
            # WARNING: this metadata file might contain info about files that were not successfully moved to the SE
            # it will be regenerated by the job recovery for the cases where there are output files in the datadir

            #try:
            #    tolog('job.workdir is %s pworkdir is %s ' % (job.workdir, self.__pworkdir)) # Eddie
            #    copy2("%s/metadata-%s.xml" % (job.workdir, job.jobId), "%s/metadata-%s.xml" % (self.__pworkdir, job.jobId))
            #except Exception, e:
            #    tolog("Warning: Could not copy metadata-%s.xml to site work dir - ddm Adder problems will occure in case of job recovery" % (job.jobId))
            #    tolog('job.workdir is %s pworkdir is %s ' % (job.workdir, self.__pworkdir)) # Eddie
            if job.result[0] == 'holding' and job.result[1] == 0:
                try:
                    # create the data directory
                    os.makedirs(job.datadir)
                except OSError, e:
                    tolog("!!WARNING!!3000!! Could not create data directory: %s, %s" % (job.datadir, str(e)))
                else:
                    # find all remaining files in case 'rf' is not empty
                    remaining_files = []
                    moved_files_list = []
                    try:
                        if rf:
                            moved_files_list = RunJobUtilities.getFileNamesFromString(rf[1])
                            remaining_files = RunJobUtilities.getRemainingFiles(moved_files_list, job.outFiles) 
                    except Exception, e:
                        tolog("!!WARNING!!3000!! Illegal return value from Mover: %s, %s" % (str(rf), str(e)))
                        remaining_files = job.outFiles

                    # move all remaining output files to the data directory
                    nr_moved = 0
                    for _file in remaining_files:
                        try:
                            os.system("mv %s/%s %s" % (job.workdir, _file, job.datadir))
                        except OSError, e:
                            tolog("!!WARNING!!3000!! Failed to move file %s (abort all)" % (_file))
                            break
                        else:
                            nr_moved += 1

                    tolog("Moved %d/%d output file(s) to: %s" % (nr_moved, len(remaining_files), job.datadir))

                    # remove all successfully copied files from the local directory
                    nr_removed = 0
                    for _file in moved_files_list:
                        try:
                            os.system("rm %s" % (_file))
                        except OSError, e:
                            tolog("!!WARNING!!3000!! Failed to remove output file: %s, %s" % (_file, e))
                        else:
                            nr_removed += 1

                    tolog("Removed %d output file(s) from local dir" % (nr_removed))

                    # copy the PoolFileCatalog.xml for non build jobs
                    if not pUtil.isBuildJob(remaining_files):
                        #_fname = os.path.join(job.workdir, "PoolFileCatalog.xml")
                        _fname = "PoolFileCatalog.xml"
                        tolog("Copying %s to %s" % (_fname, job.datadir))
                        try:
                            copy2(_fname, job.datadir)
                        except Exception, e:
                            tolog("!!WARNING!!3000!! Could not copy PoolFileCatalog.xml to data dir - expect ddm Adder problems during job recovery")

            # remove all remaining output files from the work directory
            # (a successfully copied file should already have been removed by the Mover)
            rem = False
            for inf in job.outFiles:
                if inf and inf != 'NULL' and os.path.isfile("%s/%s" % (job.workdir, inf)): # non-empty string and not NULL
                    try:
                        os.remove("%s/%s" % (job.workdir, inf))
                    except Exception,e:
                        tolog("!!WARNING!!3000!! Ignore this Exception when deleting file %s: %s" % (inf, str(e)))
                        pass
                    else:
                        tolog("Lingering output file removed: %s" % (inf))
                        rem = True
            if not rem:
                tolog("All output files already removed from local dir")

        tolog("Payload cleanup has finished")

    def sysExit(self, jobs, rf=None):
        '''
        wrapper around sys.exit
        rs is the return string from Mover::put containing a list of files that were not transferred
        '''

        exit_code = 0
        for j in jobs:
            if j.result[2]: 
                exit_code = j.result[2]
                
        sys.stderr.close()
        tolog("RunJobTitan (payload wrapper) has finished. Exit code: %s" % exit_code)
        # change to sys.exit?
        
        os._exit(exit_code) # pilotExitCode, don't confuse this with the overall pilot exit code,
                            # which doesn't get reported back to panda server anyway
    
    def failJob(self, transExitCode, pilotExitCode, job, ins=None, pilotErrorDiag=None):
        """ set the fail code and exit """

        if pilotExitCode and job.attemptNr < 4 and job.eventServiceMerge:
            pilotExitCode = PilotErrors.ERR_ESRECOVERABLE
        job.setState(["failed", transExitCode, pilotExitCode])
        if pilotErrorDiag:
            job.pilotErrorDiag = pilotErrorDiag
        tolog("Will now update local pilot TCP server for failed JobID: %s" % job.jobId)
        rt = RunJobUtilities.updatePilotServer(job, self.getPilotServer(), self.getPilotPort(), final=True)
        if ins:
            ec = pUtil.removeFiles(job.workdir, ins)
        return job
    
    
    def get_backfill(self, partition, max_nodes = None):
    
        #  Function collect information about current available resources and  
        #  return number of nodes with possible maximum value for walltime according Titan policy
        #
        
        cmd = 'showbf --blocking -p %s' % partition
        res_tuple = commands.getstatusoutput(cmd)
        showbf_str = ""
        if res_tuple[0] == 0:
            showbf_str = res_tuple[1]
    
        res = {}
        tolog ("Available resources in %s  partition" % partition)
        tolog (showbf_str)
        if showbf_str:
            shobf_out = showbf_str.splitlines()
            tolog ("Fitted resources")
            for l in shobf_out[2:]:
                d = l.split()
                nodes = int(d[2])
                if not d[3] == 'INFINITY':
                    wal_time_arr =  d[3].split(":")
                    if len(wal_time_arr) < 4:
                        wal_time_sec = int(wal_time_arr[0])*(60*60) + int(wal_time_arr[1])*60 + int(wal_time_arr[2]) 
                        if wal_time_sec > 24 * 3600:
                            wal_time_sec = 24 * 3600
                    else:
                        wal_time_sec = 24 * 3600
                        if nodes > 1:
                            nodes = nodes - 1
                else:
                    wal_time_sec = 12 * 3600
                
                # Fitting Titan policy 
                # https://www.olcf.ornl.gov/kb_articles/titan-scheduling-policy/
                if max_nodes:
                    nodes = max_nodes if nodes > max_nodes else nodes
                
                if nodes < 125 and wal_time_sec > 2 * 3600:
                    wal_time_sec = 2 * 3600
                elif nodes < 312 and wal_time_sec > 6 * 3600:
                    wal_time_sec = 6 * 3600
                elif wal_time_sec > 12 * 3600:
                    wal_time_sec = 12 * 3600
                    
                wal_time_min = wal_time_sec/60
                tolog ("Nodes: %s, Walltime (str): %s, Walltime (min) %s" % (nodes, d[3], wal_time_min))
                
                 
                res.update({nodes: wal_time_min}) 
        
        return res
    
    def get_hpc_resources(self, partition, max_nodes = None, min_nodes = 15, min_walltime = 30):

        tolog("Looking for gap more than '%s' min" % min_walltime)
        nodes = min_nodes
        walltime =  min_walltime      
        default = True
        
        backfill = self.get_backfill(partition, max_nodes)
        if backfill:
            for n in sorted(backfill.keys(), reverse=True): 
                if min_walltime <= backfill[n] and nodes <= n:
                    nodes = n
                    walltime = backfill[n] 
                    walltime = walltime - 2
                    default = False
                    break
                if walltime <= 0:
                    walltime = min_walltime
                    nodes = min_nodes
        
        return nodes, walltime, default
      

    def jobStateChangeNotification(self, src_obj, fire_on, value):
        tolog("Job state changed to '%s'" % value)
        return True

    def GetUTCTime(self, timestr):

        do = datetime.strptime(timestr, '%a %b %d  %H:%M:%S %Y')
        se = mktime(do.timetuple())
        utctime = strftime("%Y-%m-%d %H:%M:%S", gmtime(se))

        return utctime

    def executePayload(self, thisExperiment, runCommandsList, jobs, repeat_num = 0):
        """ execute the payload """
        
        t0 = os.times() 
        res_tuple = (0, 'Undefined')
        cur_dir = os.getcwd()
        
        # special setup command. should be placed in queue defenition (or job defenition) ?
        setup_commands = ['source /lustre/atlas/proj-shared/csc108/app_dir/pilot/grid_env/external/setup.sh',
                          'source $MODULESHOME/init/bash',
                          'module load python',
                          'module load python_mpi4py',
                          'tmp_dirname=$PBS_O_WORKDIR',
                          'tmp_dirname+="/tmp"',
                          'echo $tmp_dirname',
                          'mkdir -p $tmp_dirname',
                          'export TEMP=$tmp_dirname',
                          'export TMPDIR=$TEMP',
                          'export TMP=$TEMP',
                          'export LD_LIBRARY_PATH=/opt/cray/lib64:$LD_LIBRARY_PATH',
                          'export ATHENA_PROC_NUMBER=16',
                          'export G4ATLAS_SKIPFILEPEEK=1',
                          'cd $PBS_O_WORKDIR']        
        
        
        # loop over all run commands (only >1 for multi-trfs)
        current_job_number = 1
        getstatusoutput_was_interrupted = False
        number_of_jobs = len(runCommandsList)
        
        if number_of_jobs < self.max_nodes:
            tolog("Number of jobs %s less than upper limit of requested nodes %s" % (number_of_jobs, self.max_nodes))
            self.max_nodes = number_of_jobs

        nodes, walltime, d = self.get_hpc_resources(self.partition_comp, self.max_nodes, self.nodes, self.min_walltime)
        tolog("Adjusted number of nodes: %s" % nodes)
        tolog("Number of jobs: %s" % number_of_jobs)
        if nodes > number_of_jobs:
            nodes = number_of_jobs
             
        cpu_number = self.cpu_number_per_node * nodes
        f_jobs = open("list_jobs","w")
        nl = 0
        _to_be_failed = []
        _env = []  
        for i in range(len(runCommandsList)):
            if nl < nodes: 
                f_jobs.write("PandaJob_%s!!%s %s!!%s!!%s\n" % (runCommandsList[i]["jobId"], runCommandsList[i]["payload"], runCommandsList[i]["parameters"], jobs[i].stdout, jobs[i].stderr))
                for l in runCommandsList[i]['environment']:
                    if not l in _env:
                        _env.append(l)
                #tolog("Job %s added to list for execution (job_cnt = %s, nodes = %s)" % (runCommandsList[i]["jobId"], nl, nodes))
            else:
                _to_be_failed.append(runCommandsList[i]["jobId"])
                tolog("Job %s added to be failed (job_cnt = %s, nodes = %s)" % (runCommandsList[i]["jobId"], nl, nodes))
            nl += 1

        f_jobs.close()
        tolog("env = %s" % _env)
        _final_hpc_status = "NotLaunched"
        cons_time_sec = 0
        UTCstartTime = ''
        UTCendTime = ''
        tolog("Launch parameters \nWalltime limit         : %s (min)\nRequested nodes (cores): %s (%s)" % (walltime,nodes,cpu_number))
        try:
            # add the full job command to the job_setup.sh file
            to_script = "\n".join(setup_commands)
            to_script = to_script + "\n" +"\n".join(_env)
            #to_script = "%s\naprun -n %s -d %s %s/multi-job_mpi_Sim_tf_v2.py list_jobs" % (to_script, cpu_number/self.number_of_threads, self.number_of_threads, cur_dir)    
            to_script = "%s\naprun -n %s -d %s %s/multi-job_mpi_Sim_tf_v2.py list_jobs" % (to_script, cpu_number/self.number_of_threads, self.number_of_threads, cur_dir)    
            
            
            thisExperiment.updateJobSetupScript(cur_dir, to_script=to_script)
            
            # Simple SAGA fork variant
            tolog("******* SAGA call to execute payload *********")
            try:

                js = saga.job.Service("pbs://localhost")
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
                        #to avoid wrong calculation of walltime... set endTime = statTime
                        f_time = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
                        j.startTime = f_time
                        j.endTime = f_time

                        runJob.failJob(0, 0, j, ins=j.inFiles, pilotErrorDiag='No availble slot')
                        jobs.remove(j)
                    else:
                        j.setState(["running", 0, 0])
                        # update the job state file
                        j.jobState = "running"
                        j.hpcStatus = fork_job.state
                        hpcjobId = fork_job.id
                        j.HPCJobId = hpcjobId.split('-')[1][1:-1]
                        j.startTime = self.GetUTCTime(fork_job.started)
                        rt = RunJobUtilities.updatePilotServer(j, self.getPilotServer(), self.getPilotPort())
                        if rt:
                            tolog("TCP Server updated with HPC Status and CoreCount and Running state from: %s" % j.startTime)
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
                    UTCendTime = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
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
                j.setState(["transferring", j.result[1], j.result[2]])
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
        
        if res_tuple[0] == 0:
            tolog("Payload execution finished")
        else:
            tolog("Payload execution failed: res = %s" % (str(res_tuple)))
    
        t1 = os.times()
        #t = map(lambda x, y:x-y, t1, t0) # get the time consumed
        for j in jobs[:]:
            j.cpuConsumptionUnit = "s" 
            j.cpuConversionFactor = 1.0
            rep_data = []
            cct = 0.0 
            try:
                r_report = open(("%s/rank_report.txt" % j.workdir),"r")
                rep_data = r_report.readlines() 
                for s in rep_data:
                    if s.startswith("cpuConsumptionTime"):
                        cct_s = s.split()
                        cct = float(cct_s[1])
                    elif s.startswith("exitCode"):
                        trf_exitcode_s = s.split()
                        j.result[1] = int(trf_exitcode_s[1])
            except:
                tolog("Error, during reading rank report: %s/rank_report.txt for job %s" % (j.workdir, j.jobId))
                tolog("Job [%s] is about to be failed" % j.jobId)
                j.pilotErrorDiag = "Fail to extract rank report [%s], job didn't finished properly" % j.jobId
                runJob.failJob(2999, 0, j, pilotErrorDiag=j.pilotErrorDiag)
                jobs.remove(j)
            else:
                j.cpuConsumptionTime = int(cct * j.cpuConversionFactor)
                tolog("Job %s CPU usage: %s %s" % (j.jobId, j.cpuConsumptionTime, j.cpuConsumptionUnit))
                tolog("Job %s CPU conversion factor: %1.10f" % (j.jobId, j.cpuConversionFactor))
            #j.timeExe = int(round(t1[4] - t0[4])) very old method
        
        jobs_results = {}
        tolog("Original LRMS exit code: %s" % (res_tuple[0]))
        if res_tuple[0] != None:
            tolog("Exit code: %s (returned from LRMS)" % (res_tuple[0]%255))
            for j in jobs:
                res0, exitAcronym, exitMsg = self.getTrfExitInfo(j.result[1], j.workdir, delay=False)
                jobs_results[j.jobId] = (res0, exitMsg)
        else:
            tolog("Exit code: None (returned from OS, Job was canceled)")
            exitMsg = "Job was canceled by internal call"
        # check the job report for any exit code that should replace the res_tuple[0]
        
        res = (jobs_results, res_tuple[1])
    
        # dump an extract of the payload output
        #if number_of_jobs > 1:
        #    _stdout = job.stdout
        #    _stderr = job.stderr
        #   _stdout = _stdout.replace(".txt", "_N.txt")
        #    _stderr = _stderr.replace(".txt", "_N.txt")
        #    tolog("NOTE: For %s output, see files %s, %s (N = [1, %d])" % (job.payload, _stdout, _stderr, number_of_jobs))
        #else:
        #    tolog("NOTE: For %s output, see files %s, %s" % (job.payload, job.stdout, job.stderr))
    
        # JEM job-end callback
        #try:
        #    from JEMstub import notifyJobEnd2JEM
        #    notifyJobEnd2JEM(job, tolog)
        #except:
        #    pass # don't care (fire and forget)
    
        return res, jobs, getstatusoutput_was_interrupted, current_job_number


if __name__ == "__main__":

    tolog("Starting RunJobTitan")
    # Get error handler
    error = PilotErrors()

    # Get runJob object
    runJob = RunJobTitan()
    
    # Setup HPC specific parameters for Titan
    
    runJob.cpu_number_per_node = 16
    #runJob.walltime = 105 # minutes
    runJob.max_nodes =  300 #2000 
    runJob.number_of_threads = 16  # 1 - one thread per task
    runJob.min_walltime =  110  # min. 2 hour for gap
    runJob.waittime = 5
    runJob.nodes = 15
    runJob.partition_comp = 'titan'
    runJob.project_id = "CSC108"
    runJob.executed_queue = readpar('localqueue') 
    
    # Define a new parent group
    os.setpgrp()

    # Protect the runJob code with exception handling
    hP_ret = False
    try:
        # always use this filename as the new jobDef module name
        import JobsArray

        jobSite = Site.Site()

        return_tuple = runJob.argumentParser()
        tolog("argumentParser returned: %s" % str(return_tuple))
        jobSite.setSiteInfo(return_tuple)

#            jobSite.setSiteInfo(argParser(sys.argv[1:]))

        # reassign workdir for this job
        jobSite.workdir = jobSite.wntmpdir

        if runJob.getPilotLogFilename() != "":
            pUtil.setPilotlogFilename(runJob.getPilotLogFilename())

        # set node info
        #node = Node.Node()
        #node.setNodeName(os.uname()[1])
        #node.collectWNInfo(jobSite.workdir)

        # redirect stder
        sys.stderr = open("%s/runjob.stderr" % (jobSite.workdir), "w")

        tolog("Current job workdir is: %s" % os.getcwd())
        tolog("Site workdir is: %s" % jobSite.workdir)
        # get the experiment object
        thisExperiment = getExperiment(runJob.getExperiment())

        tolog("RunJob will serve experiment: %s" % (thisExperiment.getExperiment()))

        # set the cache (used e.g. by LSST)
        #if runJob.getCache():
        #    thisExperiment.setCache(runJob.getCache())

        region = readpar('region')
        #JR = JobRecovery()
        jobs = []
        try:
            for j in JobsArray.jobs:
                job = Job.Job()
                job.setJobDef(j)
                job.workdir = "%s/PandaJob_%s" % (jobSite.workdir, job.jobId) 
                job.experiment = runJob.getExperiment()
                # figure out and set payload file names
                job.setPayloadName(thisExperiment.getPayloadName(job))
                # prepare for the output file data directory
                # (will only created for jobs that end up in a 'holding' state)
                job.datadir = runJob.getParentWorkDir() + "/PandaJobsData" 
                jobs.append(job)
        except Exception, e:
            pilotErrorDiag = "Failed to process job info: %s" % str(e)
            tolog("!!WARNING!!3000!! %s" % (pilotErrorDiag))
            for j in jobs:
                runJob.failJob(0, error.ERR_UNKNOWN, job, pilotErrorDiag=pilotErrorDiag)
            
        
        #runJob.max_nodes = len(jobs)
        # register cleanup function
        atexit.register(runJob.cleanup, jobs[0])

        # to trigger an exception so that the SIGTERM signal can trigger cleanup function to run
        # because by default signal terminates process without cleanup.
        def sig2exc(sig, frm):
            """ signal handler """

            error = PilotErrors()
            runJob.setGlobalPilotErrorDiag("!!FAILED!!3000!! SIGTERM Signal %s is caught in child pid=%d!\n" % (sig, os.getpid()))
            tolog(runJob.getGlobalPilotErrorDiag())
            if sig == signal.SIGTERM:
                runJob.setGlobalErrorCode(error.ERR_SIGTERM)
            elif sig == signal.SIGQUIT:
                runJob.setGlobalErrorCode(error.ERR_SIGQUIT)
            elif sig == signal.SIGSEGV:
                runJob.setGlobalErrorCode(error.ERR_SIGSEGV)
            elif sig == signal.SIGXCPU:
                runJob.setGlobalErrorCode(error.ERR_SIGXCPU)
            elif sig == signal.SIGBUS:
                runJob.setGlobalErrorCode(error.ERR_SIGBUS)
            elif sig == signal.SIGUSR1:
                runJob.setGlobalErrorCode(error.ERR_SIGUSR1)
            else:
                runJob.setGlobalErrorCode(error.ERR_KILLSIGNAL)
            runJob.setFailureCode(runJob.getGlobalErrorCode)
            # print to stderr
            print >> sys.stderr, runJob.getGlobalPilotErrorDiag()
            raise SystemError(sig)

        signal.signal(signal.SIGTERM, sig2exc)
        signal.signal(signal.SIGQUIT, sig2exc)
        signal.signal(signal.SIGSEGV, sig2exc)
        signal.signal(signal.SIGXCPU, sig2exc)
        signal.signal(signal.SIGBUS, sig2exc)

        # see if it's an analysis job or not
        analysisJob = isAnalysisJob(job.trf.split(",")[0])
        if analysisJob:
            tolog("User analysis job")
        else:
            tolog("Production job")
        tolog("runJobTitan received a job with prodSourceLabel=%s" % (job.prodSourceLabel))

        # setup starts here ................................................................................
        t0_setup = os.times()
        runCommandsList = []
        for j in jobs[:]:
            # update the job state file
            j.jobState = "setup"
            #_retjs = JR.updateJobStateTest(job, jobSite, node, mode="test")
            _checkCMTCONFIG = False
            if jobs.index(j) == 0:
                _checkCMTCONFIG = True
            # send [especially] the process group back to the pilot
            j.setState([j.jobState, 0, 0])
            rt = RunJobUtilities.updatePilotServer(j, runJob.getPilotServer(), runJob.getPilotPort())
            
            # prepare the setup and get the run command list
            ec, runCommandList, job, multi_trf = runJob.setup(j, jobSite, thisExperiment, _checkCMTCONFIG)
            if ec != 0:
                tolog("!!WARNING!!2999!! runJob setup failed: %s" % (j.pilotErrorDiag))
                runJob.failJob(0, ec, j, pilotErrorDiag=j.pilotErrorDiag)
                jobs.remove(j)
            tolog("Setup has finished successfully")
            # job has been updated, display it again
            # job.displayJob()
            for c in runCommandList:
                c['jobId'] = j.jobId
            runCommandsList += runCommandList
        t1_setup = os.times()
        t_setup = t1_setup[4] - t0_setup[4]
        tolog("SetUp for %s jobs took %s s." % (len(jobs), t_setup))
         
        t0_stagein = os.times()    
        ins = {}
        downloaded_files = {}
        for j in jobs[:]:
            if j.inFiles and j.inFiles != ['']:
                tolog("Setting stage-in state until all input files have been copied for job: %s  inFiles: %s" % (j.jobId, j.inFiles))
                if not os.path.exists(j.workdir):
                    os.mkdir(j.workdir)
                
                j.setState(["stagein", 0, 0])
                # send the special setup string back to the pilot (needed for the log transfer on xrdcp systems)
                rt = RunJobUtilities.updatePilotServer(j, runJob.getPilotServer(), runJob.getPilotPort())
        
        
                # update the job state file
                j.jobState = "stagein"
                #_retjs = JR.updateJobStateTest(job, jobSite, node, mode="test")
                download = True
                t_guids = tuple(j.inFilesGuids)
                if t_guids in downloaded_files.keys():
                    tolog("File(s) already downloaded for other job and will be copied")
                    _src_dir = downloaded_files[t_guids]['dir']
                    try:
                        for f in glob(_src_dir + "/*"):
                            copy2(f, j.workdir)
                        download = False
                        tolog("Files copied from %s to %s" % (_src_dir, j.workdir))
                    except:
                        tolog("Unable to copy input files")
                if download:
                    # update copysetup[in] for production jobs if brokerage has decided that remote I/O should be used
                    if j.transferType == 'direct':
                        tolog('Brokerage has set transfer type to \"%s\" (remote I/O will be attempted for input files, any special access mode will be ignored)' %\
                              (j.transferType))
                        RunJobUtilities.updateCopysetups('', transferType=j.transferType)
            
                    # stage-in all input files (if necessary)
                    j, _ins, statusPFCTurl, usedFAXandDirectIO = runJob.stageIn(j, jobSite, analysisJob)
                    if j.result[2] != 0:
                        tolog("StageIn: Failing job %s with ec: %d" % (j.jobId, ec))
                        runJob.failJob(0, j.result[2], j, ins=_ins, pilotErrorDiag=j.pilotErrorDiag)
                        jobs.remove(j)
                        for c in runCommandsList[:]:
                            if j.jobId == c["jobId"]:
                                runCommandsList.remove(c) 
                        continue
                    downloaded_files[t_guids] = {"lfns": _ins, "dir": j.workdir}
                    
                ins[j.workdir] = _ins    
                # after stageIn, all file transfer modes are known (copy_to_scratch, file_stager, remote_io)
                # consult the FileState file dictionary if cmd3 should be updated (--directIn should not be set if all
                # remote_io modes have been changed to copy_to_scratch as can happen with ByteStream files)
                # and update the run command list if necessary.
                # in addition to the above, if FAX is used as a primary site mover and direct access is enabled, then
                # the run command should not contain the --oldPrefix, --newPrefix, --lfcHost options but use --usePFCTurl
                for runCommandList in runCommandsList:
                    if runCommandList['jobId'] == j.jobId:
                        hasInput = True
                        if job.inFiles == ['']:
                            hasInput = False
                        runCommandList = RunJobUtilities.updateRunCommandListMJ(runCommandList, runJob.getParentWorkDir(), job.jobId, statusPFCTurl, analysisJob, usedFAXandDirectIO, hasInput)
        t1_stagein = os.times()
        t_stagein = t1_stagein[4] - t0_stagein[4]
        tolog("StageIn for %s jobs took %s s." % (len(ins), t_stagein))
        tolog(ins)
        # (stage-in ends here) .............................................................................
        if jobs:
            tolog("About to launch %s jobs in %s commands" % (len(jobs), len(runCommandsList)))
            # change to running state since all input files have been staged
            t0_update = os.times()
            '''
            tolog("Changing to running state")
            for j in jobs:
                j.setState(["running", 0, 0])
                rt = RunJobUtilities.updatePilotServer(j, runJob.getPilotServer(), runJob.getPilotPort())
    
                # update the job state file
                j.jobState = "running"
                #_retjs = JR.updateJobStateTest(job, jobSite, node, mode="test")
            t1_update = os.times()
            t_update = t1_update[4] - t0_update[4]
            tolog("Internal update of %s jobs took %s s." % (len(jobs), t_update))
            '''
            # run the job(s) ...................................................................................
            # Set ATLAS_CONDDB if necessary, and other env vars
            RunJobUtilities.setEnvVars(jobSite.sitename)
    
            # execute the payload
            res, jobs, getstatusoutput_was_interrupted, current_job_number = runJob.executePayload(thisExperiment, runCommandsList, jobs)
            # if payload leaves the input files, delete them explicitly
            if ins:
                for workdir in ins:
                    try:
                        ec = pUtil.removeFiles(workdir, ins[workdir])
                    except:
                        pass
            # payload error handling
            ed = ErrorDiagnosis()
            if not res:
                for j in jobs:
                    j.jobState = "cancelled"
                    j.setState(["cancelled", 0, 0])
                    rt = RunJobUtilities.updatePilotServer(j, runJob.getPilotServer(), runJob.getPilotPort())
            else:
                # FInally this should be re-implemented with proper workdir for each job
                tolog("Extract info from stdout for MPI")
                for j in jobs[:]:
                    try:    
                        tolog("Extract info for job: %s" % j.jobId)
                        for runCommandList in runCommandsList:
                            if j.jobId == runCommandList['jobId']:
                                #tolog("runCommandlist:\n %s" % runCommandList)
                                r = res[0][j.jobId]
                                j = ed.interpretPayload(j, r, getstatusoutput_was_interrupted, current_job_number, runCommandList, runJob.getFailureCode())
                    except:
                        tolog("Fail to extract info for job: %s" % j.jobId)
                        #tolog("Job [%s] is about to be failed" % j.jobId)
                        #j.pilotErrorDiag = "Fail to extract info for job (job report missing)"
                        #runJob.failJob(j.result[1], j.result[2], j, pilotErrorDiag=j.pilotErrorDiag)
                        #jobs.remove(j)
                        pass
            for j in jobs[:]:
                tolog("JobId %s : trf_EC: %s, pilot_EC: %s  ['%s'] nEvents: %s" % (j.jobId, j.result[1], j.result[2], j.result[0], j.nEvents))
                if j.result[1] != 0 or j.result[2] != 0:
                    tolog("Job [%s] is about to be failed" % j.jobId)
                    runJob.failJob(j.result[1], j.result[2], j, pilotErrorDiag=j.pilotErrorDiag)
                    jobs.remove(j)

            # update the job state file
            tolog("OutPutDir: %s" % runJob.getOutputDir()) 
            #_retjs = JR.updateJobStateTest(job, jobSite, node, mode="test")

            # verify and prepare and the output files for transfer
        # stage-out ........................................................................................
        t0_stageout = os.times()
        pUtil.touch(jobSite.workdir + '/STAGEOUT_STARTED')
        tolog("StageOut for %s jobs started" % len(jobs))
        for j in jobs[:]:
            j.jobState = "stageout"
            ec, pilotErrorDiag, outs, outsDict = RunJobUtilities.prepareOutFiles(j.outFiles, j.logFile, j.workdir)
            if ec:
                # missing output file (only error code from prepareOutFiles)
                tolog("Job: %s failed during stageout (prepareOutFiles)" % j.jobId) 
                runJob.failJob(j.result[1], ec, j, pilotErrorDiag=pilotErrorDiag)
                break
            
            tolog("JobID: %s outsDict: %s" %  (j.jobId, str(outsDict)))
            #tolog("outs: %s" %  (str(outs)))
            
            # update the current file states
            updateFileStates(outs, runJob.getParentWorkDir(), j.jobId, mode="file_state", state="created")
            dumpFileStates(runJob.getParentWorkDir(), j.jobId)
            # create xml string to pass to dispatcher for atlas jobs
            outputFileInfo = {}
            if outs or (j.logFile and j.logFile != ''):
                # get the datasets for the output files
                dsname, datasetDict = runJob.getDatasets(j)

                # re-create the metadata.xml file, putting guids of ALL output files into it.
                # output files that miss guids from the job itself will get guids in PFCxml function

                # first rename and copy the trf metadata file for non-build jobs
                if not pUtil.isBuildJob(outs):
                    runJob.moveTrfMetadata(j.workdir, j.jobId)

                # create the metadata for the output + log files
                ec, job, outputFileInfo = runJob.createFileMetadata(list(outs), j, outsDict, dsname, datasetDict, jobSite.sitename, analysisJob=analysisJob)
                if ec:
                    tolog("WARNING: Metadatafile was not created")
        
            # move output files from workdir to local DDM area
            #finalUpdateDone = False
            if outs:
                tolog("Setting stage-out state until all output files have been copied")
                j.setState(["stageout", 0, 0])
                rt = RunJobUtilities.updatePilotServer(j, runJob.getPilotServer(), runJob.getPilotPort())

                # stage-out output files
            
                ec, j, rf, latereg = runJob.stageOut(j, jobSite, outs, analysisJob, dsname, datasetDict, outputFileInfo)
                # error handling
                
                if j.result[0] == "finished" or ec == error.ERR_PUTFUNCNOCALL:
                    rt = RunJobUtilities.updatePilotServer(j, runJob.getPilotServer(), runJob.getPilotPort(), final=True)
                else:
                    rt = RunJobUtilities.updatePilotServer(j, runJob.getPilotServer(), runJob.getPilotPort(), final=True, latereg=latereg)
            
            
                if ec == error.ERR_NOSTORAGE:
                    # update the current file states for all files since nothing could be transferred
                    updateFileStates(outs, runJob.getParentWorkDir(), j.jobId, mode="file_state", state="not_transferred")
                    dumpFileStates(runJob.getParentWorkDir(), j.jobId)

            #finalUpdateDone = True
            if ec != 0:
                runJob.cleanup(j, rf)
            # (stage-out ends here) .......................................................................
            tolog("Setting finished state JobID: %s" % j.jobId)
            job.setState(["finished", 0, 0])
            #if not finalUpdateDone:
            tolog("Final job state update JobID: %s" % j.jobId)
            rt = RunJobUtilities.updatePilotServer(job, runJob.getPilotServer(), runJob.getPilotPort(), final=True)
        
        t1_stageout = os.times()
        t_stageout = t1_stageout[4] - t0_stageout[4]
        tolog("StageOut for %s jobs took %s s." % (len(jobs), t_stageout))
        
        runJob.sysExit(jobs)

    except Exception, errorMsg:

        error = PilotErrors()

        if runJob.getGlobalPilotErrorDiag() != "":
            pilotErrorDiag = "Exception caught in runJobTitan: %s" % (runJob.getGlobalPilotErrorDiag())
        else:
            pilotErrorDiag = "Exception caught in runJobTitan: %s" % str(errorMsg)

        if 'format_exc' in traceback.__all__:
            pilotErrorDiag += ", " + traceback.format_exc()    

        try:
            tolog("!!FAILED!!3001!! %s" % (pilotErrorDiag))
        except Exception, e:
            if len(pilotErrorDiag) > 10000:
                pilotErrorDiag = pilotErrorDiag[:10000]
                tolog("!!FAILED!!3001!! Truncated (%s): %s" % (e, pilotErrorDiag))
            else:
                pilotErrorDiag = "Exception caught in runJobTitan: %s" % (e)
                tolog("!!FAILED!!3001!! %s" % (pilotErrorDiag))

#        # restore the proxy if necessary
#        if hP_ret:
#            rP_ret = proxyguard.restoreProxy()
#            if not rP_ret:
#                tolog("Warning: Problems with storage can occur since proxy could not be restored")
#            else:
#                hP_ret = False
#                tolog("ProxyGuard has finished successfully")

        tolog("sys.path=%s" % str(sys.path))
        cmd = "pwd;ls -lF %s;ls -lF" % (runJob.getPilotInitDir())
        tolog("Executing command: %s" % (cmd))
        out = commands.getoutput(cmd)
        tolog("%s" % (out))
        
        for j in JobsArray.jobs:
            job = Job.Job()
            job.setJobDef(j)
            job.pilotErrorDiag = pilotErrorDiag
            job.result[0] = "failed"
            if runJob.getGlobalErrorCode() != 0:
                job.result[2] = runJob.getGlobalErrorCode()
            else:
                job.result[2] = error.ERR_RUNJOBEXC
            tolog("Failing job with error code: %d" % (job.result[2]))
        # fail the job without calling sysExit/cleanup (will be called anyway)
        
        runJob.failJob(0, job.result[2], job, pilotErrorDiag=pilotErrorDiag, docleanup=False)
    
    
    
    
    
    
    
    
    
    
