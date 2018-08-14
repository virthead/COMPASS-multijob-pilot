# Class definition:
#   RunJobEvent:  module for receiving and processing events from the Event Service
#   Instances are generated with RunJobFactory via pUtil::getRunJob()
#   Implemented as a singleton class
#   http://stackoverflow.com/questions/42558/python-and-the-singleton-pattern

# Import relevant python/pilot modules
from RunJob import RunJob                        # Parent RunJob class
from pUtil import tolog, writeToFileWithStatus   # Logging method that sends text to the pilot log

# Standard python modules
import os
import re
import sys
import time
import stat
import atexit
import signal
import commands
import traceback
from optparse import OptionParser
from json import loads
from shutil import copy2
from xml.dom import minidom

# Pilot modules
import Job
import Node
import Site
import pUtil
import RunJobUtilities
import Mover as mover
from JobRecovery import JobRecovery
from FileStateClient import updateFileStates, dumpFileStates
from ErrorDiagnosis import ErrorDiagnosis # import here to avoid issues seen at BU with missing module
from PilotErrors import PilotErrors
from StoppableThread import StoppableThread
from pUtil import debugInfo, tolog, isAnalysisJob, readpar, createLockFile, getDatasetDict, getChecksumCommand,\
     tailPilotErrorDiag, getCmtconfig, getExperiment, getEventService, httpConnect,\
     getSiteInformation, getGUID
from FileHandling import getExtension, addToOSTransferDictionary
from EventRanges import downloadEventRanges, updateEventRange

try:
    from PilotYamplServer import PilotYamplServer as MessageServer
except Exception, e:
    MessageServer = None
    print "RunJobEvent caught exception:",e

class RunJobEvent(RunJob):

    # private data members
    __runjob = "RunJobEvent"                     # String defining the sub class
    __instance = None                            # Boolean used by subclasses to become a Singleton
    __error = PilotErrors()                      # PilotErrors object
    __errorCode = 0                              # Error code, e.g. set by stage-out method
    __experiment = "ATLAS"                       # Current experiment (can be set with pilot option -F <experiment>)
    __pilotserver = "localhost"                  # Default server
    __pilotport = 88888                          # Default port
    __failureCode = None                         # Set by signal handler when user/batch system kills the job
    __pworkdir = "/tmp"                          # Site work dir used by the parent
    __logguid = None                             # GUID for the log file
    __pilotlogfilename = "pilotlog.txt"          # Default pilotlog filename
    __stageinretry = None                        # Number of stage-in tries
    __stageoutretry = None                       # Number of stage-out tries
    __pilot_initdir = ""                         # location of where the pilot is untarred and started
    __proxycheckFlag = True                      # True (default): perform proxy validity checks, False: no check
    __globalPilotErrorDiag = ""                  # Global pilotErrorDiag used with signal handler (only)
    __globalErrorCode = 0                        # Global error code used with signal handler (only)
    __inputDir = ""                              # Location of input files (source for mv site mover)
    __outputDir = ""                             # Location of output files (destination for mv site mover)
    __taskID = ""                                # TaskID (needed for OS transfer file and eventually for job metrics)
    __event_loop_running = False                 # Is the event loop running?
    __output_files = []                          # A list of all files that have been successfully staged-out, used by createFileMetadata()
    __guid_list = []                             # Keep track of downloaded GUIDs
    __lfn_list = []                              # Keep track of downloaded LFNs
    __eventRange_dictionary = {}                 # eventRange_dictionary[event_range_id] = [path, cpu, wall]
    __eventRangeID_dictionary = {}               # eventRangeID_dictionary[event_range_id] = True (corr. output file has been transferred)
    __stageout_queue = []                        # Queue for files to be staged-out; files are added as they arrive and removed after they have been staged-out
    __pfc_path = ""                              # The path to the pool file catalog
    __message_server = None                      #
    __message_thread = None                      #
    __status = True                              # Global job status; will be set to False if an event range or stage-out fails
    __athenamp_is_ready = False                  # True when an AthenaMP worker is ready to process an event range
    __asyncOutputStager_thread = None            #
    __analysisJob = False                        # True for analysis job
    __jobSite = None                             # Site object
    __job = None                                 # Job object
    __cache = ""                                 # Cache URL, e.g. used by LSST
    __metadata_filename = ""                     # Full path to the metadata file
    __yamplChannelName = None                    # Yampl channel name
    __useEventIndex = True                       # Should Event Index be used? If not, a TAG file will be created
    __tokenextractor_input_list_filenane = ""    #
    __sending_event_range = False                # True while event range is being sent to payload
    __current_event_range = ""                   # Event range being sent to payload

    # Getter and setter methods

    def getExperiment(self):
        """ Getter for __experiment """

        return self.__experiment

    def setExperiment(self, experiment):
        """ Setter for __experiment """

        self.__experiment = experiment

    def getPilotServer(self):
        """ Getter for __pilotserver """

        return self.__pilotserver

    def setPilotServer(self, pilotserver):
        """ Setter for __pilotserver """

        self.__pilotserver = pilotserver

    def getPilotPort(self):
        """ Getter for __pilotport """

        return self.__pilotport

    def setPilotPort(self, pilotport):
        """ Setter for __pilotport """

        self.__pilotport = pilotport

    def getFailureCode(self):
        """ Getter for __failureCode """

        return self.__failureCode

    def setFailureCode(self, code):
        """ Setter for __failureCode """

        self.__failureCode = code

    def getParentWorkDir(self):
        """ Getter for __pworkdir """

        return self.__pworkdir

    def setParentWorkDir(self, pworkdir):
        """ Setter for __pworkdir """

        self.__pworkdir = pworkdir

    def getLogGUID(self):
        """ Getter for __logguid """

        return self.__logguid

    def setLogGUID(self, logguid):
        """ Setter for __logguid """

        self.__logguid = logguid

    def getPilotLogFilename(self):
        """ Getter for __pilotlogfilename """

        return self.__pilotlogfilename

    def setPilotLogFilename(self, pilotlogfilename):
        """ Setter for __pilotlogfilename """

        self.__pilotlogfilename = pilotlogfilename

    def getStageInRetry(self):
        """ Getter for __stageinretry """

        return self.__stageinretry

    def setStageInRetry(self, stageinretry):
        """ Setter for __stageinretry """

        self.__stageinretry = stageinretry

    def getStageOutRetry(self):
        """ Getter for __stageoutretry """

        return self.__stageoutretry

    def setStageOutRetry(self, stageoutretry):
        """ Setter for __stageoutretry """

        self.__stageoutretry = stageoutretry

    def getPilotInitDir(self):
        """ Getter for __pilot_initdir """

        return self.__pilot_initdir

    def setPilotInitDir(self, pilot_initdir):
        """ Setter for __pilot_initdir """

        self.__pilot_initdir = pilot_initdir

    def getProxyCheckFlag(self):
        """ Getter for __proxycheckFlag """

        return self.__proxycheckFlag

    def setProxyCheckFlag(self, proxycheckFlag):
        """ Setter for __proxycheckFlag """

        self.__proxycheckFlag = proxycheckFlag

    def getGlobalPilotErrorDiag(self):
        """ Getter for __globalPilotErrorDiag """

        return self.__globalPilotErrorDiag

    def setGlobalPilotErrorDiag(self, pilotErrorDiag):
        """ Setter for __globalPilotErrorDiag """

        self.__globalPilotErrorDiag = pilotErrorDiag

    def getGlobalErrorCode(self):
        """ Getter for __globalErrorCode """

        return self.__globalErrorCode

    def setGlobalErrorCode(self, code):
        """ Setter for __globalErrorCode """

        self.__globalErrorCode = code

    def getErrorCode(self):
        """ Getter for __errorCode """

        return self.__errorCode

    def setErrorCode(self, code):
        """ Setter for __errorCode """

        self.__errorCode = code

    def getInputDir(self):
        """ Getter for __inputDir """

        return self.__inputDir

    def setInputDir(self, inputDir):
        """ Setter for __inputDir """

        self.__inputDir = inputDir

    def getOutputDir(self):
        """ Getter for __outputDir """

        return self.__outputDir

    def setOutputDir(self, outputDir):
        """ Setter for __outputDir """

        self.__outputDir = outputDir

    def getEventLoopRunning(self):
        """ Getter for __event_loop_running """

        return self.__event_loop_running

    def setEventLoopRunning(self, event_loop_running):
        """ Setter for __event_loop_running """

        self.__event_loop_running = event_loop_running

    def getOutputFiles(self):
        """ Getter for __output_files """

        return self.__output_files

    def setOutputFiles(self, output_files):
        """ Setter for __output_files """

        self.__output_files = output_files

    def getGUIDList(self):
        """ Getter for __guid_list """

        return self.__guid_list

    def setGUIDList(self, guid_list):
        """ Setter for __guid_list """

        self.__guid_list = guid_list

    def getLFNList(self):
        """ Getter for __lfn_list """

        return self.__lfn_list

    def setLFNList(self, lfn_list):
        """ Setter for __lfn_list """

        self.__lfn_list = lfn_list

    def getEventRangeDictionary(self):
        """ Getter for __eventRange_dictionary """

        return self.__eventRange_dictionary

    def setEventRangeDictionary(self, eventRange_dictionary):
        """ Setter for __eventRange_dictionary """

        self.__eventRange_dictionary = eventRange_dictionary

    def getEventRangeIDDictionary(self):
        """ Getter for __eventRangeID_dictionary """

        return self.__eventRangeID_dictionary

    def setEventRangeIDDictionary(self, eventRangeID_dictionary):
        """ Setter for __eventRangeID_dictionary """

        self.__eventRangeID_dictionary = eventRangeID_dictionary

    def getStageOutQueue(self):
        """ Getter for __stageout_queue """

        return self.__stageout_queue

    def setStageOutQueue(self, stageout_queue):
        """ Setter for __stageout_queue """

        self.__stageout_queue = stageout_queue

    def getPoolFileCatalogPath(self):
        """ Getter for __pfc_path """

        return self.__pfc_path

    def setPoolFileCatalogPath(self, pfc_path):
        """ Setter for __pfc_path """

        self.__pfc_path = pfc_path

    def getMessageServer(self):
        """ Getter for __message_server """

        return self.__message_server

    def setMessageServer(self, message_server):
        """ Setter for __message_server """

        self.__message_server = message_server

    def getMessageThread(self):
        """ Getter for __message_thread """

        return self.__message_thread

    def setMessageThread(self, message_thread):
        """ Setter for __message_thread """

        self.__message_thread = message_thread

    def isAthenaMPReady(self):
        """ Getter for __athenamp_is_ready """

        return self.__athenamp_is_ready

    def setAthenaMPIsReady(self, athenamp_is_ready):
        """ Setter for __athenamp_is_ready """

        self.__athenamp_is_ready = athenamp_is_ready

    def getAsyncOutputStagerThread(self):
        """ Getter for __asyncOutputStager_thread """

        return self.__asyncOutputStager_thread

    def setAsyncOutputStagerThread(self, asyncOutputStager_thread):
        """ Setter for __asyncOutputStager_thread """

        self.__asyncOutputStager_thread = asyncOutputStager_thread

    def getAnalysisJob(self):
        """ Getter for __analysisJob """

        return self.__analysisJob

    def setAnalysisJob(self, analysisJob):
        """ Setter for __analysisJob """

        self.__analysisJob = analysisJob

    def getCache(self):
        """ Getter for __cache """

        return self.__cache

    def setCache(self, cache):
        """ Setter for __cache """

        self.__cache = cache

    def getMetadataFilename(self):
        """ Getter for __cache """

        return self.__metadata_filename

    def setMetadataFilename(self, event_range_id):
        """ Setter for __metadata_filename """

        self.__metadata_filename = os.path.join(self.__job.workdir, "metadata-%s.xml" % (event_range_id))

    def getJobSite(self):
        """ Getter for __jobSite """

        return self.__jobSite

    def setJobSite(self, jobSite):
        """ Setter for __jobSite """

        self.__jobSite = jobSite

    def getYamplChannelName(self):
        """ Getter for __yamplChannelName """

        return self.__yamplChannelName

    def setYamplChannelName(self, yamplChannelName):
        """ Setter for __yamplChannelName """

        self.__yamplChannelName = yamplChannelName

    def getStatus(self):
        """ Getter for __status """

        return self.__status

    def setStatus(self, status):
        """ Setter for __status """

        self.__status = status

    def isSendingEventRange(self):
        """ Getter for __sending_event_range """

        return self.__sending_event_range

    def setSendingEventRange(self, sending_event_range):
        """ Setter for __sending_event_range """

        self.__sending_event_range = sending_event_range

    def getCurrentEventRange(self):
        """ Getter for __current_event_range """

        return self.__current_event_range

    def setCurrentEventRange(self, current_event_range):
        """ Setter for __current_event_range """

        self.__current_event_range = current_event_range

    # Get/setters for the job object

    def getJob(self):
        """ Getter for __job """

        return self.__job

    def setJob(self, job):
        """ Setter for __job """

        self.__job = job

        # Reset the outFilesGuids list since guids will be generated by this module
        self.__job.outFilesGuids = []

    def getJobWorkDir(self):
        """ Getter for workdir """

        return self.__job.workdir

    def setJobWorkDir(self, workdir):
        """ Setter for workdir """

        self.__job.workdir = workdir

    def getJobID(self):
        """ Getter for jobId """

        return self.__job.jobId

    def setJobID(self, jobId):
        """ Setter for jobId """

        self.__job.jobId = jobId

    def getJobDataDir(self):
        """ Getter for datadir """

        return self.__job.datadir

    def setJobDataDir(self, datadir):
        """ Setter for datadir """

        self.__job.datadir = datadir

    def getJobTrf(self):
        """ Getter for trf """

        return self.__job.trf

    def setJobTrf(self, trf):
        """ Setter for trf """

        self.__job.trf = trf

    def getJobResult(self):
        """ Getter for result """

        return self.__job.result

    def setJobResult(self, result):
        """ Setter for result """

        self.__job.result = result

    def getJobState(self):
        """ Getter for jobState """

        return self.__job.jobState

    def setJobState(self, jobState):
        """ Setter for jobState """

        self.__job.jobState = jobState

    def getJobStates(self):
        """ Getter for job states """

        return self.__job.result

    def setJobStates(self, states):
        """ Setter for job states """

        self.__job.result = states
        self.__job.currentState = states[0]

    def getTaskID(self):
        """ Getter for TaskID """

        return self.__taskID

    def setTaskID(self, taskID):
        """ Setter for taskID """

        self.__taskID = taskID

    def getJobOutFiles(self):
        """ Getter for outFiles """

        return self.__job.outFiles

    def setJobOutFiles(self, outFiles):
        """ Setter for outFiles """

        self.__job.outFiles = outFiles

    def getTokenExtractorInputListFilename(self):
        """ Getter for __tokenextractor_input_list_filenane """

        return self.__tokenextractor_input_list_filenane

    def setTokenExtractorInputListFilename(self, tokenextractor_input_list_filenane):
        """ Setter for __tokenextractor_input_list_filenane """

        self.__tokenextractor_input_list_filenane = tokenextractor_input_list_filenane

    def useEventIndex(self):
        """ Should the Event Index be used? """

        return self.__useEventIndex

    def setUseEventIndex(self, jobPars):
        """ Set the __useEventIndex variable to a boolean value """

        if "--createTAGFileForES" in jobPars:
            value = False
        else:
            value = True
        self.__useEventIndex = value

    # Required methods

    def __init__(self):
        """ Default initialization """

        # e.g. self.__errorLabel = errorLabel
        self.__yamplChannelName = "EventService_EventRanges"
#        self.__yamplChannelName = "EventService_EventRanges-%s" % (commands.getoutput('uuidgen'))

    # is this necessary? doesn't exist in RunJob
    def __new__(cls, *args, **kwargs):
        """ Override the __new__ method to make the class a singleton """

        if not cls.__instance:
            cls.__instance = super(RunJobEvent, cls).__new__(cls, *args, **kwargs)

        return cls.__instance

    def getRunJob(self):
        """ Return a string with the experiment name """

        return self.__runjob

    def getRunJobFileName(self):
        """ Return the filename of the module """

        return super(RunJobEvent, self).getRunJobFileName()

    def allowLoopingJobKiller(self):
        """ Should the pilot search for looping jobs? """

        # The pilot has the ability to monitor the payload work directory. If there are no updated files within a certain
        # time limit, the pilot will consider the as stuck (looping) and will kill it. The looping time limits are set
        # in environment.py (see e.g. loopingLimitDefaultProd)

        return True

    def argumentParser(self):
        """ Argument parser for the RunJob module """

        # Return variables
        appdir = None
        queuename = None
        sitename = None
        workdir = None

        parser = OptionParser()
        parser.add_option("-a", "--appdir", dest="appdir",
                          help="The local path to the applications directory", metavar="APPDIR")
        parser.add_option("-b", "--queuename", dest="queuename",
                          help="Queue name", metavar="QUEUENAME")
        parser.add_option("-d", "--workdir", dest="workdir",
                          help="The local path to the working directory of the payload", metavar="WORKDIR")
        parser.add_option("-g", "--inputdir", dest="inputDir",
                          help="Location of input files to be transferred by the mv site mover", metavar="INPUTDIR")
        parser.add_option("-i", "--logfileguid", dest="logguid",
                          help="Log file guid", metavar="GUID")
        parser.add_option("-k", "--pilotlogfilename", dest="pilotlogfilename",
                          help="The name of the pilot log file", metavar="PILOTLOGFILENAME")
        parser.add_option("-l", "--pilotinitdir", dest="pilot_initdir",
                          help="The local path to the directory where the pilot was launched", metavar="PILOT_INITDIR")
        parser.add_option("-m", "--outputdir", dest="outputDir",
                          help="Destination of output files to be transferred by the mv site mover", metavar="OUTPUTDIR")
        parser.add_option("-o", "--parentworkdir", dest="pworkdir",
                          help="Path to the work directory of the parent process (i.e. the pilot)", metavar="PWORKDIR")
        parser.add_option("-s", "--sitename", dest="sitename",
                          help="The name of the site where the job is to be run", metavar="SITENAME")
        parser.add_option("-w", "--pilotserver", dest="pilotserver",
                          help="The URL of the pilot TCP server (localhost) WILL BE RETIRED", metavar="PILOTSERVER")
        parser.add_option("-p", "--pilotport", dest="pilotport",
                          help="Pilot TCP server port (default: 88888)", metavar="PORT")
        parser.add_option("-t", "--proxycheckflag", dest="proxycheckFlag",
                          help="True (default): perform proxy validity checks, False: no check", metavar="PROXYCHECKFLAG")
        parser.add_option("-x", "--stageinretries", dest="stageinretry",
                          help="The number of stage-in retries", metavar="STAGEINRETRY")
        #parser.add_option("-B", "--filecatalogregistration", dest="fileCatalogRegistration",
        #                  help="True (default): perform file catalog registration, False: no catalog registration", metavar="FILECATALOGREGISTRATION")
        parser.add_option("-E", "--stageoutretries", dest="stageoutretry",
                          help="The number of stage-out retries", metavar="STAGEOUTRETRY")
        parser.add_option("-F", "--experiment", dest="experiment",
                          help="Current experiment (default: ATLAS)", metavar="EXPERIMENT")
        parser.add_option("-H", "--cache", dest="cache",
                          help="Cache URL", metavar="CACHE")

        # options = {'experiment': 'ATLAS'}
        try:
            (options, args) = parser.parse_args()
        except Exception,e:
            tolog("!!WARNING!!3333!! Exception caught:" % (e))
            print options.experiment
        else:

            if options.appdir:
#                self.__appdir = options.appdir
                appdir = options.appdir
            if options.experiment:
                self.__experiment = options.experiment
            if options.logguid:
                self.__logguid = options.logguid
            if options.inputDir:
                self.__inputDir = options.inputDir
            if options.pilot_initdir:
                self.__pilot_initdir = options.pilot_initdir
            if options.pilotlogfilename:
                self.__pilotlogfilename = options.pilotlogfilename
            if options.pilotserver:
                self.__pilotserver = options.pilotserver
            if options.proxycheckFlag:
                if options.proxycheckFlag.lower() == "false":
                    self.__proxycheckFlag = False
                else:
                    self.__proxycheckFlag = True
            else:
                self.__proxycheckFlag = True
            if options.pworkdir:
                self.__pworkdir = options.pworkdir
            if options.outputDir:
                self.__outputDir = options.outputDir
            if options.pilotport:
                try:
                    self.__pilotport = int(options.pilotport)
                except Exception, e:
                    tolog("!!WARNING!!3232!! Exception caught: %s" % (e))
# self.__queuename is not needed
            if options.queuename:
                queuename = options.queuename
            if options.sitename:
                sitename = options.sitename
            if options.stageinretry:
                try:
                    self.__stageinretry = int(options.stageinretry)
                except Exception, e:
                    tolog("!!WARNING!!3232!! Exception caught: %s" % (e))
            if options.stageoutretry:
                try:
                    self.__stageoutretry = int(options.stageoutretry)
                except Exception, e:
                    tolog("!!WARNING!!3232!! Exception caught: %s" % (e))
            if options.workdir:
                workdir = options.workdir
            if options.cache:
                self.__cache = options.cache

        # use sitename as queuename if queuename == ""
        if queuename == "":
            queuename = sitename

        return sitename, appdir, workdir, "", queuename # get rid of the dq2url (, "") in this return list

    def cleanup(self, rf=None):
        """ Cleanup function """
        # 'rf' is a list that will contain the names of the files that could be transferred
        # In case of transfer problems, all remaining files will be found and moved
        # to the data directory for later recovery.

        tolog("********************************************************")
        tolog(" This job ended with (trf,pilot) exit code of (%d,%d)" % (self.__job.result[1], self.__job.result[2]))
        tolog("********************************************************")

        # clean up the pilot wrapper modules
        pUtil.removePyModules(self.__job.workdir)

        if os.path.isdir(self.__job.workdir):
            os.chdir(self.__job.workdir)

            # remove input files from the job workdir
            remFiles = self.__job.inFiles
            for inf in remFiles:
                if inf and inf != 'NULL' and os.path.isfile("%s/%s" % (self.__job.workdir, inf)): # non-empty string and not NULL
                    try:
                        os.remove("%s/%s" % (self.__job.workdir, inf))
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

            try:
                copy2("%s/metadata-%s.xml" % (self.__job.workdir, self.__job.jobId), "%s/metadata-%s.xml" % (self.__pworkdir, self.__job.jobId))
            except Exception, e:
                tolog("Warning: Could not copy metadata-%s.xml to site work dir - ddm Adder problems will occure in case of job recovery" % \
                          (self.__job.jobId))

            if self.__job.result[0] == 'holding' and self.__job.result[1] == 0:
                try:
                    # create the data directory
                    os.makedirs(self.__job.datadir)
                except OSError, e:
                    tolog("!!WARNING!!3000!! Could not create data directory: %s, %s" % (self.__job.datadir, str(e)))
                else:
                    # find all remaining files in case 'rf' is not empty
                    remaining_files = []
                    moved_files_list = []
                    try:
                        if rf != None:
                            moved_files_list = RunJobUtilities.getFileNamesFromString(rf[1])
                            remaining_files = RunJobUtilities.getRemainingFiles(moved_files_list, self.__job.outFiles)
                    except Exception, e:
                        tolog("!!WARNING!!3000!! Illegal return value from Mover: %s, %s" % (str(rf), str(e)))
                        remaining_files = self.__job.outFiles

                    # move all remaining output files to the data directory
                    nr_moved = 0
                    for _file in remaining_files:
                        try:
                            os.system("mv %s %s" % (_file, self.__job.datadir))
                        except OSError, e:
                            tolog("!!WARNING!!3000!! Failed to move file %s (abort all)" % (_file))
                            break
                        else:
                            nr_moved += 1

                    tolog("Moved %d/%d output file(s) to: %s" % (nr_moved, len(remaining_files), self.__job.datadir))

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
                        _fname = os.path.join(self.__job.workdir, "PoolFileCatalog.xml")
                        tolog("Copying %s to %s" % (_fname, self.__job.datadir))
                        try:
                            copy2(_fname, self.__job.datadir)
                        except Exception, e:
                            tolog("!!WARNING!!3000!! Could not copy PoolFileCatalog.xml to data dir - expect ddm Adder problems during job recovery")

            # remove all remaining output files from the work directory
            # (a successfully copied file should already have been removed by the Mover)
            rem = False
            for inf in self.__job.outFiles:
                if inf and inf != 'NULL' and os.path.isfile("%s/%s" % (self.__job.workdir, inf)): # non-empty string and not NULL
                    try:
                        os.remove("%s/%s" % (self.__job.workdir, inf))
                    except Exception, e:
                        tolog("!!WARNING!!3000!! Ignore this Exception when deleting file %s: %s" % (inf, e))
                        pass
                    else:
                        tolog("Lingering output file removed: %s" % (inf))
                        rem = True
            if not rem:
                tolog("All output files already removed from local dir")

        tolog("Payload cleanup has finished")

    def sysExit(self, rf=None):
        '''
        wrapper around sys.exit
        rs is the return string from Mover::put_data() containing a list of files that were not transferred
        '''

        self.cleanup(rf=rf)
        sys.stderr.close()
        tolog("RunJobEvent (payload wrapper) has finished")

        # change to sys.exit?
        os._exit(self.__job.result[2]) # pilotExitCode, don't confuse this with the overall pilot exit code,
                                       # which doesn't get reported back to panda server anyway
    def failJob(self, transExitCode, pilotExitCode, job, ins=None, pilotErrorDiag=None, docleanup=True):
        """ set the fail code and exit """

        job.setState(["failed", transExitCode, pilotExitCode])
        if pilotErrorDiag:
            job.pilotErrorDiag = pilotErrorDiag
        tolog("Will now update local pilot TCP server")
        rt = RunJobUtilities.updatePilotServer(job, self.__pilotserver, self.__pilotport, final=True)
        if ins:
            ec = pUtil.removeFiles(job.workdir, ins)
        if docleanup:
            self.sysExit()

    def getTrfExitInfo(self, exitCode, workdir):
        """ Get the trf exit code and info from job report if possible """

        exitAcronym = ""
        exitMsg = ""

        # does the job report exist?
        extension = getExtension(alternative='pickle')
        if extension.lower() == "json":
            _filename = "jobReport.%s" % (extension)
        else:
            _filename = "jobReportExtract.%s" % (extension)
        filename = os.path.join(workdir, _filename)

        # first backup the jobReport to the job workdir since it will be needed later
        # (the current location will disappear since it will be tarred up in the jobs' log file)
        d = os.path.join(workdir, '..')
        try:
            copy2(filename, os.path.join(d, _filename))
        except Exception, e:
            tolog("Warning: Could not backup %s to %s: %s" % (_filename, d, e))
        else:
            tolog("Backed up %s to %s" % (_filename, d))

        # It might take a short while longer until the job report is created (unknown why)
        count = 1
        max_count = 10
        nap = 5
        found = False
        while count <= max_count:
            if os.path.exists(filename):
                tolog("Found job report: %s" % (filename))
                found = True
                break
            else:
                tolog("Waiting %d s for job report to arrive (#%d/%d)" % (nap, count, max_count))
                time.sleep(nap)
                count += 1

        if found:
            # search for the exit code
            try:
                f = open(filename, "r")
            except Exception, e:
                tolog("!!WARNING!!1112!! Failed to open job report: %s" % (e))
            else:
                if extension.lower() == "json":
                    from json import load
                else:
                    from pickle import load
                data = load(f)

                # extract the exit code and info
                _exitCode = self.extractDictionaryObject("exitCode", data)
                if _exitCode:
                    if _exitCode == 0 and exitCode != 0:
                        tolog("!!WARNING!!1111!! Detected inconsistency in %s: exitcode listed as 0 but original trf exit code was %d (using original error code)" %\
                                  (filename, exitCode))
                    else:
                        exitCode = _exitCode
                _exitAcronym = self.extractDictionaryObject("exitAcronym", data)
                if _exitAcronym:
                    exitAcronym = _exitAcronym
                _exitMsg = self.extractDictionaryObject("exitMsg", data)
                if _exitMsg:
                    exitMsg = _exitMsg

                f.close()

                tolog("Trf exited with:")
                tolog("...exitCode=%d" % (exitCode))
                tolog("...exitAcronym=%s" % (exitAcronym))
                tolog("...exitMsg=%s" % (exitMsg))

                # Ignore special trf error for now
                if (exitCode == 65 and exitAcronym == "TRF_EXEC_FAIL") or (exitCode == 68 and exitAcronym == "TRF_EXEC_LOGERROR") or (exitCode == 66 and exitAcronym == "TRF_EXEC_VALIDATION_FAIL"):
                    exitCode = 0
                    exitAcronym = ""
                    exitMsg = ""
                    tolog("!!WARNING!!3333!! Reset TRF error codes..")
        else:
            tolog("Job report not found: %s" % (filename))

        return exitCode, exitAcronym, exitMsg

    def convertToLFNs(self):
        """ Convert the output file names to LFNs """
        # Remove the file paths

        lfns = []
        for f in self.getOutputFiles():
            lfns.append(os.path.basename(f))

        return lfns

    def createFileMetadata(self, outsDict, dsname, datasetDict, sitename):
        """ create the metadata for the output + log files """

        ec = 0

        # get the file sizes and checksums for the local output files
        # WARNING: any errors are lost if occur in getOutputFileInfo()
        ec, pilotErrorDiag, fsize, checksum = pUtil.getOutputFileInfo(list(self.getOutputFiles()), getChecksumCommand(), skiplog=True, logFile=self.__job.logFile)
        if ec != 0:
            tolog("!!FAILED!!2999!! %s" % (pilotErrorDiag))
            self.failJob(self.__job.result[1], ec, self.__job, pilotErrorDiag=pilotErrorDiag)

        # Get the correct log guid (a new one is generated for the Job() object, but we need to get it from the -i logguid parameter)
        if self.__logguid:
            guid = self.__logguid
        else:
            guid = self.__job.tarFileGuid

        # Convert the output file list to LFNs
        lfns = self.convertToLFNs()

        # Create preliminary metadata (no metadata yet about log file - added later in pilot.py)
        _fname = "%s/metadata-%s.xml" % (self.__job.workdir, self.__job.jobId)
        tolog("fguids=%s"%str(self.__job.outFilesGuids))

        lfns = []
        self.__job.outFilesGuids = []
        tolog("Reset output file LFN and GUID list (pilot will not report these to the server - xml shoould only contain log file info)")

        try:
            _status = pUtil.PFCxml(self.__experiment, _fname, fnlist=lfns, fguids=self.__job.outFilesGuids, fntag="lfn", alog=self.__job.logFile, alogguid=guid,\
                                       fsize=fsize, checksum=checksum, analJob=self.__analysisJob)
        except Exception, e:
            pilotErrorDiag = "PFCxml failed due to problematic XML: %s" % (e)
            tolog("!!WARNING!!1113!! %s" % (pilotErrorDiag))
            self.failJob(self.__job.result[1], self.__error.ERR_MISSINGGUID, self.__job, pilotErrorDiag=pilotErrorDiag)
        else:
            if not _status:
                pilotErrorDiag = "Missing guid(s) for output file(s) in metadata"
                tolog("!!FAILED!!2999!! %s" % (pilotErrorDiag))
                self.failJob(self.__job.result[1], self.__error.ERR_MISSINGGUID, self.__job, pilotErrorDiag=pilotErrorDiag)

        tolog("NOTE: Output file info will not be sent to the server as part of xml metadata")
        tolog("..............................................................................................................")
        tolog("Created %s with:" % (_fname))
        tolog(".. log            : %s (to be transferred)" % (self.__job.logFile))
        tolog(".. log guid       : %s" % (guid))
        tolog(".. out files      : %s" % str(self.__job.outFiles))
        tolog(".. out file guids : %s" % str(self.__job.outFilesGuids))
        tolog(".. fsize          : %s" % str(fsize))
        tolog(".. checksum       : %s" % str(checksum))
        tolog("..............................................................................................................")

        # convert the preliminary metadata-<jobId>.xml file to OutputFiles-<jobId>.xml for NG and for CERNVM
        # note: for CERNVM this is only really needed when CoPilot is used
        if os.environ.has_key('Nordugrid_pilot') or sitename == 'CERNVM':
            if RunJobUtilities.convertMetadata4NG(os.path.join(self.__job.workdir, self.__job.outputFilesXML), _fname, outsDict, dsname, datasetDict):
                tolog("Metadata has been converted to NG/CERNVM format")
            else:
                self.__job.pilotErrorDiag = "Could not convert metadata to NG/CERNVM format"
                tolog("!!WARNING!!1999!! %s" % (self.__job.pilotErrorDiag))

        # try to build a file size and checksum dictionary for the output files
        # outputFileInfo: {'a.dat': (fsize, checksum), ...}
        # e.g.: file size for file a.dat: outputFileInfo['a.dat'][0]
        # checksum for file a.dat: outputFileInfo['a.dat'][1]
        try:
            # remove the log entries
            _fsize = fsize[1:]
            _checksum = checksum[1:]
            outputFileInfo = dict(zip(self.__job.outFiles, zip(_fsize, _checksum)))
        except Exception, e:
            tolog("!!WARNING!!2993!! Could not create output file info dictionary: %s" % str(e))
            outputFileInfo = {}
        else:
            tolog("Output file info dictionary created: %s" % str(outputFileInfo))

        return ec, outputFileInfo

    def createFileMetadata4EventRange(self, outputFile, event_range_id):
        """ Create the metadata for an output file """

        # This function will create a metadata file called metadata-<event_range_id>.xml using file info
        # from PoolFileCatalog.xml
        # Return: ec, pilotErrorDiag, outputFileInfo, fname
        #         where outputFileInfo: {'<full path>/filename.ext': (fsize, checksum, guid), ...}
        #         (dictionary is used for stage-out)
        #         fname is the name of the metadata/XML file containing the file info above

        ec = 0
        pilotErrorDiag = ""
        outputFileInfo = {}

        # Get/assign a guid to the output file
        guid = getGUID()
        if guid == "":
            ec = self.__error.ERR_UUIDGEN
            pilotErrorDiag = "uuidgen failed to produce a guid"
            tolog("!!WARNING!!2999!! %s" % (pilotErrorDiag))
            return ec, pilotErrorDiag, None

        guid_list = [guid]
        tolog("Generated GUID %s for file %s" % (guid_list[0], outputFile))

        # Add the new guid to the outFilesGuids list
        self.__job.outFilesGuids.append(guid)

        # Get the file size and checksum for the local output file
        # WARNING: any errors are lost if occur in getOutputFileInfo()
        ec, pilotErrorDiag, fsize_list, checksum_list = pUtil.getOutputFileInfo([outputFile], getChecksumCommand(), skiplog=True)
        if ec != 0:
            tolog("!!WARNING!!2999!! %s" % (pilotErrorDiag))
            return ec, pilotErrorDiag, None
        else:
            tolog("fsize = %s" % str(fsize_list))
            tolog("checksum = %s" % str(checksum_list))

        # Create the metadata
        try:
            self.setMetadataFilename(event_range_id)
            fname = self.getMetadataFilename()
            tolog("Metadata filename = %s" % (fname))
        except Exception,e:
            tolog("!!WARNING!!2222!! Caught exception: %s" % (e))

        _status = pUtil.PFCxml(self.__experiment, fname, fnlist=[outputFile], fguids=guid_list, fntag="pfn", fsize=fsize_list,\
                                   checksum=checksum_list, analJob=self.__analysisJob)
        if not _status:
            pilotErrorDiag = "Missing guid(s) for output file(s) in metadata"
            tolog("!!WARNING!!2999!! %s" % (pilotErrorDiag))
            return ec, pilotErrorDiag, None

        tolog("..............................................................................................................")
        tolog("Created %s with:" % (fname))
        tolog(".. output file      : %s" % (outputFile))
        tolog(".. output file guid : %s" % str(guid_list))
        tolog(".. fsize            : %s" % str(fsize_list))
        tolog(".. checksum         : %s" % str(checksum_list))
        tolog("..............................................................................................................")

        # Build a file size and checksum dictionary for the output file
        # outputFileInfo: {'a.dat': (fsize, checksum, guid), ...}
        # e.g.: file size for file a.dat: outputFileInfo['a.dat'][0]
        # checksum for file a.dat: outputFileInfo['a.dat'][1]
        try:
            outputFileInfo = dict(zip([outputFile], zip(fsize_list, checksum_list, guid_list)))
        except Exception, e:
            tolog("!!WARNING!!2993!! Could not create output file info dictionary: %s" % str(e))
        else:
            tolog("Output file info dictionary created: %s" % str(outputFileInfo))

        return ec, pilotErrorDiag, outputFileInfo, fname

    def getDatasets(self):
        """ Get the datasets for the output files """

        # Get the default dataset
        if self.__job.destinationDblock and self.__job.destinationDblock[0] != 'NULL' and self.__job.destinationDblock[0] != ' ':
            dsname = self.__job.destinationDblock[0]
        else:
            dsname = "%s-%s-%s" % (time.localtime()[0:3]) # pass it a random name

        # Create the dataset dictionary
        # (if None, the dsname above will be used for all output files)
        datasetDict = getDatasetDict(self.__job.outFiles, self.__job.destinationDblock, self.__job.logFile, self.__job.logDblock)
        if datasetDict:
            tolog("Dataset dictionary has been verified: %s" % str(datasetDict))
        else:
            tolog("Dataset dictionary could not be verified, output files will go to: %s" % (dsname))

        return dsname, datasetDict

    def stageOut(self, file_list, dsname, datasetDict, outputFileInfo, metadata_fname):
        """ Perform the stage-out """

        ec = 0
        pilotErrorDiag = ""

        rs = "" # return string from put_data with filename in case of transfer error
        tin_0 = os.times()
        try:
            ec, pilotErrorDiag, rf, rs, self.__job.filesNormalStageOut, self.__job.filesAltStageOut = mover.mover_put_data("xmlcatalog_file:%s" %\
                                         (metadata_fname), dsname, self.__jobSite.sitename, self.__jobSite.computingElement,\
                                         analysisJob=self.__analysisJob, pinitdir=self.__pilot_initdir, scopeOut=self.__job.scopeOut,\
                                         proxycheck=self.__proxycheckFlag, spsetup=self.__job.spsetup, token=self.__job.destinationDBlockToken,\
                                         userid=self.__job.prodUserID, datasetDict=datasetDict, prodSourceLabel=self.__job.prodSourceLabel,\
                                         outputDir=self.__outputDir, jobId=self.__job.jobId, jobWorkDir=self.__job.workdir, DN=self.__job.prodUserID,\
                                         dispatchDBlockTokenForOut=self.__job.dispatchDBlockTokenForOut, outputFileInfo=outputFileInfo,\
                                         jobDefId=self.__job.jobDefinitionID, jobCloud=self.__job.cloud,\
                                         logFile=self.__job.logFile, stageoutTries=self.__stageoutretry, experiment=self.__experiment,\
                                         fileDestinationSE=self.__job.fileDestinationSE, eventService=True, job=self.__job)
            tin_1 = os.times()
            self.__job.timeStageOut = int(round(tin_1[4] - tin_0[4]))
        except Exception, e:
            tin_1 = os.times()
            self.__job.timeStageOut = int(round(tin_1[4] - tin_0[4]))

            if 'format_exc' in traceback.__all__:
                trace = traceback.format_exc()
                pilotErrorDiag = "Put function can not be called for staging out: %s, %s" % (str(e), trace)
            else:
                tolog("traceback.format_exc() not available in this python version")
                pilotErrorDiag = "Put function can not be called for staging out: %s" % (str(e))
            tolog("!!WARNING!!3000!! %s" % (pilotErrorDiag))

            ec = self.__error.ERR_PUTFUNCNOCALL
            self.__job.setState(["holding", self.__job.result[1], ec])
        else:
            if self.__job.pilotErrorDiag != "":
                if self.__job.pilotErrorDiag.startswith("Put error:"):
                    pre = ""
                else:
                    pre = "Put error: "
                self.__job.pilotErrorDiag = pre + tailPilotErrorDiag(self.__job.pilotErrorDiag, size=256-len("pilot: Put error: "))

            tolog("Put function returned code: %d" % (ec))
            if ec != 0:
                # is the job recoverable?
                if self.__error.isRecoverableErrorCode(ec):
                    _state = "holding"
                    _msg = "WARNING"
                else:
                    _state = "failed"
                    _msg = "FAILED"
                tolog("!!%s!!1212!! %s" % (_msg, self.__error.getErrorStr(ec)))

                # set the internal error, to be picked up at the end of the job
                self.setErrorCode(ec)

        return ec, pilotErrorDiag

    def getEventRangeID(self, filename):
        """ Return the event range id for the corresponding output file """

        event_range_id = ""
        for event_range in self.__eventRange_dictionary.keys():
            if self.__eventRange_dictionary[event_range][0] == filename:
                event_range_id = event_range
                break

        return event_range_id

    def transferToObjectStore(self, outputFileInfo, metadata_fname):
        """ Transfer the output file to the object store """

        # FORMAT:  outputFileInfo = {'<full path>/filename.ext': (fsize, checksum, guid), ...}
        # Normally, the dictionary will only contain info about a single file

        ec = 0
        pilotErrorDiag = ""

        # Get the site information object
        si = getSiteInformation(self.__experiment)

        # Get the queuename - which is only needed if objectstores field is not present in queuedata
        jobSite = self.getJobSite()
        queuename = jobSite.computingElement
        si.setQueueName(queuename)

        # Extract all information from the dictionary
        for path in outputFileInfo.keys():

            fsize = outputFileInfo[path][0]
            checksum = outputFileInfo[path][1]
            guid = outputFileInfo[path][2]

            # First backup some schedconfig fields that need to be modified for the secondary transfer
            copytool_org = readpar('copytool')

            # Temporarily modify the schedconfig fields with values
            tolog("Temporarily modifying queuedata for log file transfer to secondary SE")
            ec = si.replaceQueuedataField("copytool", "objectstore")

            # needs to know source, destination, fsize=0, fchecksum=0, **pdict, from pdict: lfn, guid, logPath
            # where source is local file path and destination is not used, set to empty string

            # Get the dataset name for the output file
            dsname, datasetDict = self.getDatasets()

            # Transfer the file
            ec, pilotErrorDiag = self.stageOut([path], dsname, datasetDict, outputFileInfo, metadata_fname)
            if ec == 0:
                try:
                    # Get the OS name identifier and bucket endpoint
                    os_name = si.getObjectstoreName("eventservice")
                    os_bucket_endpoint = si.getObjectstoreBucketEndpoint("eventservice")

                    # Add the transferred file to the OS transfer file
                    addToOSTransferDictionary(os.path.basename(path), self.__pilot_initdir, os_name, os_bucket_endpoint)
                except Exception, e:
                    tolog("!!WARNING!!2121!! Caught exception: %s" % (e))
            # Finally restore the modified schedconfig fields
            tolog("Restoring queuedata fields")
            _ec = si.replaceQueuedataField("copytool", copytool_org)

        return ec, pilotErrorDiag

    def startMessageThread(self):
        """ Start the message thread """

        self.__message_thread.start()

    def stopMessageThread(self):
        """ Stop the message thread """

        self.__message_thread.stop()

    def joinMessageThread(self):
        """ Join the message thread """

        self.__message_thread.join()

    def startAsyncOutputStagerThread(self):
        """ Start the asynchronous output stager thread """

        self.__asyncOutputStager_thread.start()

    def stopAsyncOutputStagerThread(self):
        """ Stop the asynchronous output stager thread """

        self.__asyncOutputStager_thread.stop()

    def joinAsyncOutputStagerThread(self):
        """ Join the asynchronous output stager thread """

        self.__asyncOutputStager_thread.join()

    def asynchronousOutputStager(self):
        """ Transfer output files to stage-out area asynchronously """

        # Note: this is run as a thread

        tolog("Asynchronous output stager thread initiated")
        while not self.__asyncOutputStager_thread.stopped():

            if len(self.__stageout_queue) > 0:
                for f in self.__stageout_queue:
                    # Create the output file metadata (will be sent to server)
                    tolog("Preparing to stage-out file %s" % (f))
                    event_range_id = self.getEventRangeID(f)
                    if event_range_id == "":
                        tolog("!!WARNING!!1111!! Did not find the event range for file %s in the event range dictionary" % (f))
                    else:
                        tolog("Creating metadata for file %s and event range id %s" % (f, event_range_id))
                        ec, pilotErrorDiag, outputFileInfo, metadata_fname = self.createFileMetadata4EventRange(f, event_range_id)
                        if ec == 0:
                            try:
                                ec, pilotErrorDiag = self.transferToObjectStore(outputFileInfo, metadata_fname)
                            except Exception, e:
                                tolog("!!WARNING!!2222!! Caught exception: %s" % (e))
                            else:
                                tolog("Removing %s from stage-out queue" % (f))
                                self.__stageout_queue.remove(f)
                                tolog("Adding %s to output file list" % (f))
                                self.__output_files.append(f)
                                tolog("output_files = %s" % (self.__output_files))
                                if ec == 0:
                                    status = 'finished'
                                else:
                                    status = 'failed'

                                    # Update the global status field in case of failure
                                    self.setStatus(False)

                                    # Note: the rec pilot must update the server appropriately

                                # Time to update the server
                                msg = updateEventRange(event_range_id, self.__eventRange_dictionary[event_range_id], status=status)

                        else:
                            tolog("!!WARNING!!1112!! Failed to create file metadata: %d, %s" % (ec, pilotErrorDiag))
            time.sleep(1)

        tolog("Asynchronous output stager thread has been stopped")

    def listener(self):
        """ Listen for messages """

        # Note: this is run as a thread

        # Listen for messages as long as the thread is not stopped
        while not self.__message_thread.stopped():

            try:
                # Receive a message
                tolog("Waiting for a new message")
                size, buf = self.__message_server.receive()
                while size == -1 and not self.__message_thread.stopped():
                    time.sleep(1)
                    size, buf = self.__message_server.receive()
                tolog("Received new message: %s" % (buf))

                max_wait = 600
                i = 0
                if self.__sending_event_range:
                    tolog("Will wait for current event range to finish being sent (pilot not yet ready to process new request)")
                while self.__sending_event_range:
                    # Wait until previous send event range has completed (to avoid racing condition), but wait maximum 60 seconds then fail job
                    time.sleep(0.1)
                    if i > max_wait:
                        # Abort with error
                        buf = "ERR_FATAL_STUCK_SENDING %s: Stuck sending event range to payload; new message: %s" % (self.__current_event_range, buf)
                        break
                    i += 1
                if i > 0:
                    tolog("Delayed %d s for send message to complete" % (i*10))

#                if not "Ready for" in buf:
#                    if self.__eventRangeID_dictionary.keys():
#                        try:
#                            keys = self.__eventRangeID_dictionary.keys()
#                            key = keys[0]
#                            tolog("Faking error for range = %s" % (key))
#                            buf = "ERR_TE_RANGE %s: Range contains wrong positional number 5001" % (key)
#                        except Exception,e:
#                            tolog("No event ranges yet:%s" % (e))
#                    #buf = "ERR_TE_FATAL Range-2: CURL curl_easy_perform() failed! Couldn't resolve host name"
#                    #buf = "ERR_TE_FATAL 5211313-2452346274-2058479689-3-8: URL No tokens for GUID 00224B03-8005-E849-BCD5-D8F8F764B630"

                # Interpret the message and take the appropriate action
                if "Ready for events" in buf:
                    buf = ""
                    tolog("AthenaMP is ready for events")
                    self.__athenamp_is_ready = True

                elif buf.startswith('/'):
                    tolog("Received file and process info from client: %s" % (buf))

                    # Extract the information from the message
                    path, event_range_id, cpu, wall = self.interpretMessage(buf)
                    if path not in self.__stageout_queue and path != "":
                        # Correct the output file name if necessary
                        # path = correctFileName(path, event_range_id)

                        # Add the extracted info to the event range dictionary
                        self.__eventRange_dictionary[event_range_id] = [path, cpu, wall]

                        # Add the file to the stage-out queue
                        self.__stageout_queue.append(path)
                        tolog("File %s has been added to the stage-out queue (length = %d)" % (path, len(self.__stageout_queue)))

                elif buf.startswith('ERR'):
                    tolog("Received an error message: %s" % (buf))

                    # Extract the error acronym and the error diagnostics
                    error_acronym, event_range_id, error_diagnostics = self.extractErrorMessage(buf)
                    if event_range_id != "":
                        tolog("!!WARNING!!2144!! Extracted error acronym %s and error diagnostics \'%s\' for event range %s" % (error_acronym, error_diagnostics, event_range_id))

                        # Time to update the server
                        msg = updateEventRange(event_range_id, [], status='failed')
                        if msg != "":
                            tolog("!!WARNING!!2145!! Problem with updating event range: %s" % (msg))
                        else:
                            tolog("Updated server for failed event range")

                        # Was the error fatal? If so, the pilot should abort
                        if "FATAL" in error_acronym:
                            tolog("!!WARNING!!2146!! A FATAL error was encountered, prepare to finish")

                            # Fail the job
                            if error_acronym == "ERR_TE_FATAL" and "URL Error" in error_diagnostics:
                                error_code = self.__error.ERR_TEBADURL
                            elif error_acronym == "ERR_TE_FATAL" and "resolve host name" in error_diagnostics:
                                error_code = self.__error.ERR_TEHOSTNAME
                            elif error_acronym == "ERR_TE_FATAL" and "Invalid GUID length" in error_diagnostics:
                                error_code = self.__error.ERR_TEINVALIDGUID
                            elif error_acronym == "ERR_TE_FATAL" and "No tokens for GUID" in error_diagnostics:
                                error_code = self.__error.ERR_TEWRONGGUID
                            elif error_acronym == "ERR_TE_FATAL":
                                error_code = self.__error.ERR_TEFATAL
                            else:
                                error_code = self.__error.ERR_ESFATAL
                            result = ["failed", 0, error_code]
                            tolog("Setting error code: %d" % (error_code))
                            self.setJobResult(result)

                            # ..

                    else:
                        tolog("!!WARNING!!2245!! Extracted error acronym %s and error diagnostics \'%s\' (event range could not be extracted - cannot update server)" % (error_acronym, error_diagnostics))

                else:
                    tolog("Pilot received message:%s" % buf)
            except Exception, e:
                tolog("Caught exception:%s" % e)
            time.sleep(1)

        tolog("listener has finished")

    def extractErrorMessage(self, msg):
        """ Extract the error message from the AthenaMP message """

        # msg = 'ERR_ATHENAMP_PROCESS 130-2068634812-21368-1-4: Failed to process event range'
        # -> error_acronym = 'ERR_ATHENAMP_PROCESS'
        #    event_range_id = '130-2068634812-21368-1-4'
        #    error_diagnostics = 'Failed to process event range')
        #
        # msg = ERR_ATHENAMP_PARSE "u'LFN': u'mu_E50_eta0-25.evgen.pool.root',u'eventRangeID': u'130-2068634812-21368-1-4', u'startEvent': 5, u'GUID': u'74DFB3ED-DAA7-E011-8954-001E4F3D9CB1'": Wrong format
        # -> error_acronym = 'ERR_ATHENAMP_PARSE'
        #    event_range = "u'LFN': u'mu_E50_eta0-25.evgen.pool.root',u'eventRangeID': u'130-2068634812-21368-1-4', ..
        #    error_diagnostics = 'Wrong format'
        #    -> event_range_id = '130-2068634812-21368-1-4' (if possible to extract)

        error_acronym = ""
        event_range_id = ""
        error_diagnostics = ""

        # Special error acronym
        if "ERR_ATHENAMP_PARSE" in msg:
            # Note: the event range will be in the msg and not the event range id only
            pattern = re.compile(r"(ERR\_[A-Z\_]+)\ (.+)\:\ ?(.+)")
            found = re.findall(pattern, msg)
            if len(found) > 0:
                try:
                    error_acronym = found[0][0]
                    event_range = found[0][1] # Note: not the event range id only, but the full event range
                    error_diagnostics = found[0][2]
                except Exception, e:
                    tolog("!!WARNING!!2211!! Failed to extract AthenaMP message: %s" % (e))
                    error_acronym = "EXTRACTION_FAILURE"
                    error_diagnostics = e
                else:
                    # Can the event range id be extracted?
                    if "eventRangeID" in event_range:
                        pattern = re.compile(r"eventRangeID\'\:\ ?.?\'([0-9\-]+)")
                        found = re.findall(pattern, event_range)
                        if len(found) > 0:
                            try:
                                event_range_id = found[0]
                            except Exception, e:
                                tolog("!!WARNING!!2212!! Failed to extract event_range_id: %s" % (e))
                            else:
                                tolog("Extracted event_range_id: %s" % (event_range_id))
                    else:
                        tolog("!!WARNING!!2213!1 event_range_id not found in event_range: %s" % (event_range))
        else:
            # General error acronym
            pattern = re.compile(r"(ERR\_[A-Z\_]+)\ ([0-9\-]+)\:\ ?(.+)")
            found = re.findall(pattern, msg)
            if len(found) > 0:
                try:
                    error_acronym = found[0][0]
                    event_range_id = found[0][1]
                    error_diagnostics = found[0][2]
                except Exception, e:
                    tolog("!!WARNING!!2211!! Failed to extract AthenaMP message: %s" % (e))
                    error_acronym = "EXTRACTION_FAILURE"
                    error_diagnostics = e
            else:
                tolog("!!WARNING!!2212!! Failed to extract AthenaMP message")
                error_acronym = "EXTRACTION_FAILURE"
                error_diagnostics = msg

        return error_acronym, event_range_id, error_diagnostics

    def correctFileName(self, path, event_range_id):
        """ Correct the output file name if necessary """

        # Make sure the output file name follows the format OUTPUT_FILENAME_FROM_JOBDEF.EVENT_RANGE_ID

        outputFileName = self.__job.outFiles[0]
        if outputFileName != "":
            fname = os.path.basename(path)
            dirname = os.path.dirname(path)

            constructedFileName = outputFileName + "." + event_range_id
            if fname == constructedFileName:
                tolog("Output file name verified")
            else:
                tolog("Output file name does not follow convension: OUTPUT_FILENAME_FROM_JOBDEF.EVENT_RANGE_ID: %s" % (fname))
                fname = constructedFileName
                _path = os.path.join(dirname, fname)
                cmd = "mv %s %s" % (path, _path)
                out = commands.getoutput(cmd)
                path = _path
                tolog("Corrected output file name: %s" % (path))

        return path

    def interpretMessage(self, msg):
        """ Interpret a message containing file and processing info """

        # The message is assumed to have the following format
        # Format: "<file_path>,<event_range_id>,CPU:<number_in_sec>,WALL:<number_in_sec>"
        # Return: path, event_range_id, cpu time (s), wall time (s)

        path = ""
        event_range_id = ""
        cpu = ""
        wall = ""

        if "," in msg:
            message = msg.split(",")

            try:
                path = message[0]
            except:
                tolog("!!WARNING!!1100!! Failed to extract file path from message: %s" % (msg))

            try:
                event_range_id = message[1]
            except:
                tolog("!!WARNING!!1101!! Failed to extract event range id from message: %s" % (msg))

            try:
                # CPU:<number_in_sec>
                _cpu = message[2]
                cpu = _cpu.split(":")[1]
            except:
                tolog("!!WARNING!!1102!! Failed to extract CPU time from message: %s" % (msg))

            try:
                # WALL:<number_in_sec>
                _wall = message[3]
                wall = _wall.split(":")[1]
            except:
                tolog("!!WARNING!!1103!! Failed to extract wall time from message: %s" % (msg))

        else:
            tolog("!!WARNING!!1122!! Unknown message format: missing commas: %s" % (msg))

        return path, event_range_id, cpu, wall

    def getTokenExtractorInputListEntry(self, input_file_guid, input_filename):
        """ Prepare the guid and filename string for the token extractor file with the proper format """

        return "%s,PFN:%s\n" % (input_file_guid.upper(), input_filename)

    def getTokenExtractorProcess(self, thisExperiment, setup, input_file, input_file_guid, stdout=None, stderr=None, url=""):
        """ Execute the TokenExtractor """

        options = ""

        # Should the event index be used or should a tag file be used?
        if url == "" and self.__useEventIndex:
            tolog("!!WARNING!!5656!! Event index URL not specified (switching off event index mode)")
            self.__useEventIndex = False

        if not self.__useEventIndex:
            # In this case, the input file is the tag file
            # First create a file with format: <guid>,PFN:<input_tag_file>
            filename = os.path.join(os.getcwd(), "tokenextractor_input_list.txt")
            self.setTokenExtractorInputListFilename(filename) # needed later when we add the files from the event ranges
            s = self.getTokenExtractorInputListEntry(input_file_guid, input_file)
            status = writeToFileWithStatus(filename, s)

            # Define the options
            options += "-v --source %s" % (filename)

        else:
            # In this case the input file is an EVT file
            # Define the options
            options = '-v -e -s \"%s\"' % (url)

        # Define the command
        cmd = "%s TokenExtractor %s" % (setup, options)

        # Execute and return the TokenExtractor subprocess object
        return self.getSubprocess(thisExperiment, cmd, stdout=stdout, stderr=stderr)

    def createMessageServer(self):
        """ Create the message server socket object """

        status = False

        # Create the server socket
        if MessageServer:
            self.__message_server = MessageServer(socketname=self.__yamplChannelName, context='local')

            # is the server alive?
            if not self.__message_server.alive():
                # destroy the object
                tolog("!!WARNING!!3333!! Message server is not alive")
                self.__message_server = None
            else:
                status = True
        else:
            tolog("!!WARNING!!3333!! MessageServer object is not available")

        return status

    def getTAGFileInfo(self, inFiles, guids):
        """ Extract the TAG file from the input files list """

        # Note: assume that there is only one TAG file
        tag_file = ""
        guid = ""
        i = -1

        if len(inFiles) == len(guids):
            for f in inFiles:
                i += 1
                if ".TAG." in f:
                    tag_file = f
                    break
            i = -1
            for f in inFiles:
                i += 1
                if not ".TAG." in f: # fix this, just added 'not' to get theother guid - won't work of course in thelong run
                    guid = guids[i]
                    break
        else:
            tolog("!!WARNING!!2121!! Input file list not same length as guid list")

        return tag_file, guid

    def sendMessage(self, message):
        """ Send a message """

        # Filter away unwanted fields
        if "scope" in message:
            # First replace an ' with " since loads() cannot handle ' signs properly
            # Then convert to a list and get the 0th element (there should be only one)
            try:
                #_msg = loads(message.replace("'",'"'))[0]
                _msg = loads(message.replace("'",'"').replace('u"','"'))[0]
            except Exception, e:
                tolog("!!WARNING!!2233!! Caught exception: %s" % (e))
            else:
                # _msg = {u'eventRangeID': u'79-2161071668-11456-1011-1', u'LFN': u'EVNT.01461041._000001.pool.root.1', u'lastEvent': 1020, u'startEvent': 1011, u'scope': u'mc12_8TeV', u'GUID': u'BABC9918-743B-C742-9049-FC3DCC8DD774'}
                # Now remove the "scope" key/value
                scope = _msg.pop("scope")
                # Convert back to a string
                message = str([_msg])

        self.__message_server.send(message)
        tolog("Sent %s" % (message))

    def getPoolFileCatalog(self, dsname, tokens, workdir, dbh, DBReleaseIsAvailable,\
                               scope_dict, filesizeIn, checksumIn, thisExperiment=None, inFilesGuids=None, lfnList=None, ddmEndPointIn=None):
        """ Wrapper function for the actual getPoolFileCatalog function in Mover """

        # This function is a wrapper to the actual getPoolFileCatalog() in Mover, but also contains SURL to TURL conversion

        file_info_dictionary = {}

        from SiteMover import SiteMover
        sitemover = SiteMover()

        # Is the inFilesGuids list populated (ie the case of the initial PFC creation) or
        # should the __guid_list be used (ie for files downloaded via server messages)?
        # (same logic for lfnList)
        if not inFilesGuids:
            inFilesGuids = self.__guid_list
        if not lfnList:
            lfnList = self.__lfn_list

        # Create the PFC
        ec, pilotErrorDiag, xml_from_PFC, xml_source, replicas_dic, surl_filetype_dictionary, copytool_dictionary = mover.getPoolFileCatalog("", inFilesGuids, lfnList, self.__pilot_initdir,\
                                                                                                  self.__analysisJob, tokens, workdir, dbh,\
                                                                                                  DBReleaseIsAvailable, scope_dict, filesizeIn, checksumIn,\
                                                                                                  sitemover, thisExperiment=thisExperiment, ddmEndPointIn=ddmEndPointIn,\
                                                                                                  pfc_name=self.getPoolFileCatalogPath())
        if ec != 0:
            tolog("!!WARNING!!2222!! %s" % (pilotErrorDiag))
        else:
            # Create the file dictionaries needed for the TURL conversion
            file_nr = 0
            fileInfoDic = {}
            dsdict = {}
            xmldoc = minidom.parseString(xml_from_PFC)
            fileList = xmldoc.getElementsByTagName("File")
            for thisfile in fileList: # note that there should only ever be one file
                surl = str(thisfile.getElementsByTagName("pfn")[0].getAttribute("name"))
                guid = inFilesGuids[file_nr]
#                guid = self.__guid_list[file_nr]
                # Fill the file info dictionary (ignore the file size and checksum values since they are irrelevant for the TURL conversion - set to 0)
                fileInfoDic[file_nr] = (guid, surl, 0, 0)
                if not dsdict.has_key(dsname): dsdict[dsname] = []
                dsdict[dsname].append(os.path.basename(surl))
                file_nr += 1

            transferType = ""
            sitename = ""
            usect = False
            eventService = True

            # Create a TURL based PFC
            tokens_dictionary = {} # not needed here, so set it to an empty dictionary
            ec, pilotErrorDiag, createdPFCTURL, usect = mover.PFC4TURLs(self.__analysisJob, transferType, fileInfoDic, self.getPoolFileCatalogPath(),\
                                                                            sitemover, sitename, usect, dsdict, eventService, tokens_dictionary, sitename, "", lfnList, scope_dict)
            if ec != 0:
                tolog("!!WARNING!!2222!! %s" % (pilotErrorDiag))

            # Finally return the TURL based PFC
            if ec == 0:
                file_info_dictionary = mover.getFileInfoDictionaryFromXML(self.getPoolFileCatalogPath())

        return ec, pilotErrorDiag, file_info_dictionary

    def createPoolFileCatalog(self, inFiles, scopeIn, inFilesGuids, tokens, filesizeIn, checksumIn, thisExperiment, workdir, ddmEndPointIn):
        """ Create the Pool File Catalog """

        # Note: this method is only used for the initial PFC needed to start AthenaMP

        # Create the scope dictionary
        scope_dict = {}
        n = 0
        for lfn in inFiles:
            scope_dict[lfn] = scopeIn[n]
            n += 1

        # set the guids for the input files
#        self.

        tolog("Using scope dictionary for initial PFC: %s" % str(scope_dict))

        dsname = 'dummy_dsname' # not used by getPoolFileCatalog()
        dbh = None
        DBReleaseIsAvailable = False

        self.setPoolFileCatalogPath(os.path.join(workdir, "PFC.xml"))
        tolog("Using PFC path: %s" % (self.getPoolFileCatalogPath()))

        # Get the TURL based PFC
        ec, pilotErrorDiag, file_info_dictionary = self.getPoolFileCatalog(dsname, tokens, workdir, dbh, DBReleaseIsAvailable, scope_dict,\
                                                           filesizeIn, checksumIn, thisExperiment=thisExperiment, inFilesGuids=inFilesGuids, lfnList=inFiles, ddmEndPointIn=ddmEndPointIn)
        if ec != 0:
            tolog("!!WARNING!!2222!! %s" % (pilotErrorDiag))

        return ec, pilotErrorDiag, file_info_dictionary

    def createPoolFileCatalogFromMessage(self, message, thisExperiment):
        """ Prepare and create the PFC using file/guid info from the event range message """

        # Note: the PFC created by this function will only contain a single LFN
        # while the intial PFC can contain multiple LFNs

        # WARNING!!!!!!!!!!!!!!!!!!!!!!
        # Consider rewrite: this function should append an entry into the xml, not replace the entire xml file

        ec = 0
        pilotErrorDiag = ""
        file_info_dictionary = {}

        # Reset the guid and lfn lists
#        self.__guid_list = []
#        self.__lfn_list = []

        if not "No more events" in message:
            # Convert string to list
            msg = loads(message)

            # Get the LFN and GUID (there is only one LFN/GUID per event range)
            try:
                # Must convert unicode strings to normal strings or the catalog lookups will fail
                lfn = str(msg[0]['LFN'])
                guid = str(msg[0]['GUID'])
                scope = str(msg[0]['scope'])
            except Exception, e:
                ec = -1
                pilotErrorDiag = "Failed to extract LFN from event range: %s" % (e)
                tolog("!!WARNING!!3434!! %s" % (pilotErrorDiag))
            else:
                # Has the file already been used? (If so, the PFC already exists)
                if guid in self.__guid_list:
                    tolog("PFC for GUID in downloaded event range has already been created")
                else:
                    self.__guid_list.append(guid)
                    self.__lfn_list.append(lfn)

                    tolog("Updating PFC for lfn=%s, guid=%s, scope=%s" % (lfn, guid, scope))

                    # Create the PFC (includes replica lookup over multiple catalogs)
                    scope_dict = { lfn : scope }
                    tokens = ['NULL']
                    filesizeIn = ['']
                    checksumIn = ['']
                    ddmEndPointIn = ['']
                    dsname = 'dummy_dsname' # not used by getPoolFileCatalog()
                    workdir = os.getcwd()
                    dbh = None
                    DBReleaseIsAvailable = False

                    ec, pilotErrorDiag, file_info_dictionary = self.getPoolFileCatalog(dsname, tokens, workdir, dbh, DBReleaseIsAvailable,\
                                                                              scope_dict, filesizeIn, checksumIn, thisExperiment=thisExperiment, ddmEndPointIn=ddmEndPointIn)
                    if ec != 0:
                        tolog("!!WARNING!!2222!! %s" % (pilotErrorDiag))

        return ec, pilotErrorDiag, file_info_dictionary

    def getEventRangeFilesDictionary(self, event_ranges, eventRangeFilesDictionary):
        """ Build and return the event ranges dictionary out of the event_ranges dictinoary """

        # Format: eventRangeFilesDictionary = { guid: [lfn, is_added_to_token_extractor_file_list (boolean)], .. }
        for event_range in event_ranges:
            guid = event_range['GUID']
            lfn = event_range['LFN']
            if not eventRangeFilesDictionary.has_key(guid):
                eventRangeFilesDictionary[guid] = [lfn, False]

        return eventRangeFilesDictionary

    def updateTokenExtractorInputFile(self, eventRangeFilesDictionary, input_tag_file):
        """ Add the new file info to the token extractor file list """

        for guid in eventRangeFilesDictionary.keys():
            lfn = input_tag_file #eventRangeFilesDictionary[guid][0]
            already_added = eventRangeFilesDictionary[guid][1]
            if not already_added:
                s = self.getTokenExtractorInputListEntry(guid, lfn)
                filename = self.getTokenExtractorInputListFilename()
                status = writeToFileWithStatus(filename, s, attribute='a')
                if not status:
                    tolog("!!WARNING!!2233!! Failed to update %s" % (filename))
                else:
                    eventRangeFilesDictionary[guid][1] = True

        return eventRangeFilesDictionary

    def extractEventRangeIDs(self, event_ranges):
        """ Extract the eventRangeID's from the event ranges """

        eventRangeIDs = []
        for event_range in event_ranges:
            eventRangeIDs.append(event_range['eventRangeID'])

        return eventRangeIDs

    def areAllOutputFilesTransferred(self):
        """ Verify whether all files have been staged out or not """

        status = True
        for eventRangeID in self.__eventRangeID_dictionary.keys():
            if self.__eventRangeID_dictionary[eventRangeID] == False:
                status = False
                break

        return status

    def addEventRangeIDsToDictionary(self, currentEventRangeIDs):
        """ Add the latest eventRangeIDs list to the total event range id dictionary """

        # The eventRangeID_dictionary is used to keep track of which output files have been returned from AthenaMP
        # (eventRangeID_dictionary[eventRangeID] = False means that the corresponding output file has not been created/transferred yet)
        # This is necessary since otherwise the pilot will not know what has been processed completely when the "No more events"
        # message arrives from the server

        for eventRangeID in currentEventRangeIDs:
            if not self.__eventRangeID_dictionary.has_key(eventRangeID):
                self.__eventRangeID_dictionary[eventRangeID] = False

    def getProperInputFileName(self, input_files):
        """ Return the first non TAG file name in the input file list """

        # AthenaMP needs to know the name of an input file to be able to start
        # Currently an Event Service job also has a TAG file in the input file list
        # but AthenaMP cannot start with that file, so identify the proper name and return it

        filename = ""
        for f in input_files:
            if ".TAG." in f:
                continue
            else:
                filename = f
                break

        return filename


# main process starts here
if __name__ == "__main__":

    tolog("Starting RunJobEvent")

    if not os.environ.has_key('PilotHomeDir'):
        os.environ['PilotHomeDir'] = os.getcwd()

    # Get error handler
    error = PilotErrors()

    # Get runJob object
    runJob = RunJobEvent()

    # Define a new parent group
    os.setpgrp()

    # Protect the runEvent code with exception handling
    hP_ret = False
    try:
        # always use this filename as the new jobDef module name
        import newJobDef

        jobSite = Site.Site()
        jobSite.setSiteInfo(runJob.argumentParser())

        # Reassign workdir for this job
        jobSite.workdir = jobSite.wntmpdir

        # Done with setting jobSite data members, not save the object so that the runJob methods have access to it
        runJob.setJobSite(jobSite)

        tolog("runJob.getPilotLogFilename=%s"%runJob.getPilotLogFilename())
        if runJob.getPilotLogFilename() != "":
            pUtil.setPilotlogFilename(runJob.getPilotLogFilename())

        # Set node info
        node = Node.Node()
        node.setNodeName(os.uname()[1])
        node.collectWNInfo(jobSite.workdir)

        # Redirect stderr
        sys.stderr = open("%s/runevent.stderr" % (jobSite.workdir), "w")

        tolog("Current job workdir is: %s" % os.getcwd())
        tolog("Site workdir is: %s" % jobSite.workdir)

        # Get the experiment object
        thisExperiment = getExperiment(runJob.getExperiment())
        tolog("runEvent will serve experiment: %s" % (thisExperiment.getExperiment()))

        # Get the event service object using the experiment name (since it can be experiment specific)
        thisEventService = getEventService(runJob.getExperiment())

        JR = JobRecovery()
        try:
            job = Job.Job()
            job.setJobDef(newJobDef.job)
            job.workdir = jobSite.workdir
            job.experiment = runJob.getExperiment()
            # figure out and set payload file names
            job.setPayloadName(thisExperiment.getPayloadName(job))
            # reset the default job output file list which is anyway not correct
            job.outFiles = []
            runJob.setOutputFiles(job.outFiles)
        except Exception, e:
            pilotErrorDiag = "Failed to process job info: %s" % str(e)
            tolog("!!WARNING!!3000!! %s" % (pilotErrorDiag))
            job.failJob(0, error.ERR_UNKNOWN, job, pilotErrorDiag=pilotErrorDiag)
        runJob.setJob(job)

        # Should the Event Index be used?
        runJob.setUseEventIndex(job.jobPars)

        # Set the taskID
        runJob.setTaskID(job.taskID)
        tolog("taskID = %s" % (runJob.getTaskID()))

        # Prepare for the output file data directory
        # (will only created for jobs that end up in a 'holding' state)
        runJob.setJobDataDir(runJob.getParentWorkDir() + "/PandaJob_%s_data" % (job.jobId))

        # Register cleanup function
        atexit.register(runJob.cleanup, job)

        # To trigger an exception so that the SIGTERM signal can trigger cleanup function to run
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
            runJob.setFailureCode(runJob.getGlobalErrorCode())
            # print to stderr
            print >> sys.stderr, runJob.getGlobalPilotErrorDiag()
            raise SystemError(sig)

        signal.signal(signal.SIGTERM, sig2exc)
        signal.signal(signal.SIGQUIT, sig2exc)
        signal.signal(signal.SIGSEGV, sig2exc)
        signal.signal(signal.SIGXCPU, sig2exc)
        signal.signal(signal.SIGUSR1, sig2exc)
        signal.signal(signal.SIGBUS, sig2exc)

        # See if it's an analysis job or not
        trf = runJob.getJobTrf()
        analysisJob = isAnalysisJob(trf.split(",")[0])
        runJob.setAnalysisJob(analysisJob)

        # Create a message server object (global message_server)
        if runJob.createMessageServer():
            tolog("The message server is alive")
        else:
            pilotErrorDiag = "The message server could not be created, cannot continue"
            tolog("!!WARNING!!1111!! %s" % (pilotErrorDiag))
            runJob.failJob(0, job.result[2], job, pilotErrorDiag=pilotErrorDiag)

        # Setup starts here ................................................................................

        # Update the job state file
        job.jobState = "setup"
        runJob.setJobState(job.jobState)
        _retjs = JR.updateJobStateTest(runJob.getJob(), jobSite, node, mode="test")

        # Send [especially] the process group back to the pilot
        job.setState([job.jobState, 0, 0])
        runJob.setJobState(job.result)
        rt = RunJobUtilities.updatePilotServer(job, runJob.getPilotServer(), runJob.getPilotPort())

        # Prepare the setup and get the run command list
        ec, runCommandList, job, multi_trf = runJob.setup(job, jobSite, thisExperiment)
        if ec != 0:
            tolog("!!WARNING!!2999!! runJob setup failed: %s" % (job.pilotErrorDiag))
            runJob.failJob(0, ec, job, pilotErrorDiag=job.pilotErrorDiag)
        tolog("Setup has finished successfully")
        runJob.setJob(job)

        # Job has been updated, display it again
        job.displayJob()

        # Stage-in .........................................................................................

        # Update the job state
        tolog("Setting stage-in state until all input files have been copied")
        job.jobState = "stagein"
        job.setState([job.jobState, 0, 0])
        runJob.setJobState(job.jobState)
        _retjs = JR.updateJobStateTest(job, jobSite, node, mode="test")
        rt = RunJobUtilities.updatePilotServer(job, runJob.getPilotServer(), runJob.getPilotPort())

        # Update copysetup[in] for production jobs if brokerage has decided that remote I/O should be used
        if job.transferType == 'direct':
            tolog('Brokerage has set transfer type to \"%s\" (remote I/O will be attempted for input files, any special access mode will be ignored)' %\
                  (job.transferType))
            RunJobUtilities.updateCopysetups('', transferType=job.transferType)

        # Stage-in all input files (if necessary)
        job, ins, statusPFCTurl, usedFAXandDirectIO = runJob.stageIn(job, jobSite, analysisJob, pfc_name="PFC.xml")
        if job.result[2] != 0:
            tolog("Failing job with ec: %d" % (ec))
            runJob.failJob(0, job.result[2], job, ins=ins, pilotErrorDiag=job.pilotErrorDiag)
        runJob.setJob(job)

        # after stageIn, all file transfer modes are known (copy_to_scratch, file_stager, remote_io)
        # consult the FileState file dictionary if cmd3 should be updated (--directIn should not be set if all
        # remote_io modes have been changed to copy_to_scratch as can happen with ByteStream files)
        # and update the run command list if necessary.
        # in addition to the above, if FAX is used as a primary site mover and direct access is enabled, then
        # the run command should not contain the --oldPrefix, --newPrefix, --lfcHost options but use --usePFCTurl
        if job.inFiles != ['']:
            hasInput = True
        else:
            hasInput = False
        runCommandList = RunJobUtilities.updateRunCommandList(runCommandList, runJob.getParentWorkDir(), job.jobId, statusPFCTurl, analysisJob, usedFAXandDirectIO, hasInput)

        # (stage-in ends here) .............................................................................

        # Prepare XML for input files to be read by the Event Server

        # runEvent determines the physical file replica(s) to be used as the source for input event data
        # It determines this from the input dataset/file info provided in the PanDA job spec

        # threading starts here ............................................................................

        # update the job state file
        job.jobState = "running"
        runJob.setJobState(job.jobState)
        job.setState([job.jobState, 0, 0])
        _retjs = JR.updateJobStateTest(job, jobSite, node, mode="test")
        rt = RunJobUtilities.updatePilotServer(job, runJob.getPilotServer(), runJob.getPilotPort())

        event_loop_running = True
        payload_running = False

        # Create and start the stage-out thread which will run in an infinite loop until it is stopped
        asyncOutputStager_thread = StoppableThread(name='asynchronousOutputStager', target=runJob.asynchronousOutputStager)
#        asyncOutputStager_thread.start()
        runJob.setAsyncOutputStagerThread(asyncOutputStager_thread)
        runJob.startAsyncOutputStagerThread()

        # Create and start the message listener thread
        message_thread = StoppableThread(name='listener', target=runJob.listener)
#        message_thread.start()
        runJob.setMessageThread(message_thread)
        runJob.startMessageThread()

        # Stdout/err file objects
        tokenextractor_stdout = None
        tokenextractor_stderr = None
        athenamp_stdout = None
        athenamp_stderr = None

        # Create and start the TokenExtractor

        # Extract the proper setup string from the run command
        setupString = thisEventService.extractSetup(runCommandList[0], job.trf)
        tolog("The Token Extractor will be setup using: %s" % (setupString))

        # Create the file objects
        tokenextractor_stdout, tokenextractor_stderr = runJob.getStdoutStderrFileObjects(stdoutName="tokenextractor_stdout.txt", stderrName="tokenextractor_stderr.txt")

        # In case the event index is not to be used, we need to create a TAG file
        if not runJob.useEventIndex():
            input_file, input_file_guid = runJob.createTAGFile(runCommandList[0], job.trf, job.inFiles, "MakeRunEventCollection.py")

            if input_file == "" or input_file_guid == "":
                pilotErrorDiag = "Required TAG file/guid could not be identified"
                tolog("!!WARNING!!1111!! %s" % (pilotErrorDiag))

                # Stop threads
                runJob.stopAsyncOutputStagerThread()
                runJob.joinAsyncOutputStagerThread()
                runJob.stopMessageThread()
                runJob.joinMessageThread()

                # Set error code
                job.result[0] = "failed"
                job.result[2] = error.ERR_ESRECOVERABLE
                runJob.failJob(0, job.result[2], job, pilotErrorDiag=pilotErrorDiag)

        else:
            input_file = job.inFiles[0]
            input_file_guid = job.inFilesGuids[0]

        # Get the Token Extractor command
        tolog("Will use input file %s for the TokenExtractor" % (input_file))
        tokenExtractorProcess = runJob.getTokenExtractorProcess(thisExperiment, setupString, input_file, input_file_guid,\
                                                                    stdout=tokenextractor_stdout, stderr=tokenextractor_stderr,\
                                                                    url=thisEventService.getEventIndexURL())

        # Create the file objects
        athenamp_stdout, athenamp_stderr = runJob.getStdoutStderrFileObjects(stdoutName="athena_stdout.txt", stderrName="athena_stderr.txt")

        # Remove the 1>.. 2>.. bit from the command string (not needed since Popen will handle the streams)
        if " 1>" in runCommandList[0] and " 2>" in runCommandList[0]:
            runCommandList[0] = runCommandList[0][:runCommandList[0].find(' 1>')]

        # AthenaMP needs the PFC when it is launched (initial PFC using info from job definition)
        # The returned file info dictionary contains the TURL for the input file. AthenaMP needs to know the full path for the --inputEvgenFile option
        ec, pilotErrorDiag, file_info_dictionary = runJob.createPoolFileCatalog(job.inFiles, job.scopeIn, job.inFilesGuids, job.prodDBlockToken,\
                                                                                    job.filesizeIn, job.checksumIn, thisExperiment, runJob.getParentWorkDir(), job.ddmEndPointIn)
        if ec != 0:
            tolog("!!WARNING!!4440!! Failed to create initial PFC - cannot continue, will stop all threads")

            # Stop threads
            runJob.stopAsyncOutputStagerThread()
            runJob.joinAsyncOutputStagerThread()
            runJob.stopMessageThread()
            runJob.joinMessageThread()
            tokenExtractorProcess.kill()

            # Close stdout/err streams
            if tokenextractor_stdout:
                tokenextractor_stdout.close()
            if tokenextractor_stderr:
                tokenextractor_stderr.close()

            job.result[0] = "failed"
            job.result[2] = error.ERR_ESRECOVERABLE
            runJob.failJob(0, job.result[2], job, pilotErrorDiag=pilotErrorDiag)

        if not os.environ.has_key('ATHENA_PROC_NUMBER'):
            tolog("ATHENA_PROC_NUMBER not defined, setting it to 1")
            runCommandList[0] = 'export ATHENA_PROC_NUMBER=1; %s' % (runCommandList[0])

        # AthenaMP needs to know where exactly is the PFC
        runCommandList[0] += " '--postExec' 'svcMgr.PoolSvc.ReadCatalog += [\"xmlcatalog_file:%s\"]'" % (runJob.getPoolFileCatalogPath())

        # ONLY IF STAGE-IN IS SKIPPED: (WHICH CURRENTLY DOESN'T WORK)

        # Now update the --inputEvgenFile option with the full path to the input file using the TURL
        #inputFile = getProperInputFileName(job.inFiles)
        #turl = file_info_dictionary[inputFile][0]
        #runCommandList[0] = runCommandList[0].replace(inputFile, turl)
        #tolog("Replaced '%s' with '%s' in the run command" % (inputFile, turl))

        # Create and start the AthenaMP process
        athenaMPProcess = runJob.getSubprocess(thisExperiment, runCommandList[0], stdout=athenamp_stdout, stderr=athenamp_stderr)

        # Start the utility if required
        utility_subprocess = runJob.getUtilitySubprocess(thisExperiment, runCommandList[0], athenaMPProcess.pid, job)

        # Main loop ........................................................................................

        # nonsense counter used to get different "event server" message using the downloadEventRanges() function
        tolog("Entering monitoring loop")

        k = 0
        max_wait = 30
        nap = 5
        eventRangeFilesDictionary = {}
        while True:
            # if the AthenaMP workers are ready for event processing, download some event ranges
            # the boolean will be set to true in the listener after the "Ready for events" message is received from the client
            if runJob.isAthenaMPReady():

                # Pilot will download some event ranges from the Event Server
                message = downloadEventRanges(job.jobId, job.jobsetID, job.taskID)

                # Create a list of event ranges from the downloaded message
                event_ranges = runJob.extractEventRanges(message)

                # Are there any event ranges?
                if event_ranges == []:
                    tolog("No more events")
                    runJob.sendMessage("No more events")
                    break

                # Update the token extractor file list and keep track of added guids to the file list (not needed for Event Index)
                if not runJob.useEventIndex():
                    eventRangeFilesDictionary = runJob.getEventRangeFilesDictionary(event_ranges, eventRangeFilesDictionary)
                    eventRangeFilesDictionary = runJob.updateTokenExtractorInputFile(eventRangeFilesDictionary, input_file)

                # Get the current list of eventRangeIDs
                currentEventRangeIDs = runJob.extractEventRangeIDs(event_ranges)

                # Store the current event range id's in the total event range id dictionary
                runJob.addEventRangeIDsToDictionary(currentEventRangeIDs)

                # Create a new PFC for the current event ranges
                ec, pilotErrorDiag, file_info_dictionary = runJob.createPoolFileCatalogFromMessage(message, thisExperiment)
                if ec != 0:
                    tolog("!!WARNING!!4444!! Failed to create PFC - cannot continue, will stop all threads")
                    runJob.sendMessage("No more events")
                    break

                # Loop over the event ranges and call AthenaMP for each event range
                i = 0
                j = 0
                for event_range in event_ranges:
                    # Send the event range to AthenaMP
                    tolog("Sending a new event range to AthenaMP (id=%s)" % (currentEventRangeIDs[j]))
                    runJob.setSendingEventRange(True)
                    runJob.setCurrentEventRange(currentEventRangeIDs[j])
                    runJob.sendMessage(str([event_range]))
                    runJob.setSendingEventRange(False)

                    # Set the boolean to false until AthenaMP is again ready for processing more events
                    runJob.setAthenaMPIsReady(False)

                    # Wait until AthenaMP is ready to receive another event range
                    while not runJob.isAthenaMPReady():
                        # Take a nap
                        if i%10 == 0:
                            tolog("Event range loop iteration #%d" % (i))
                        i += 1
                        time.sleep(nap)

                        # Is AthenaMP still running?
                        if athenaMPProcess.poll() is not None:
                            tolog("AthenaMP appears to have finished (aborting event processing loop for this event range)")
                            break

                        if runJob.isAthenaMPReady():
                            tolog("AthenaMP is ready for new event range")
                            break

                        # Make sure that the utility subprocess is still running
                        if utility_subprocess:
                            if not utility_subprocess.poll() is None:
                                # If poll() returns anything but None it means that the subprocess has ended - which it should not have done by itself
                                tolog("!!WARNING!!4343!! Dectected crashed utility subprocess - will restart it")
                                utility_subprocess = self.getUtilitySubprocess(thisExperiment, cmd, main_subprocess.pid, job)

                        # Make sure that the token extractor is still running
                        if not tokenExtractorProcess.poll() is None:
                            max_wait = 0
                            job.pilotErrorDiag = "Token Extractor has crashed"
                            job.result[0] = "failed"
                            job.result[2] = error.ERR_TEFATAL
                            tolog("!!WARNING!!2322!! %s (aborting monitoring loop)" % (job.pilotErrorDiag))
                            break

                    # Is AthenaMP still running?
                    if athenaMPProcess.poll() is not None:
                        tolog("AthenaMP has finished (aborting event range loop for current event ranges)")
                        break

                    # Was there a fatal error in the inner loop?
                    if job.result[0] == "failed":
                        tolog("Detected a failure - aborting event range loop")
                        break

                    j += 1

                # Is AthenaMP still running?
                if athenaMPProcess.poll() is not None:
                    tolog("AthenaMP has finished (aborting event range loop)")
                    break

            else:
                time.sleep(6)

                if k%10 == 0:
                    tolog("AthenaMP waiting loop iteration #%d" % (k))
                k += 1

                # Is AthenaMP still running?
                if athenaMPProcess.poll() is not None:
                    job.pilotErrorDiag = "AthenaMP finished prematurely"
                    job.result[0] = "failed"
                    job.result[2] = error.ERR_ESATHENAMPDIED
                    tolog("!!WARNING!!2222!! %s (aborting monitoring loop)" % (job.pilotErrorDiag))
                    break
                
                # Make sure that the utility subprocess is still running
                if utility_subprocess:
                    if not utility_subprocess.poll() is None:
                        # If poll() returns anything but None it means that the subprocess has ended - which it should not have done by itself
                        tolog("!!WARNING!!4343!! Dectected crashed utility subprocess - will restart it")
                        utility_subprocess = self.getUtilitySubprocess(thisExperiment, cmd, main_subprocess.pid, job)

                # Make sure that the token extractor is still running
                if not tokenExtractorProcess.poll() is None:
                    max_wait = 0
                    job.pilotErrorDiag = "Token Extractor has crashed"
                    job.result[0] = "failed"
                    job.result[2] = error.ERR_TEFATAL
                    tolog("!!WARNING!!2322!! %s (aborting monitoring loop)" % (job.pilotErrorDiag))
                    break

        # Wait for AthenaMP to finish
        i = 0
        kill = False
        while athenaMPProcess.poll() is None:
            tolog("Waiting for AthenaMP to finish (#%d)" % (i))
            if i > max_wait:
                # Stop AthenaMP
                tolog("Waited long enough - Stopping AthenaMP process")
                athenaMPProcess.kill()
                tolog("(Kill signal SIGTERM sentto AthenaMP - jobReport might get lost)")
                kill = True
                break

            time.sleep(60)
            i += 1

        if not kill:
            tolog("AthenaMP has finished")

        # Stop the utility
        if utility_subprocess:
            utility_subprocess.send_signal(signal.SIGUSR1)
            tolog("Terminated the utility subprocess")

            _nap = 10
            tolog("Taking a short nap (%d s) to allow the utility to finish writing to the summary file" % (_nap))
            time.sleep(_nap)

            # Copy the output JSON to the pilots init dir
            _path = os.path.join(job.workdir, thisExperiment.getUtilityJSONFilename())
            if os.path.exists(_path):
                try:
                    copy2(_path, runJob.getPilotInitDir())
                except Exception, e:
                    tolog("!!WARNING!!2222!! Caught exception while trying to copy JSON files: %s" % (e))
                else:
                    tolog("Copied %s to pilot init dir" % (_path))
            else:
                tolog("File %s was not created" % (_path))

        # Do not stop the stageout thread until all output files have been transferred
        starttime = time.time()
        maxtime = 30*60
#        while len (runJob.getStageOutQueue()) > 0 and (time.time() - starttime < maxtime):
#            tolog("stage-out queue: %s" % (runJob.getStageOutQueue()))
#            tolog("(Will wait for a maximum of %d seconds, so far waited %d seconds)" % (maxtime, time.time() - starttime))
#            time.sleep(5)

        while not runJob.areAllOutputFilesTransferred():
            if len(runJob.getStageOutQueue()) == 0:
                tolog("No files in stage-out queue, no point in waiting for transfers since AthenaMP has finished (job is failed)")
                break

            tolog("Will wait for a maximum of %d seconds for file transfers to finish (so far waited %d seconds)" % (maxtime, time.time() - starttime))
            tolog("stage-out queue: %s" % (runJob.getStageOutQueue()))
            if (len(runJob.getStageOutQueue())) > 0 and (time.time() - starttime > maxtime):
                tolog("Aborting stage-out thread (timeout)")
                break
            time.sleep(30)

        # replace the default job output file list which is anyway not correct
        # (it is only used by AthenaMP for generating output file names)
#        job.outFiles = output_files
#        runJob.setJobOutFiles(job.outFiles)
#        tolog("output_files = %s" % (output_files))

        # Get the datasets for the output files
        dsname, datasetDict = runJob.getDatasets()
        tolog("dsname = %s" % (dsname))
        tolog("datasetDict = %s" % (datasetDict))

        # Create the output file dictionary needed for generating the metadata
        ec, pilotErrorDiag, outs, outsDict = RunJobUtilities.prepareOutFiles(job.outFiles, job.logFile, job.workdir, fullpath=True)
        if ec:
            # missing output file (only error code from prepareOutFiles)
            runJob.failJob(job.result[1], ec, job, pilotErrorDiag=pilotErrorDiag)
        tolog("outsDict: %s" % str(outsDict))

        # Create metadata for all successfully staged-out output files (include the log file as well, even if it has not been created yet)
        ec, outputFileInfo = runJob.createFileMetadata(outsDict, dsname, datasetDict, jobSite.sitename)
        if ec:
            runJob.failJob(0, ec, job, pilotErrorDiag=job.pilotErrorDiag)

        tolog("Stopping stage-out thread")
        runJob.stopAsyncOutputStagerThread()
        runJob.joinAsyncOutputStagerThread()
#        asyncOutputStager_thread.stop()
#        asyncOutputStager_thread.join()
#        runJob.setAsyncOutputStagerThread(asyncOutputStager_thread)

        # Stop Token Extractor
#        if tokenExtractorProcess:
#            tolog("Stopping Token Extractor process")
#            tokenExtractorProcess.kill()
#            tolog("(Kill signal SIGTERM sent)")
#        else:
#            tolog("No Token Extractor process running")

        # Close stdout/err streams
        if tokenextractor_stdout:
            tokenextractor_stdout.close()
        if tokenextractor_stderr:
            tokenextractor_stderr.close()

        # Close stdout/err streams
        if athenamp_stdout:
            athenamp_stdout.close()
        if athenamp_stderr:
            athenamp_stderr.close()

        tolog("Stopping message thread")
        runJob.stopMessageThread()
        runJob.joinMessageThread()
#        message_thread.stop()
#        message_thread.join()
#        runJob.setMessageThread(message_thread)

        # Rename the metadata produced by the payload
        # if not pUtil.isBuildJob(outs):
        runJob.moveTrfMetadata(job.workdir, job.jobId)

        # Check the job report for any exit code that should replace the res_tuple[0]
        res0, exitAcronym, exitMsg = runJob.getTrfExitInfo(0, job.workdir)
        res = (res0, exitMsg, exitMsg)

        # If payload leaves the input files, delete them explicitly
        if ins:
            ec = pUtil.removeFiles(job.workdir, ins)

        # Payload error handling
        ed = ErrorDiagnosis()
        job = ed.interpretPayload(job, res, False, 0, runCommandList, runJob.getFailureCode())
        if job.result[1] != 0 or job.result[2] != 0:
            runJob.failJob(job.result[1], job.result[2], job, pilotErrorDiag=job.pilotErrorDiag)
        runJob.setJob(job)

        # wrap up ..........................................................................................

        errorCode = runJob.getErrorCode()
        if not runJob.getStatus() or errorCode != 0:
            tolog("Detected at least one transfer failure, job will be set to failed")
            job.jobState = "failed"
            job.result[2] = errorCode
        else:
            tolog("No transfer failures detected, job will be set to finished")
            job.jobState = "finished"
        job.setState([job.jobState, job.result[1], job.result[2]])
        runJob.setJobState(job.jobState)
        rt = RunJobUtilities.updatePilotServer(job, runJob.getPilotServer(), runJob.getPilotPort(), final=True)

        tolog("Done")
        runJob.sysExit(job)

    except Exception, errorMsg:

        error = PilotErrors()

        if runJob.getGlobalPilotErrorDiag() != "":
            pilotErrorDiag = "Exception caught in RunJobEvent: %s" % (runJob.getGlobalPilotErrorDiag())
        else:
            pilotErrorDiag = "Exception caught in RunJobEvent: %s" % str(errorMsg)

        if 'format_exc' in traceback.__all__:
            pilotErrorDiag += ", " + traceback.format_exc()

        try:
            tolog("!!FAILED!!3001!! %s" % (pilotErrorDiag))
        except Exception, e:
            if len(pilotErrorDiag) > 10000:
                pilotErrorDiag = pilotErrorDiag[:10000]
                tolog("!!FAILED!!3001!! Truncated (%s): %s" % (str(e), pilotErrorDiag))
            else:
                pilotErrorDiag = "Exception caught in RunJobEvent: %s" % str(e)
                tolog("!!FAILED!!3001!! %s" % (pilotErrorDiag))

        job = Job.Job()
        job.setJobDef(newJobDef.job)
        job.pilotErrorDiag = pilotErrorDiag
        job.result[0] = "failed"
        if runJob.getGlobalErrorCode() != 0:
            job.result[2] = runJob.getGlobalErrorCode()
        else:
            job.result[2] = error.ERR_RUNEVENTEXC
        tolog("Failing job with error code: %d" % (job.result[2]))
        # fail the job without calling sysExit/cleanup (will be called anyway)
        runJob.failJob(0, job.result[2], job, pilotErrorDiag=pilotErrorDiag, docleanup=False)

    # end of RunJobEvent
