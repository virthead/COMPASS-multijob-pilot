# Class definition:
#   COMPASSExperiment
#   This class is the prototype of an experiment class inheriting from Experiment
#   Instances are generated with ExperimentFactory via pUtil::getExperiment()
#   Implemented as a singleton class
#   http://stackoverflow.com/questions/42558/python-and-the-singleton-pattern

# import relevant python/pilot modules
from Experiment import Experiment               # Main experiment class
from pUtil import tolog                         # Logging method that sends text to the pilot log
from pUtil import grep                          # Grep function - reimplement using cli command
from PilotErrors import PilotErrors             # Error codes
from RunJobUtilities import getStdoutFilename   #

# Standard python modules
import re
import os
import time
import commands
from numpy import mat

class COMPASSExperiment(Experiment):

    # private data members
    __experiment = "COMPASS"
    __instance = None
    __error = PilotErrors()                # PilotErrors object
    __doFileLookups = False                # True for LFC based file lookups (basically a dummy data member here since singleton object is static)
    __cache = ""                           # Cache URL used e.g. by LSST

    # Required methods

    def __init__(self):
        """ Default initialization """

        # e.g. self.__errorLabel = errorLabel
        pass

    def __new__(cls, *args, **kwargs):
        """ Override the __new__ method to make the class a singleton """

        if not cls.__instance:
            cls.__instance = super(COMPASSExperiment, cls).__new__(cls, *args, **kwargs)

        return cls.__instance

    def getExperiment(self):
        """ Return a string with the experiment name """

        return self.__experiment

    def setParameters(self, *args, **kwargs):
        """ Set any internally needed variables """

        # set initial values
        self.__job = kwargs.get('job', None)
        if self.__job:
            self.__analysisJob = isAnalysisJob(self.__job.trf)
        else:
            self.__warning = "setParameters found no job object"

    def getJobExecutionCommand(self, job, jobSite, pilot_initdir):
        """ Define and test the command(s) that will be used to execute the payload """
        # E.g. cmd = "source <path>/setup.sh; <path>/python "

        cmd = { "interpreter": '',
                "payload": '',
                "parameters": job.jobPars,
                "environment": '' }
#        cmd = job.jobPars
#        cmd = job.trf + " " + job.jobPars

#        return 0, pilotErrorDiag, cmd, special_setup_cmd, JEM, cmtconfig
        return 0, "", cmd, "", "", ""

    def willDoFileLookups(self):
        """ Should (LFC) file lookups be done by the pilot or not? """

        return False

    def willDoFileRegistration(self):
        """ Should (LFC) file registration be done by the pilot or not? """

        return False

    def doFileLookups(self, doFileLookups):
        """ Update the file lookups boolean """

        # Only implement this method if class really wants to update the __doFileLookups boolean
        # ATLAS wants to implement this, but not CMS
        # Method is used by Mover
        # self.__doFileLookups = doFileLookups
        pass

    def specialChecks(self, **kwargs):
        """ Implement special checks here """
        # Return False if fatal failure, otherwise return True
        # The pilot will abort if this method returns a False

        status = False

        tolog("No special checks for \'%s\'" % (self.__experiment))

        return True # obviously change this to 'status' once implemented

    # Optional
    def setCache(self, cache):
        """ Cache URL """
        # Used e.g. by LSST

        self.__cache = cache

    # Optional
    def getCache(self):
        """ Return the cache URL """
        # Used e.g. by LSST

        return self.__cache

    # Optional
    def useTracingService(self):
        """ Use the DQ2 Tracing Service """
        # A service provided by the DQ2 system that allows for file transfer tracking; all file transfers
        # are reported by the pilot to the DQ2 Tracing Service if this method returns True

        return False

    def formatReleaseString(self, release):
        """ Return a special formatted release string """

        return release

    def getNumberOfEvents(self, **kwargs):
        """ Return the number of events """
        # ..and a string of the form N|N|..|N with the number of jobs in the trf(s)

        job = kwargs.get('job', None)
        number_of_jobs = kwargs.get('number_of_jobs', 1)

        if not job:
            tolog("!!WARNING!!2332!! getNumberOfEvents did not receive a job object")
            return 0, 0, ""
        
        nEventsRead = 0
        nEventsWritten = 0
        tolog("Looking for number of processed events (pass 2: Resorting to brute force grepping of payload stdout)")
        nEvents_str = ""
        for i in range(number_of_jobs):
            _stdout = job.stdout
            if number_of_jobs > 1:
                _stdout = _stdout.replace(".txt", "_%d.txt" % (i + 1))
            filename = os.path.join(job.workdir, _stdout)
            N = 0
            if os.path.exists(filename):
                tolog("Processing stdout file of normal job: %s" % (filename))
                matched_lines = grep(["events had been written to miniDST"], filename)
                if len(matched_lines) > 0:
                    N = int(re.match('^(\d+) events had been written to miniDST.*', matched_lines[-1]).group(1))
                
                tolog("Processing stdout file of merging: %s" % (filename))
                matched_lines = grep(["Number of events saved to output"], filename)
                if len(matched_lines) > 0:
                    N = int(re.match('^Number of events saved to output\s+:\s+(\d+)', matched_lines[-1]).group(1))

            if len(nEvents_str) == 0:
                nEvents_str = str(N)
            else:
                nEvents_str += "|%d" % (N)
            nEventsRead += N

        return nEventsRead, nEventsWritten, nEvents_str
            
    def isSomethingInStd(self, **kwargs):
        """ Check if job output contains message """
            
        test = False
        
        job = kwargs.get('job', None)
        what = kwargs.get('what', None)
        where = kwargs.get('where', None)
        number_of_jobs = kwargs.get('number_of_jobs', 1)

        if not job:
            tolog("!!WARNING!!3222!! isSomethingInStd() did not receive a job object")
            return False

        tolog("Checking %s in %s.." % (what, where))
        for i in range(number_of_jobs):
            if where == 'stdout':
                _f = job.stdout
            else:
                _f = job.stderr
            if number_of_jobs > 1:
                _f = _f.replace(".txt", "_%d.txt" % (i + 1))
            filename = os.path.join(job.workdir, _f)
            if os.path.exists(filename):
                tolog("Processing %s file: %s" % (where, filename))
                matched_lines = grep([what], filename)
                if len(matched_lines) > 0:
                    tolog("Identified a '%s' in %s %s:" % (what, job.payload, where))
                    for line in matched_lines:
                        tolog(line)
                    test = True
            else:
                tolog("Warning: File %s does not exist" % (filename))

        return test
    
    def interpretPayloadStdout(self, job, res, getstatusoutput_was_interrupted, current_job_number, runCommandList, failureCode):
        """ Payload error interpretation and handling """

        error = PilotErrors()
        transExitCode = res[0]%255

        # Get the proper stdout filename
        number_of_jobs = 1
        filename = getStdoutFilename(job.workdir, job.stdout, current_job_number, number_of_jobs)
        
        if job.trf == 'test production' or job.trf == 'merging mdst' or job.trf == 'mass production' or job.trf == 'technical production':
            # Try to identify something wrong in stdout and stderr of job
            is_end_of_job = self.isSomethingInStd(job=job, number_of_jobs=number_of_jobs, what="End of Job", where="stdout")
        elif job.trf == 'DDD filtering':
            is_end_of_job = self.isSomethingInStd(job=job, number_of_jobs=number_of_jobs, what="End of the decoding!", where="stdout")
        else:
            is_end_of_job = True
        
        a_fatal_error_appeared = self.isSomethingInStd(job=job, number_of_jobs=number_of_jobs, what="A FATAL ERROR APPEARED", where="stdout")
        core_dumped = self.isSomethingInStd(job=job, number_of_jobs=number_of_jobs, what="\(core dumped\) \$CORAL/../phast/coral/coral.exe", where="stderr")
        no_events = self.isSomethingInStd(job=job, number_of_jobs=number_of_jobs, what="char\* exception: DaqEventsManager::GetEvent\(\): no event", where="stderr")
        abnormal_job_termination = self.isSomethingInStd(job=job, number_of_jobs=number_of_jobs, what="Abnormal job termination. Exception #2 had been caught", where="stdout")
        aborted = self.isSomethingInStd(job=job, number_of_jobs=number_of_jobs, what="Aborted ", where="stderr")
        cannot_allocate = self.isSomethingInStd(job=job, number_of_jobs=number_of_jobs, what="TLattice::TLattice: Cannot Allocate [0-9]+ Bytes in Memory!", where="stderr")
        empty_string = self.isSomethingInStd(job=job, number_of_jobs=number_of_jobs, what="_LEDinSpills empty string from calib file", where="stderr")
        cant_connect_to_cdb = self.isSomethingInStd(job=job, number_of_jobs=number_of_jobs, what="CsEvent::getNextEvent: can't connect to CDB database", where="stderr")
        read_calib_bad_line = self.isSomethingInStd(job=job, number_of_jobs=number_of_jobs, what="Exception in readCalibration(): EC02P1__: InputTiSdepCorr EC02P1__ bad line", where="stderr")
        killed = self.isSomethingInStd(job=job, number_of_jobs=number_of_jobs, what="Killed\* \$CORAL/../phast/coral/coral.exe", where="stderr")
        exiting_with_code3 = self.isSomethingInStd(job=job, number_of_jobs=number_of_jobs, what="CORAL exiting with return code -3", where="stdout")
        
        failed = False
        is_no_end_of_job = False
        if not is_end_of_job:
            failed = True
            is_no_end_of_job = True
        if a_fatal_error_appeared or core_dumped or no_events or abnormal_job_termination or aborted or cannot_allocate or empty_string or cant_connect_to_cdb or read_calib_bad_line or killed or exiting_with_code3:
            failed = True
        
        # A killed job can have empty output but still transExitCode == 0
        no_payload_output = False
        installation_error = False
        if getstatusoutput_was_interrupted:
            if os.path.exists(filename):
                if os.path.getsize(filename) > 0:
                    tolog("Payload produced stdout but was interrupted (getstatusoutput threw an exception)")
                else:
                    no_payload_output = True
                failed = True
            else:
                failed = True
                no_payload_output = True
        elif len(res[1]) < 20: # protect the following comparison against massive outputs
            if res[1] == 'Undefined':
                failed = True
                no_payload_output = True
        elif failureCode:
            failed = True
        else:
            # check for installation error
            res_tmp = res[1][:1024]
            if res_tmp[0:3] == "sh:" and 'setup.sh' in res_tmp and 'No such file or directory' in res_tmp:
                failed = True
                installation_error = True

        # handle non-zero failed job return code but do not set pilot error codes to all payload errors
        if transExitCode or failed:
            if failureCode:
                job.pilotErrorDiag = "Payload failed: Interrupt failure code: %d" % (failureCode)
                # (do not set pilot error code)
            elif getstatusoutput_was_interrupted:
                raise Exception, "Job execution was interrupted (see stderr)"
            elif a_fatal_error_appeared:
                job.pilotErrorDiag = "A fatal error appeared"
                job.result[2] = error.ERR_AFATALERRORAPPEARED
            elif core_dumped:
                job.pilotErrorDiag = "(core dumped) $CORAL/../phast/coral/coral.exe"
                job.result[2] = error.ERR_COREDUMPED
            elif no_events:
                job.pilotErrorDiag = "char* exception: DaqEventsManager::GetEvent(): no event"
                job.result[2] = error.ERR_NOEVENTS
            elif abnormal_job_termination:
                job.pilotErrorDiag = "Abnormal job termination. Exception #2 had been caught"
                job.result[2] = error.ERR_ABNORMALJOBTERMINATION
            elif aborted:
                job.pilotErrorDiag = "Aborted "
                job.result[2] = error.ERR_ABORTED
            elif cannot_allocate:
                job.pilotErrorDiag = "TLattice::TLattice: Cannot Allocate N Bytes in Memory!"
                job.result[2] = error.ERR_CANNOTALLOCATE
#            elif events_skipped:
#                job.pilotErrorDiag = "Event skipped due to decoding troubles"
#                job.result[2] = error.ERR_EVENTSSKIPPED
            elif empty_string:
                job.pilotErrorDiag = "_LEDinSpills empty string from calib file"
                job.result[2] = error.ERR_EMPTYSTRING
            elif cant_connect_to_cdb:
                job.pilotErrorDiag = "std::exception: CsEvent::getNextEvent: can't connect to CDB database"
                job.result[2] = error.ERR_CANTCONNECTTOCDB
            elif read_calib_bad_line:
                job.pilotErrorDiag = "Exception in readCalibration(): EC02P1__: InputTiSdepCorr EC02P1__ bad line"
                job.result[2] = error.ERR_READCALIBBADLINE
            elif killed:
                job.pilotErrorDiag = "Killed $CORAL/../phast/coral/coral.exe"
                job.result[2] = error.ERR_KILLED
            elif exiting_with_code3:
                job.pilotErrorDiag = "CORAL exiting with return code -3"
                job.result[2] = error.ERR_EXITED_WITH_CODE3
            elif is_no_end_of_job:
                job.pilotErrorDiag = "No End of Job message found in stdout"
                job.result[2] = error.ERR_NOENDOFJOB
            elif no_payload_output:
                job.pilotErrorDiag = "Payload failed: No output"
                job.result[2] = error.ERR_NOPAYLOADOUTPUT
            elif installation_error:
                job.pilotErrorDiag = "Payload failed: Missing installation"
                job.result[2] = error.ERR_MISSINGINSTALLATION
            elif transExitCode:
                # Handle PandaMover errors
                if transExitCode == 176:
                    job.pilotErrorDiag = "PandaMover staging error: File is not cached"
                    job.result[2] = error.ERR_PANDAMOVERFILENOTCACHED
                elif transExitCode == 86:
                    job.pilotErrorDiag = "PandaMover transfer failure"
                    job.result[2] = error.ERR_PANDAMOVERTRANSFER
                else:
                    # check for specific errors in athena stdout
                    if os.path.exists(filename):
                        e1 = "prepare 5 database is locked"
                        e2 = "Error SQLiteStatement"
                        _out = commands.getoutput('grep "%s" %s | grep "%s"' % (e1, filename, e2))
                        if 'sqlite' in _out:
                            job.pilotErrorDiag = "NFS/SQLite locking problems: %s" % (_out)
                            job.result[2] = error.ERR_NFSSQLITE
                        else:
                            job.pilotErrorDiag = "Job failed: Non-zero failed job return code: %d" % (transExitCode)
                            # (do not set a pilot error code)
                    else:
                        job.pilotErrorDiag = "Job failed: Non-zero failed job return code: %d (%s does not exist)" % (transExitCode, filename)
                        # (do not set a pilot error code)
            else:
                job.pilotErrorDiag = "Payload failed due to unknown reason (check payload stdout)"
                job.result[2] = error.ERR_UNKNOWN
            tolog("!!FAILED!!3000!! %s" % (job.pilotErrorDiag))

        # set the trf diag error
#         if res[2] != "":
#             tolog("TRF diagnostics: %s" % (res[2]))
#             job.exeErrorDiag = res[2]

        job.result[1] = transExitCode
        return job
    
if __name__ == "__main__":

    print "Implement test cases here"
    
