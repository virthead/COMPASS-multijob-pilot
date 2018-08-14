# Class definition:
#   COMPASSSiteInformation
#   This class is the prototype of a site information class inheriting from SiteInformation
#   Instances are generated with SiteInformationFactory via pUtil::getSiteInformation()
#   Implemented as a singleton class
#   http://stackoverflow.com/questions/42558/python-and-the-singleton-pattern

# import relevant python/pilot modules
import os
import traceback
import Job, SiteMover
from SiteInformation import SiteInformation  # Main site information class
from pUtil import tolog                      # Logging method that sends text to the pilot log
from pUtil import readpar                    # Used to read values from the schedconfig DB (queuedata)
from PilotErrors import PilotErrors          # Error codes

class COMPASSSiteInformation(SiteInformation):

    # private data members
    __experiment = "COMPASS"
    __instance = None

    # Required methods

    def __init__(self):
        """ Default initialization """
# not needed?

        pass

    def __new__(cls, *args, **kwargs):
        """ Override the __new__ method to make the class a singleton """

        if not cls.__instance:
            cls.__instance = super(COMPASSSiteInformation, cls).__new__(cls, *args, **kwargs)

        return cls.__instance

    def getExperiment(self):
        """ Return a string with the experiment name """

        return self.__experiment

    def isTier1(self, sitename):
        """ Is the given site a Tier-1? """

        return False

    def isTier2(self, sitename):
        """ Is the given site a Tier-2? """

        return False

    def isTier3(self):
        """ Is the given site a Tier-3? """
        # Note: defined by DB

        if readpar('ddm') == "local":
            status = True
        else:
            status = False

        return status

    def allowAlternativeStageOut(self, flag=False):
        """ Is alternative stage-out allowed? """
        # E.g. if stage-out to primary SE (at Tier-2) fails repeatedly, is it allowed to attempt stage-out to secondary SE (at Tier-1)?

        return False

    def extractJobPar(self, job, par, ptype="string"):

        strpars = job.jobPars
        cmdopt = shlex.split(strpars)
        parser = PassThroughOptionParser()
        parser.add_option(par,\
                          dest='par',\
                          type=ptype)
        (options,args) = parser.parse_args(cmdopt)
        return options.par
    
    def getProperPaths(self, error, analyJob, token, prodSourceLabel, dsname, filename, jobId, jobPars, **pdict):
        """ Get proper paths (SURL and LFC paths) """

        ec = 0
        pilotErrorDiag = ""
        tracer_error = ""
        dst_gpfn = ""
        lfcdir = ""
        surl = ""

        alt = pdict.get('alt', False)
        scope = pdict.get('scope', None)
                
        # Get the proper endpoint
        sitemover = SiteMover.SiteMover()
        se = sitemover.getProperSE(token, alt=alt)

        # For production jobs, the SE path is stored in seprodpath
        # For analysis jobs, the SE path is stored in sepath

        destination = sitemover.getPreDestination(analyJob, token, prodSourceLabel, alt=alt)
        if destination == '':
            pilotErrorDiag = "put_data destination path in SE not defined"
            tolog('!!WARNING!!2990!! %s' % (pilotErrorDiag))
            tracer_error = 'PUT_DEST_PATH_UNDEF'
            ec = error.ERR_STAGEOUTFAILED
            return ec, pilotErrorDiag, tracer_error, dst_gpfn, lfcdir, surl
        else:
            tolog("Going to store job output at: %s" % (destination))
            # /dpm/grid.sinica.edu.tw/home/atlas/atlasscratchdisk/
                
        if not ".log.tgz" in filename:
            try:
                # always use this filename as the new jobDef module name
                se_path = ''
                sw_path = ''
                prod_name = ''
                prodSlt = ''
                TMPMDSTFILE = ''
                TMPHISTFILE = ''
                EVTDUMPFILE = ''
                MERGEDMDSTFILE = ''
                MERGEDHISTFILE = ''
                MERGEDDUMPFILE = ''
        
                tolog("Trying to get sw path, name and hist filename from job definition.")
                tolog("jobPars: %s" % (jobPars))
                sw_prefix, sw_path, prod_name, prodSlt, TMPMDSTFILE, TMPHISTFILE, EVTDUMPFILE, MERGEDMDSTFILE, MERGEDHISTFILE, MERGEDDUMPFILE = self.getSWPathAndNameAndFilename(jobPars)
                tolog("sw_prefix: %s" % (sw_prefix))
                tolog("sw_path: %s" % (sw_path))
                tolog("prod_name: %s" % (prod_name))
                tolog("prodSlt: %s" % (prodSlt))
                tolog("TMPMDSTFILE: %s" % (TMPMDSTFILE))
                tolog("TMPHISTFILE: %s" % (TMPHISTFILE))
                tolog("EVTDUMPFILE: %s" % (EVTDUMPFILE))
                tolog("MERGEDMDSTFILE: %s" % (MERGEDMDSTFILE))
                tolog("MERGEDHISTFILE: %s" % (MERGEDHISTFILE))
                tolog("MERGEDDUMPFILE: %s" % (MERGEDDUMPFILE))
                
                # prod
                if filename == TMPMDSTFILE :
                    se_path = sw_prefix + sw_path + prod_name + '/mDST.chunks'
                if filename == TMPHISTFILE:
                    se_path = sw_prefix + sw_path + prod_name + '/TRAFDIC'
                if filename == "testevtdump.raw":
                    se_path = sw_prefix + sw_path + prod_name + '/evtdump/slot' + prodSlt
                    filename = EVTDUMPFILE
                if filename == "payload_stdout.txt":
                    se_path = sw_prefix + sw_path + prod_name + '/logFiles'
                    filename = prod_name + '.' + TMPHISTFILE.replace('.root', '.stdout')
                if filename == "payload_stderr.txt":
                    se_path = sw_prefix + sw_path + prod_name + '/logFiles'
                    filename = prod_name + '.' + TMPHISTFILE.replace('.root', '.stderr')
                
                # merge
                if filename == MERGEDMDSTFILE :
                    se_path = sw_prefix + sw_path + prod_name + '/mDST'
                if filename == MERGEDHISTFILE:
                    se_path = sw_prefix + sw_path + prod_name + '/histos'
                if filename == MERGEDDUMPFILE:
                    se_path = sw_prefix + sw_path + prod_name + '/mergedDump/slot' + prodSlt
                
                destination = se_path
                
            except Exception, errorMsg:
                error = PilotErrors()
                pilotErrorDiag = "Exception caught in COMPASSSiteInformation: %s" % str(errorMsg)
                
                if 'format_exc' in traceback.__all__:
                    pilotErrorDiag += ", " + traceback.format_exc()    

                try:
                    tolog("!!FAILED!!3001!! %s" % (pilotErrorDiag))
                except Exception, e:
                    if len(pilotErrorDiag) > 10000:
                        pilotErrorDiag = pilotErrorDiag[:10000]
                        tolog("!!FAILED!!3001!! Truncated (%s): %s" % (e, pilotErrorDiag))
                    else:
                        pilotErrorDiag = "Exception caught in runJob: %s" % (e)
                        tolog("!!FAILED!!3001!! %s" % (pilotErrorDiag))
                    
        # Define the SURL
        surl = "%s%s/%s" % (se, destination, filename)
        dst_gpfn = "%s/%s" % (destination, filename)
        lfcdir = destination

        # Correct the SURL which might start with something like 'token:ATLASMCTAPE:srm://srm-atlas.cern.ch:8443/srm/man/..'
        # If so, remove the space token before the srm info
        if surl.startswith('token'):
            tolog("Removing space token part from SURL")
            dummy, surl = sitemover.extractSE(surl)
                
        tolog("SURL = %s" % (surl))
        tolog("dst_gpfn = %s" % (dst_gpfn))
        tolog("lfcdir = %s" % (lfcdir))
        
        return ec, pilotErrorDiag, tracer_error, dst_gpfn, lfcdir, surl

    def getSWPathAndNameAndFilename(self, jobPars):
        """ Get COMPASS_SW_PATH and COMPASS_PROD_NAME from JobPars """
        
        a = jobPars.find('COMPASS_SW_PREFIX')
        b = jobPars[a:]
        c = b.find(';')
        d = b[:c]
        sw_prefix = d[d.find('=') + 1:]
        
        a = jobPars.find('COMPASS_SW_PATH')
        b = jobPars[a:]
        c = b.find(';')
        d = b[:c]
        sw_path = d[d.find('=') + 1:]

        a = jobPars.find('COMPASS_PROD_NAME')
        b = jobPars[a:]
        c = b.find(';')
        d = b[:c]
        prod_name = d[d.find('=') + 1:]
        
        a = jobPars.find('prodSlt')
        b = jobPars[a:]
        c = b.find(';')
        d = b[:c]
        prodSlt = d[d.find('=') + 1:]
        
        a = jobPars.find('TMPMDSTFILE')
        b = jobPars[a:]
        c = b.find(';')
        d = b[:c]
        TMPMDSTFILE = d[d.find('=') + 1:]
        
        a = jobPars.find('TMPHISTFILE')
        b = jobPars[a:]
        c = b.find(';')
        d = b[:c]
        TMPHISTFILE = d[d.find('=') + 1:]
        
        a = jobPars.find('EVTDUMPFILE')
        b = jobPars[a:]
        c = b.find(';')
        d = b[:c]
        EVTDUMPFILE = d[d.find('=') + 1:]
        
        a = jobPars.find('MERGEDMDSTFILE')
        b = jobPars[a:]
        c = b.find(';')
        d = b[:c]
        MERGEDMDSTFILE = d[d.find('=') + 1:]
        
        a = jobPars.find('MERGEDHISTFILE')
        b = jobPars[a:]
        c = b.find(';')
        d = b[:c]
        MERGEDHISTFILE = d[d.find('=') + 1:]
        
        a = jobPars.find('MERGEDDUMPFILE')
        b = jobPars[a:]
        c = b.find(';')
        d = b[:c]
        MERGEDDUMPFILE = d[d.find('=') + 1:]
        
        return sw_prefix, sw_path, prod_name, prodSlt, TMPMDSTFILE, TMPHISTFILE, EVTDUMPFILE, MERGEDMDSTFILE, MERGEDHISTFILE, MERGEDDUMPFILE

if __name__ == "__main__":

    a = COMPASSSiteInformation()
    tolog("Experiment: %s" % (a.getExperiment()))

