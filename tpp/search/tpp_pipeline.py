#!/usr/bin/env python3

"""
Assumptions: We are converting all input files to filterbanks after doing default RFI mitigation, 
searching all spectra, only stokes I is searched, runs on default gpu Id, i.e 0, 
adaptive scrunching on heimdall is enabled, candmaker runs on gpu 0, 
FETCH uses model a and a probability of 0.1. Subbanded search is not yet implemented.

Print statements are to help with logging. Time commands too.

On the use of logging:
 - "debug" tag will be used for writing information that will be put into the database manager.
 - "info" will report status.
 - error and warnings will be used as intended.

This way, a database-ingestion script can easily scan and interpret logs.
"""

#TPPDB: determine job start
from datetime import datetime
time_start_UTC = datetime.utcnow()


import argparse
import glob
import your
from your import Your
import subprocess
import os
from tpp.infrastructure import database as db
from your.utils.misc import YourArgparseFormatter
from timeit import default_timer as timer
import numpy as np
#import pandas as pd # I think pandas is not needed but see if it runs ok without!
from tpp.search import candcsvmaker
import traceback
import csv

"""
<

TPPDB in case of DB communication issue, Bikash recommends writing the
desired "push" dictionaries to a file (and maybe transferring it to
tingle.) These could be regularly sucked over to tingle by some other
external cron job running on TF/DS and then pushed from tingle (from a
separate cron), or they could be attempted to be directly pushed by a
cron job on TF/DS for each user directly onto TPPDB. However, perhaps
the user's home directory should contain a folder that is the nominal
static end location for that kind of thing.


"""


def check_space():
    """ !H!H!H IMPORTANT NEED TO CHECK THERE's ENOUGH SPACE TO HOUSE 3X THE MAIN FILE SIZE. """
    #TPPDB
    
def print_dberr():
    logger.error("*****DB COMMUNICATIONS ERROR, could not push to database.*****")
    logger.error("**************************************************************")
    # I don't think we want exiting behavior here, but this is
    # where we might be able to add in a "save for next update
    # later when TPPDB communications are back up".
    return
    
def dm_max(obslen,f_low,f_high):
    dm_h=(obslen*10**3/4.15)*(1/((1/f_low**2)-(1/f_high**2)))
    return dm_h

def tpp_state(status):
    """.
    This updates the processing_outcomes status.
    An attempted (but failed) update current provides an EXCEPT but no exit.
    """

    time_now = datetime.now().isoformat()

    try:
        # POST UPDATE.
        data={"job_state_time":time_now,
              "job_state": status}
        db.patch("processing_outcomes",outcomeID,data=data)
    except:
        print_dberr()
        
    return


def do_RFI_filter(filenames,basename):
    '''
    Reshma comment: Running your_writer with standard RFI
    mitigation. Clean file to run heimdall and candmaker on. Doesn't
    have to do RFI mitigation on each step. Also, filterbanks
    required for decimate.
    '''

    writer_start=timer()
    writer_cmd="your_writer.py -v -f "+str(filenames)+" -t fil -r -sksig 4 -sgsig 4 -sgfw 15 -name "+basename+"_converted"
    
    logger.debug('WRITER/RFI FILTER: command = ' + writer_cmd)
    subprocess.call(writer_cmd,shell=True)
    writer_end=timer()


    # Make sure the file exits, throw exception if not.
    out_file = basename+"_converted.fil"
    if not os.path.isfile(out_file):
        errorstring = out_file+" file does not exist"
        raise FileNotFoundError(errorstring)
    
    logger.debug('WRITER/RFI FILTER: your_writer.py took '+str(writer_end-writer_start)+' s')

    """
    # Below is the code when we were using a "mask file" mode. It
    # did not end up working because the later codes had trouble
    # understanding the mask file.
    writer_cmd="your_rfimask.py -v -f "+str(filenames)+" -sk_sigma 4 -sg_sigma 4 -sg_frequency 15"
    writer_basename=str(basename)+'_your_rfi_mask'
    killmask_file= f"{writer_basename}.bad_chans"
    with open(killmask_file,'r') as myfile:
    	file_str = myfile.read()
    my_list = [] ##initializing a list
    for chan in file_str.split(' '): ##using split function to split, the list.this splits the value in index specified and return the value.
         my_list.append(chan)
    for chan in my_list:
    	 if chan == '':
        	 my_list.remove(chan)
    if len(my_list) == 0:
         logger.info(f'RFI MASK: No channels zapped')
    else:
         logger.debug(f'RFI MASK: No: of channels zapped = {len(my_list)}')
         logger.info('RFI MASK: Percentage of channels zapped = '+str((len(my_list)/your_object.your_header.nchans)*100)+' %') 

    return len(my_list)
    """
    return


def do_heimdall(your_fil_object):
    """.

    Heimdall performs GPU-based dedispersion and a basic clustering
    and event rejection algorithm.

    It produces .cand files populated with meta-data for individual
    events, including peak DM, S/N, number of clusters, etc.

    """
    heimdall_start=timer()
    logger.info("HEIMDALL:Using the RFI mitigated filterbank file " + str(your_fil_object.your_header.filename)+" for Heimdall")
    logger.info("HEIMDALL:Preparing to run Heimdall..\n")
    f_low=(center_freq+bw/2)*10**(-3) #in GHz
    f_high=(center_freq-bw/2)*10**(-3) #in GHz
    max_heimdall_dm=int(min(dm_max(obs_len,f_low,f_high),10000))
    heimdall_cmd = "your_heimdall.py -f "+ your_fil_object.your_header.filename+" -dm 0 " + str(max_heimdall_dm) 
    subprocess.call(heimdall_cmd,shell=True)
    heimdall_end=timer()
    logger.debug('HEIMDALL: your_heimdall.py took '+str(heimdall_end-heimdall_start)+' s')

def do_candcsvmaker(your_fil_object):
    """.

    candcsvmaker.py has a simple function: create one big csv file
    with all of the candidates inside of it.

    It produces a file called (input-file-base-name).csv

    """
    candcsvmaker_start = timer()
    logger.info('CANDCSVMAKER:Creating a csv file to get all the info from all the cand files...\n')
    fil_file=your_fil_object.your_header.filename
    file_list = "../"+str(fil_file)

    # Expand out file list and cand list before passing to candcsvmaker.
    pattern = r".*\.cand$"
    cand_file_list = []
    for filename in os.listdir("./"):
        if re.search(pattern,filename):
            cand_file_list.append(filename)
    
    # The threshold values below are set to let heimdall, your, and fetch control what gets through.
    n_events,n_members,event_dict = candcsvmaker.gencandcsv(candsfiles=cand_file_list,filelist=file_list,snr_th=0,clustersize_th=0,dm_min=0,dm_max=10000,label=1)

    # The event_dict returned from candcsvmaker is a list of dictionaries with the following keys:
    #    - dm
    #    - tcand (but this is time since start of file in seconds; we need to convert to MJD+time)
    #    - width (but this is number of samples; we need to convert to time width based on tsamp)
    #    - sn
    # Conversion will be done in the main code; we need to later ensure a match to h5/fetch event IDs.

    #!!!RESHMA, GRAHAM, BIKASH AND OTHERS: Large change here, I changed
    #! candcsvmaker to directly call the source function from the code
    #! (candcsvmaker.py was just a "main" input parser around the core
    #! function gencandcsv). However, this new modality is untested and
    #! we should make sure it's working on a rerun of this.
    #!
    #! Secondly, I have set the thresholds to zero for candcsvmaker to
    #! avoid unwanted filtering of candidates, which should be
    #! controlled by us explicitly throughout the pipeline. This extra
    #! layer of filtering seems redundant, given that these values
    #! should be controlled by heimdall, I believe? Is that right?
    #os.system('python ../candcsvmaker.py --snr_th 0 --dm_min_th 0 --dm_max_th 10000 --clustersize_th 0 -v -f ../'+str(fil_file)+' -c *cand')

    candcsvmaker_end = timer()
    logger.debug('CANDCSVMAKER: your_candmaker.py took '+ str(candcsvmaker_end-candcsvmaker_start)+' s')
    logger.debug('CANDCSVMAKER: found ' + str(n_events) + ' events with ' + str(n_members) + ' members.')

    return n_events,n_members,event_dict

def do_your_candmaker(your_fil_object):
    """

    your_candmaker creates h5 files based on the big CSV list of
    files. Fetch score information is not available yet.

    """
    candmaker_start=timer()
    logger.info('CANDMAKER:Preparing to run your_candmaker.py that makes h5 files.....\n')
    if your_fil_object.your_header.nchans <= 256:
        gg = -1
    else:
        gg = 0 
    candmaker_cmd ="your_candmaker.py -v -c *csv -g "+str(gg)+" -n 4 -o ./h5/"
    subprocess.call(candmaker_cmd,shell=True)
    candmaker_end=timer()
    logger.debug('CANDMAKER: your_candmaker.py took '+ str(candmaker_end-candmaker_start)+' s')

def do_fetch():
    fetch_start=timer()
    logger.info("FETCH:Preparing to run FETCH....\n")
    fetch_cmd='predict.py -v -c . -m a -p 0.2'
    subprocess.call(fetch_cmd,shell=True)
    fetch_end=timer()
    logger.debug('FETCH: predict.py took '+str(fetch_end-fetch_start)+' s')

def do_your_h5plotter():
    h5_start=timer()
    logger.info("YOUR_H5PLOTTER: Preparing to plot h5 files....\n")
    plotter_cmd="your_h5plotter.py -c results_a.csv"
    subprocess.call(plotter_cmd,shell=True)
    h5_end=timer()
    logger.debug('YOUR_H5PLOTTER: Took '+str(h5_end-h5_start)+' s') 
        
if __name__ == "__main__":
    # Initiate Logging. Logging types  are:
    #    info (for blah blah blah)
    #    warn (for warnings)
    #    debug (for timestamp checks or other temp debugging)
    #    error (self explanatory)
    import logging
    logging.basicConfig(format='%(asctime)s  %(levelname)s: %(message)s',datefmt='%m-%d-%Y_%H:%M:%S')
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    # For some reason the below line needs to be included for logging to
    # function hereafter... haven't figured out why. - SBS
    logging.info("(This current line is of no consequence)")
    

    parser = argparse.ArgumentParser(
        prog="tpp_pipeline.py",
        description="Convert PSRFITS to FILTERBANK, makes a dedispersion plan, decimate the files if needed, runs HEIMDALL, makes h5s files of candidates, classifies using FETCH. For TPP pipeline usage only, turn on database manager with the secret code. ",
        formatter_class=YourArgparseFormatter,
    )
    parser.add_argument(
        "-f",
        "--files",
        help="Input files to be converted to an output format.",
        required=True,
        nargs="+",
    )
    parser.add_argument(
        "-t",
        "--tpp_db",
        help="Turn on updating to database manager. THIS IS FOR TPP OFFICIAL USE ONLY. To avoid mistaken turn-on, you must include the following arguments to turn it on for real: mastersword outcomeID working_dir",
        required=False,
        nargs=3
    )
    parser.add_argument(
        "-v",
        "--verbose",
        help="Turn on DEBUG-level (general) logging.",
        required=False,
        action='store_true'
    )


    values = parser.parse_args() 


    # Check logging level
    if (values.verbose):
        logger.setLevel(logging.DEBUG)
        # For some reason the below line needs to be included for logging to
        # function hereafter... haven't figured out why. - SBS
    logging.info("(This current line is of no consequence)")


    # Read and check data files
    your_files = Your(values.files)
    logger.info("Reading raw data from "+str(values.files))
    filelist = your_files.your_header.filelist #list of filenames
    filestring = ' '.join(filelist) #single string containing all file names

    center_freq=your_files.your_header.center_freq
    logger.info("The center frequency is "+str(center_freq)+" MHz")

    # gl, gb conversion done within your header class.
    gl=your_files.your_header.gl
    gb=your_files.your_header.gb
    logger.info("The gl is "+str(gl)+" deg")
    logger.info("The gb is "+str(gb)+" deg")

    # !H!H THIS NEEDS TO BE CHECKED IF IT ACTUALLY COMES OUT AS AN MJD!!!
    mjd = your_files.your_header.tstart
    logger.info("The start MJD of the file is "+str(mjd))
    
    bw=your_files.your_header.bw
    logger.info("The bandwidth is "+str(bw)+" MHz")

    tsamp=your_files.your_header.native_tsamp
    logger.info("The native sampling time is "+str(tsamp)+" s")

    obs_len = your_files.your_header.native_nspectra*tsamp
    if obs_len >= 60:
        obs_len_min = obs_len/60
        logger.info("Dataset length is "+str(obs_len_min)+" minutes")
    else:
        logger.info("Dataset length is "+str(obs_len)+" seconds")


    #TPPDB: !!!!! May need to read declination from the TPPDB, not the file itself; Graham says that sometimes declination is listed with seconds>60. !!!!!!
    #  (update on this Sep 2024: "your" may do this correctly, we don't read the RA/Dec directly from the header here, it's done thru the astropy interpreter).

    #TPPDB: !!!!! NEED TO ADD AN INTERNAL CHECK HERE TO MAKE SURE THAT THE DATA INFORMATION READ IS SANE!!!


    # Check Database Manager connection request
    logger.info("My database writer value is "+str(values.tpp_db))
    db_on = False
    if values.tpp_db is not None:
        db_password = values.tpp_db[0]
        outcomeID = values.tpp_db[1]
        working_dir = values.tpp_db[2]
        if (db_password != "mastersword"):
            logger.error("******************************************************************")
            logger.error("***** It looks like you tried to turn on the TPP Manager but *****")
            logger.error("*****        provided the wrong password. Exiting now.       *****")
            logger.error("******************************************************************")
            exit()
        elif (len(values.tpp_db) < 3):
            logger.error("******************************************************************")
            logger.error("***** It looks like you tried to turn on the TPP Manager but *****")
            logger.error("*****  didn't provide pw, outcomeID, directory. Exiting now. *****")
            logger.error("******************************************************************")
            exit()
        elif (db_password == "mastersword"):
            logger.warning("******************************************************************")
            logger.warning("*****Pipeline results will be pushed to TPP Database Manager.*****")
            logger.warning("*****      If this is unintentional, abort your run now.     *****")
            logger.warning("******************************************************************")
            db_on = True
            logger.debug("Changing to the directory provided by TPP-DB interface, " + working_dir)
            os.chdir(working_dir)

        # Test basic TPPDB connection and existence of outcomeID.
        try:
            outcome_doc = db.get("processing_outcomes",outcomeID)
            submissionID = outcome_doc['submissionID']
            dataID = outcome_doc['dataID']
        except:
            print_dberr()
            exit()
            
    else:
        logger.info("No connections will be made to TPP Database Manager.")
        db_on = False


    # Determine node_name and current working directory.
    node_name = os.uname()[1]
    cwd = os.getcwd()
    logger.info("Processing in directory "+str(cwd)+" on node "+str(node_name)+", began at UTC "+str(time_start_UTC.isoformat()))
    if (db_on):
        tpp_state("started")
        try:
            data = {"node_name":node_name,
                    "job_start":time_start_UTC.isoformat()}
            db.patch("processing_outcomes",outcomeID,data=data)
        except:
            print_dberr()



    ############## ############## ############## 
    ##############  YOUR_WRITER   ############## 
    ############## ############## ############## 
    # Runs RFI filtering; also converts psrfits-format files to an RFI-filtered filterbank-format file.
    
    logger.info('WRITER:Preparing to run your_writer to convert the PSRFITS to FILTERBANK and to do RFI mitigation on the fly\n')

    if (db_on):
        tpp_state("your_writer")

    try:
        do_RFI_filter(filestring,your_files.your_header.basename)
        #TPPDB Once we get RFI functionality working we'll include the stat here.
        #TPPDB n_zapped = do_RFI_filter(filestring,your_files.your_header.basename)
    except:
        if (db_on):
            status = "ERROR in your_writer: "+ str(traceback.format_exc())
            tpp_state(status)
        else:
            logger.error(str(traceback.format_exc()))
        exit()
        
    your_fil_object=Your(your_files.your_header.basename+"_converted.fil")

"""
    #TPPDB RFI database functionality!
    # Report level of zapping
    if (n_zapped == 0):
         logger.info(f'RFI MASK: No channels zapped')
    else:
         logger.debug(f'RFI MASK: Number of channels zapped = {len(my_list)}')
         rfi_fraction = n_zapped/your_files.your_header.nchans
         logger.info('RFI MASK: Percentage of channels zapped = '+str(rfi_fraction*100)+' %')

    # Report RFI_fraction (and ideally pre-/post-zap RMS, though we might be able to obtain info for these values).
    if (db_on):
        try:
            data = {"rfi_fraction":rfi_fraction}
            db.patch("processing_outcomes",outcomeID,data=data)
        except:
            print_dberr()
"""

    logger.debug('Writer done, moving on')

    

            

    ############## ############## ############## 
    ##############     DDPLAN     ############## 
    ############## ############## ############## 
    # Will be used for low-frequency data.
    '''

# Running DDplan.py
if center_freq<1000: 
logger.warning("Low frequency (< 1 GHz) data. Preparing to run DDplan.py....\n")

    ddplan_cmd="DDplan.py -o "+your_files.your_header.basename+"_ddplan -l 0 -d 3600 -f "+str(center_freq)+ " -b "+str(np.abs(bw))+ " -n "+str(your_files.your_header.native_nchans)+ " -t"+str(tsamp)+" -w >"+ your_files.your_header.basename+"_ddplan.txt" 
    subprocess.call(ddplan_cmd,shell=True)
    logger.info('DDplan completed. A text file is created\n')
    # Read the input from the text file and decimate. To be fixed....
    deci_cmd="decimate *fil -t 2 -c 1 >"+str(your_files.your_header.basename)+"_decimated.fil" 
    subprocess.call(deci_cmd,shell=True)

    '''


    ############## ############## ############## 
    ##############    HEIMDALL    ############## 
    ############## ############## ############## 

    if (db_on):
        tpp_state("heimdall")


    try:
        do_heimdall(your_fil_object)
    except:
        if (db_on):
            status = "ERROR in heimdall: "+str(traceback.format_exc())
            tpp_state(status)
        else:
            logger.error(str(traceback.format_exc()))
        exit()


    ############## ############## ############## 
    ##############  CANDCSVMAKER  ############## 
    ############## ############## ############## 
    if (db_on):
        tpp_state("candcsvmaker")

    # Go to the new directory with the heimdall cands
    cand_dir=os.chdir(os.getcwd()
                      + "/"
                      + your_fil_object.your_header.basename)
    logger.debug("DIR CHECK:Now you are at "+str(os.getcwd())+"\n")

    try:
        n_events,n_members,event_dict = do_candcsvmaker(your_fil_object)
        # This event_dict returned from candcsvmaker is a list of dictionaries with the following keys:
        #    - dm
        #    - tcand (but this is time since start of file in seconds; we need to convert to MJD+time)
        #    - width (but this is number of samples; we need to convert to time width based on tsamp)
        #    - sn
        # Conversion will be done below; we need to later ensure a match to h5/fetch event IDs.
    except:
        if (db_on):
            status = "ERROR in candcsvmaker: "+str(traceback.format_exc())
            tpp_state(status)
        else:
            logger.error(str(traceback.format_exc()))
        exit()

    # POST UPDATE of n_events and n_members
    if (db_on):
        try:
            data={"n_detections": n_events,
                  "n_members": n_members}
            db.patch("processing_outcomes",outcomeID,data=data)
        except:
            print_dberr()

     
    logger.info('CHECKPOINT: Number of candidates created = '+num_events)


    #Create a directory for the h5s
    cwd = os.getcwd()
    try:
        os.makedir("h5")
    except FileExistsError:
        logger.error("Could not create h5 directory in current directory "+str(cwd))
        exit()
    
    """
    !!!
    NOTE IT IS HERE THAT WE NEED TO DO COORDINATE CORRECTION FOR DRIFTSCAN DATA
    !!!
    """



    ############## ############## ############## 
    ############## YOUR_CANDMAKER ############## 
    ############## ############## ############## 
    if (db_on):
        tpp_state("your_candmaker")

    try:
        do_your_candmaker(your_fil_object)
    except:
        if (db_on):
            status = "ERROR in your_candmaker: "+str(traceback.format_exc())
            tpp_state(status)
        else:
            logger.error(str(traceback.format_exc()))
        exit()

        
    # Go into h5 directory, check all h5 files created appropriately.
    os.chdir(cwd+'/h5')
    logger.debug("DIR CHECK:Now you are at "+str(os.getcwd())+"\n")

    dir_path='./'
    count = 0
    for path in os.listdir(dir_path):
        if os.path.isfile(os.path.join(dir_path, path)):
            count += 1
    num_h5s = count-1   #subracting the log file

    if int(num_h5s)==int(num_cands):
        logger.debug('CHECK:All candidiate h5s created')
    else:
        logger.warning('POSSIBLE ISSUE: Not all cand h5s are created')
        #!RESHMA can you check if you agree that this is an exit-able offense?
        if (db_on):
            logger.error("ERROR in h5 file creation: Not all cand h5s were created.")
            tpp_state("ERROR in h5 file creation: Not all cand h5s were created.")
            exit()




    ############## ############## ############## 
    ##############     FETCH      ############## 
    ############## ############## ############## 
    if (db_on):
        tpp_state("fetch")

    try:
        do_fetch()
    except:
        if (db_on):
            status = "ERROR in fetch: "+str(traceback.format_exc())
            tpp_state(status)
        else:
            logger.error(str(traceback.format_exc()))
        exit()


    if os.path.isfile('results_a.csv'):
        logger.info('FETCH: FETCH ran successfully')
    else:
        logger.error('FETCH:FETCH did not create a csv file')
        if (db_on):
            status = "ERROR in fetch: Fetch did not create a results_a.csv file."
            tpp_state(status)
        else:
            logger.error(str(traceback.format_exc()))
        exit()     


    
    ############## ############## ############## 
    ##############  TPPDB_CANDS   ############## 
    ############## ############## ############## 
    """
    The event_dict returned from candcsvmaker is a list of dictionaries with the following keys:
       - dm
       - tcand (but this is time since start of file in seconds; we need to convert to MJD+time)
       - width (but this is number of samples; we need to convert to time width based on tsamp)
       - sn
    Here we need to later ensure a match to the h5/fetch event IDs generated by your/candmaker.

    Candidate name tracking in Your_candmaker:
    self.id = f"cand_tstart_{self.tstart:.12f}_tcand_{self.tcand:.7f}_dm_{self.dm:.5f}_snr_{self.snr:.5f}"
    tstart is MJD start of the file
    tcand is cand start in seconds 
    dm is actual dm
    snr is snr
    Examples:
    cand_tstart_59876.047106496655_tcand_148.0920000_dm_708.78400_snr_6.12012
    cand_tstart_59876.047106496655_tcand_126.7080000_dm_1479.38000_snr_6.02877
    cand_tstart_59876.047106496655_tcand_57.5152000_dm_1657.08000_snr_6.45542

    Here we can make a loop over the results_a.csv to get the fetch
    score and ID cands. This way, we ensure a double check if one
    of the fetch listed cand_ids is not found due to some issue in
    the way the different codes construct the cand_id. In addition,
    candidates with low fetch scores will not be updated in this
    way.

    read results_a.csv
    loop over each fetchcand:
       deconstruct cand_id
       use pythonic commands to find that cand in event_dict (using tcand, dm)
       if not found, raise alarm.
       if found, update fetch score.
    After loop ends, loop event_dict and update with fixed tcand and width.
    Also can make fetch histogram after loop ends!
    """

    if (db_on):
        tpp_state("db_cand_push")
        
        
        try:
            fetch_scores = []

            # Open and read the CSV file
            with open('results_a.csv', mode='r') as file:
                reader = csv.reader(file)
                
                # Skip the header if there is one
                header = next(reader, None)
    
                # Process each row in the CSV file
                for row in reader:
                    # Split the second column by underscore
                    split_values = row[1].split('_')
            
                    # Append values to respective lists if they exist
                    mjd = float(split_values[2])
                    tcand = float(split_values[4])
                    dm = float(split_values[6])
                    snr = float(split_values[8].replace('.h5',''))

                    # Occasionally the fetch score will be empty; not
                    # sure why this happens. Emailed resh/graham/evan
                    # on 9/9/24 to gather intel.
                    if (row[2] != ""):
                        fetch_score = float(row[2])
                        fetch_scores.append(fetch_score)

                        # FETCH "CANDIDATE" THRESHOLD SET TO 0.2 BY DEFAULT.
                        if (fetch_score >= 0.2):
                            n_candidates += 1
                    else:
                        fetch_score = None
                        logger.warning("Found candidate "+row[1]+" with a blank fetch score.")

                # Map fetch scores to appropriate candidate in
                # event_dict. To do this, for each results_a.csv
                # listing, search event_dict for the entry with the
                # appropriate DM and tcand values. When found, update
                # the fetch score.
                ii==0
                found_cand = False
                for mydict in event_dict:
                    if (mydict['dm'] == dm and mydict['tcand'] == tcand):
                        found_cand = True
                        event_dict[ii]['fetch_score'] = fetch_score
                        event_dict[ii]['result_name'] = row[1].replace('.h5','.png')
                        break
                    ii += 1

                if (found_cand == False):
                    logger.warning("I could not map fetch/event_dict for candidate "+row[1]+" ... this may point to some insidious error in tpp_pipeline.py that should be investigated.")
                    raise LookupError("I could not map fetch/event_dict for candidate "+row[1]+" ... this may point to some insidious error in tpp_pipeline.py that should be investigated.")
                    
        except Exception as error:
            # Yes this db_on check is redundant but I'm leaving it
            # here in case we want to add any part of the above
            # capability for non-TPP usage.
            if (db_on):
                status = "ERROR in db_cand_push: "+str(traceback.format_exc())
                tpp_state(status)
            else:
                logger.error(str(traceback.format_exc()))
            exit()

        # Now loop over event dictionary and update tcand and width.
        # Also update gl, gb, f_ctr parameters.
        # !H!H TPPDB NEED TO MAKE SURE THIS WORKS PROPERLY!!!
        for ii,mydict in enumerate(event_dict):
            event_mjd = mjd + mydict["tcand"]/(3600.0 * 24.0)
            mydict["tcand"] = event_mjd
            event_width = tsamp * mydict["width"]
            mydict["width"] = event_width
            mydict["gl"] = gl
            mydict["gb"] = gb
            mydict["f_ctr"] = center_freq
            mydict["outcomeID"] = outcomeID
            mydict["submissionID"] = submissionID
            mydict["dataID"] = dataID
                
        # Make histogram of fetch scores.
        fetch_hist,bins = np.histogram(fetch_scores,bins=10)



    

    ############## ############## ##############
    ########## SUBMIT CANDS TO DATABASE ########
    ############## ############## ##############
    
    #TPPDB:!!! Here need a "write results to a file if db connection not
    #TPPDB:!!! happening successfully." We don't want to lose candidate
    #TPPDB:!!! records at this stage!
    #TPPDB: gather all relevant info for RESULTS and push every
    #TPPDB: detection to database. Is there a way to do this in bulk?
    #TPPDB: ---ask Bikash. We will need to make sure we catch
    #TPPDB: range/format issues here and report them appropriately.

    # Need to just push events_dict.
    # also update outcomes with fetch_hist. Other outcomes?
    
    if (db_on):
        tpp_state("big_db_submit")
        try:

            # TPP pipeline needs to update a few things:
            """
            Results:
            - all the candidates (new instances, each)

            Outcomes: 
            - (regular updates): job_state_time, job_state
            - (beginning) node_name, job_start
            - (mid) rfi_fraction (or rms pre/post zap)
            - (mid/late) fetch_histogram, n_members, n_detections, n_candidates.
            - (end) job_end
            """

            # TPPDB RFI functionality, add this back in to outcome_data when it works... "rfi_fraction":rfi_fraction,
            outcome_data = {"node_name":node_name,
                    "job_start":time_start_UTC.isoformat(),
                    "fetch_histogram":fetch_hist,
                    "n_detections": n_events,
                    "n_members": n_members,
                    "n_candidates":n_candidates}
                # ALSO NEED JOB END BUT WE GET IT LATER.
            db.patch("processing_outcomes",outcomeID,data=data)


            # Tests:
            #    - are misordered dict entries ok?
            #    - do the datas look right and post fine?
            #    - can I post multiple results in one post?
            db.post("results",event_dict)

        except:
            print_dberr()




    #basic test: number submitted is equal to number of cands?
                

        
    ############## ############## ############## 
    ##############   H5 PLOTTER   ############## 
    ############## ############## ############## 
    if (db_on):
        tpp_state("your_h5plotter")
        
    try:
        do_your_h5plotter()
    except Exception as error:
        if (db_on):
            status = "ERROR in your_h5plotter: "+str(traceback.format_exc())
            tpp_state(status)
        else:
            logger.error(str(traceback.format_exc()))
        exit()

    

    ############## ############## ############## 
    ##############     WRAP-UP    ############## 
    ############## ############## ############## 
    time_end_UTC = datetime.utcnow()
    if (db_on):
        tpp_state("complete")
        try:
            data = {"job_end":time_end_UTC}
            db.patch("processing_outcomes",outcomesID,data=data)
        except:
            print_dberr()

    delta_time = time_end_UTC - time_start_UTC
    duration_minutes = int(delta_time.seconds()/60)

    logger.info(f"Job complete after {duration_minutes:d} minutes")

    # (All done).


    
    #TPPDB PUSH: CHECK OUTPUT DIRECTORY AND TELL TPPDB what's the best location.
    # Joe's thing: /tingle/data/results/survey/MJDint/####/(hd5 or png)

    #TPPDB CHECK: read results document and double check everything exists and is populated.
    #TPPDB: for the instances of db_err(), write any status points to a temporary file sought by error-scanner cron job.
