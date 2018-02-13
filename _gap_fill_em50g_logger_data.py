"""
NOTE: If you've downloaded the repository from Github, then you already
    have the necessary directory hierarchy and can skips for setting them up.

This script is used for gap-filling sensor data that comes off the Em50g data loggers
made by Decagon (now Meter Group). It does this:
    1. Iterates through a directory that has multiple logger data files.
    2. Extracts Logger ID and stores in each table (upper lefthand corner).
    3. Trims sensor data based on a  given Start and End date/time.
    4. If needed, it gapfills any missing time-points (with No-Data)
    5. Updates record/observation count (in upper lefthand corner).
    6. Outputs each logger data file (as csv or excel file: change in script if needed)

Gap-filling data to specific start and end times for all logger data coming off 
of a project will make it much easier to join them for comparison later.

This is needed b/c sensor data can have the following:
    1. Data gaps due to sensor failure or data logger failure.
    2. Different start dates/times due to when they were installed/removed.
    3. And a number of other reasons that may have caused time-gaps.
     
BEFORE RUNNING THIS SCRIPT:
    This script requires the following setup:
        1. A directory to be setup called: 'sensor-data'
        2. Insided 'sensor-data' create a directory called: 'raw-sensor-data'
        3. Download/import '.xls' logger data files to: 'raw-sensor-data'
        4. Your file names for each logger data file should:
            a. Start with your Logger ID: D3Y as example (D3Y-12Feb2018-0914.xls).
    
    Inside the script itself do the following:
        1. Change the string for 'path_to_raw_data' to the path on your local
            computer where you've put your 'raw-sensor-data' directory.
        2. Based on your particular study and when/how data was collected, change the 
            following variable strings to meet your needs (in the script below):
                start = '8/21/2017 15:00:00' (time-point when good data came in)
                end = '11/21/2017 10:00:00' (time-point when good data stopped coming in)
                frequency = 'H'  (H for hourly, D for daily, etc.)
                    
Note:
    1. The data coming off of the em50g is in a particular format ('wide table') that 
        has 3 headers above the data. This complicates processing and this script 
        deals with that issue. 
    2. The output from this script is still in a wide formatted table identical
        to the output from the logger. The difference is that it will be
        trimmed and gapfilled to when the loggers started and stopped
        recording data in the field.
    3. You will still need to clean and process the data for analysis.
    4. Because of the akward headings you will need to do the 
        following after running the script (if running output as excel files):
            a. Open each excel file.
            b. Change data-types to numbers instead of strings. """

# import packages
import numpy as np
import pandas as pd
import os, shutil, glob

############ UPDATE PATH TO WHERE YOU HAVE YOUR: raw-sensor-data ##############

############# See Instructions Above for Setting up your Directories  ##########

# path to where your raw em50g logger data resides ('raw-sensor-data')
path_to_raw_data = r'C:\Users\jdext\Desktop\em50g-data-logger-gap-fill-tool\sensor-data\raw-sensor-data'

# change working directory to: 'path_to_raw_data'
os.chdir(path_to_raw_data)

# move 1 step back in your directories to create new directories for output.
os.chdir('..')

# create directory for gap-filled data (i.e., final data outputs)
gap_filled_directory = 'gap-filled-sensor-data' # variable for output directory

if not os.path.exists(gap_filled_directory):  # this creates directory if it does not exist
    os.makedirs(gap_filled_directory)
else:                                   # this removes and recreates directory if it exists
    shutil.rmtree(gap_filled_directory)          
    os.makedirs(gap_filled_directory)
    
# create directory for intermediate data output (i.e., garbage)
intermediate_directory = 'intermediate-data' # variable for output directory

if not os.path.exists(intermediate_directory):  # this creates directory if it does not exist
    os.makedirs(intermediate_directory)
else:                                   # this removes and recreates directory if it exists
    shutil.rmtree(intermediate_directory)          
    os.makedirs(intermediate_directory)

# change working directory back to: 'path_to_raw_data'
os.chdir(path_to_raw_data)

'''Create a date_time_range that compliments the study trial period for when sensors were
collecting data. This date_time_range dataframe will be used to join, trim, and gap-fill 
logger data. Date-time range determined by install date/time and 
removal date/time of sensors and data loggers.'''

######## UPDATE START, END, FREQUENCY BASED ON YOUR NEEDS ############

# start and end date/time based on the beginning/end of your trial study period
start = '12/14/2017 17:00:00' # the date/time when your sensors starting recording data
end = '2/11/2018 18:00:00'# the date/time when your sensors stopped recording data
frequency = 'H' # this is the frequency that your data was collected (e.g., 'H', 'D', 'W')

# function for creating a 1 column dataframe with a time-series of when loggers collected data
'''this is used for joining to the raw data for the purpose of gap-filling missing rows.'''

def create_date_time_range(start, end, frequency='H'):  # default is hourly: 'H'
    time_series = pd.date_range(start, end, freq=frequency)
    date_range_series = pd.Series(time_series)
    date_time_range = pd.DataFrame(date_range_series)
    date_time_range.columns = ['date_time']
    date_time_range = date_time_range.set_index('date_time')
    
    return date_time_range  # returns time-series as a dataframe 

# run function and save output as dataframe in 'date_time_range'
date_time_range = create_date_time_range(start, end, frequency)

# create list of file pathways for loop to iterate over during processing 
''' uses pathname matching with '.xls' to get a list of pathways for each raw data logger file
This stores all logger data file pathways to a list so that it can be iterated over'''
logger_files = glob.glob(path_to_raw_data + '\*.xls')

#%%

# for-loop that takes a file and joins it to timeDate_df for gap-filling
for file in logger_files:
    
    '''This loop does the followign:
    1. Iterates through a directory that has multiple logger data files.
    2. Extracts Logger ID and stores in each table (upper lefthand corner).
    3. Trims sensor data based on a  given Start and End date/time.
    4. If needed, it gapfills any missing time-points (with No-Data)
    5. Updates record/observation count (in upper lefthand corner)'''
    
    # read in file as dataframes, one with headers and one without
    without_headers = pd.read_excel(file,  header=None) # no headers 
    with_headers = pd.read_excel(file,  header=2, mangle_dupe_cols=True) # with 3rd row as header
    
    # obtain logger ID from 'without_headers' by pulling from upper lefthand corner of dataframe
    logger_id = without_headers.iloc[0,0]  # extract file name from upper lefthand corner
    logger_id = logger_id.split('.')[0]    # remove things after '.' (xls)
    logger_id = logger_id.split('-')[0]    # extract logger ID by keeping everything before 1st hyphen
    
    # replace file in upper lefthand corner with Logger ID.
    without_headers.iloc[0,0] = logger_id
    
    # replace old count of records with new count from created time series
    without_headers.iloc[1,0] = str(len(date_time_range)) + ' records'
    
    # subset first three rows from 'without_headers' (these will be inserted as headers later)
    headers_for_insert = without_headers.iloc[0:3,:]

    # set index as date_time column for joining purposes
    with_headers = with_headers.set_index('Measurement Time')
    
    # Left hand join using created time-series as series to join on.
    join_for_gap_filling = date_time_range.join(with_headers)

    # remove '***' and replace with 'nan' (Not a Number(nan))
    # the '***' are from the sensor data and are added by data logger software
    join_for_gap_filling.replace('***', np.nan, inplace=True)  
    
    # reset index to prepare table for final processing steps
    join_for_gap_filling = join_for_gap_filling.reset_index()
    
    # change working directory to: 'intermediate-outputs'
    os.chdir('../' + intermediate_directory)
    
    # write 'headers_for_insert' to csv as a way to start building table (this is the 1st 2 headers)
    intermediate = logger_id + '_intermediate.csv'
    headers_for_insert.to_csv(intermediate, 
                              index=False, header=False, encoding='utf-8')
    
    # Open file with 3 headers for each file and add in 3rd header and subsequent data
    with open(intermediate, 'a', encoding='utf-8') as f:
        join_for_gap_filling.to_csv(f, index=False, header=True, encoding='utf-8')

    # Read in csv for gap-filled logger
    gap_filled = pd.read_csv(intermediate, header=None, encoding='utf-8')    
    gap_filled_final = gap_filled.drop(gap_filled.index[3])
    
    # change working directory to: 'gap-filled-sensor-data'
    os.chdir('../' + gap_filled_directory)
    
    # Write final gap-filled output to individual excel files (as xlsx)
    ## gap_filled_logger_data = logger_id + '_gap-filled-sensor-data.xlsx'
    ## gap_filled_final.to_excel(gap_filled_logger_data, sheet_name=logger_id, index=False, header=False)

    # Write final gap-filled output to individual excel files (as xlsx)
    gap_filled_logger_data = logger_id + '_gap-filled-sensor-data.csv'
    gap_filled_final.to_csv(gap_filled_logger_data, index=False, header=False, encoding='utf-8') 
 
# change working directory to: 'path_to_raw_data'
os.chdir(path_to_raw_data)

print('Dexter Solutions')

#########################################################################################


