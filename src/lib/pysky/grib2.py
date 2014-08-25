from pysky import utils

# Base URL for downloading grib2 files
base_url = 'http://weather.noaa.gov/pub/SL.us008001/ST.opnl/DF.gr2/DC.ndfd/AR.conus'
noaa_params = ['maxt', 'temp', 'mint', 'pop12', 'sky', 'wspd', 'apt', 'qpf', 'snow', 'wx', 'wgust', 'icons', 'rhm']

# Degrib path
degrib_path = '/usr/local/bin/degrib'
geodata_path = None

def download_command_line():
    """ Handle download from command-line """
    from optparse import OptionParser

    usage = "usage:\n%prog download [options]"
    parser = OptionParser(usage)

    parser.add_option('-g', '--grib2-dir', dest='grib2_dir',
        action='store',
        help='Directory to download grib2 files to')
    parser.add_option('-v', '--verbose', dest='verbose', default=False,
        action='store_true',
        help='Show verbose output')

    (options, args) = parser.parse_args()

     # use current dir if none provided
    grib2_dir = options.grib2_dir if options.grib2_dir else os.path.abspath(os.path.dirname(__file__))
    utils.verbose = options.verbose
    download(options.grib2_dir)

def download(data_dir, new_data_dir=None):
    """
    Download grib2 files to data directory

    args:
        data_dir     Old directory containing existing data files
        new_data_dir Directory to save new data files. This will only be
                     created if files are downloaded. You may remove the
                     old data_dir after calling this function. Pass
                     None to indicate files shall be updated in-place.
    returns:
        True if new files were downloaded, False otherwise
    """
    import urllib2, re, os, sys, time, urllib2, dateutil, shutil
    from datetime import datetime
    from dateutil import tz
    from dateutil.parser import parse

    if not new_data_dir:
        new_data_dir = data_dir

    files_downloaded = False # whether files have been downloaded

    files_to_copy = []

    # Loop over directories that have forecast data files
    for dir in ['VP.001-003','VP.004-007']: # loop over remote directories

        new_data_subdir = "{0}/{1}".format(new_data_dir, dir)

        utils.info('\nChecking directory {0}'.format(dir))

        # To save time, first check to see whether the directory listing file
        # itself was updated.
        check_local_path = "{0}/{1}/{2}".format(data_dir, dir, "ls-l")
        save_local_path = "{0}/{1}/{2}".format(new_data_dir, dir, "ls-l")
        ls_local_time = os.stat(check_local_path).st_mtime if os.path.exists(check_local_path) else 0
        utils.info("Local: {0} last modified {1}".format(check_local_path, ls_local_time))

        ls_request = urllib2.urlopen("{0}/{1}/ls-l".format(base_url, dir))
        last_modified_str = ls_request.info()['Last-Modified']
        ls_remote_time = _utc2local(parse(last_modified_str))
        utils.info("Remote: {0} last modified {1}".format("ls-l", ls_remote_time))

        # If it was, download it and save it to the new directory
        ls_downloaded = False
        if not os.path.exists(check_local_path) or ls_local_time < ls_remote_time:
            ls_downloaded = True
            utils.info("Saving new ls-l file")
            if not os.path.exists(new_data_subdir):
                os.makedirs(new_data_subdir)
            _download_file(ls_request, save_local_path)
            os.utime(save_local_path, (ls_remote_time, ls_remote_time))
            files_downloaded = True
            ls_file = save_local_path
        else: # If not, remember it to be copied into new directory later if needed
            files_to_copy.append((check_local_path, save_local_path))
            ls_file = check_local_path

        # Loop over each file in the directory listing
        for line in open(ls_file):
            # Check file modified date if this is a .bin file
            if line.find(".bin") != -1:
 
                # Split line to get date and filename
                month, day, rtime, filename = re.split("\s+", line)[5:9]
 
                # Split filename to get noaa param name
                param = filename.split('.')[1]
 
                # Only download files if we are interested in this parameter
                if noaa_params == 'ALL' or param in noaa_params:
 
                    # Local path and time
                    check_local_path = "{0}/{1}/{2}".format(data_dir, dir, filename)
                    save_local_path = "{0}/{1}/{2}".format(new_data_dir, dir, filename)

                    if ls_downloaded: # Only bother checking if we downloaded a new ls file
                        local_time = os.stat(check_local_path).st_mtime if os.path.exists(check_local_path) else 0
                        utils.info("Local: {0} last modified {1}".format(check_local_path, local_time))
 
                        # Remote path and time
                        remote_path = "{0}/{1}/{2}".format(base_url, dir, filename)
                        request = urllib2.urlopen(remote_path)
                        last_modified_str = request.info()['Last-Modified']
                        remote_time = _utc2local(parse(last_modified_str))
                        utils.info("Remote: {0} last modified {1}".format(remote_path, remote_time))
 
		        # If file does not exist or the local file is older than the remote file, download
                        if not os.path.exists(check_local_path) or local_time < remote_time:
                            utils.info('Downloading remote file {0}'.format(remote_path))
                            _download_file(request, save_local_path)
                            os.utime(save_local_path, (remote_time, remote_time))
                            files_downloaded = True
                        else: # Otherwise, remember this file for if it needs to be copied later
                            files_to_copy.append((check_local_path, save_local_path))
                            utils.info('Local file is up-to-date, skipping download')
                    else: # ls was not downloaded, so just remember the files for later copying if needed
                        files_to_copy.append((check_local_path, save_local_path))
                        utils.info('Local file is up-to-date, skipping download')
                    
    # Cube data files if any were downloaded
    if files_downloaded:
        if data_dir != new_data_dir:
            # Copy the files remembered earlier into the new data directory
            for src, dst in files_to_copy:
                dst_dir = os.path.dirname(dst)
                if not os.path.exists(dst_dir):
                    os.makedirs(dst_dir)
                shutil.copy2(src, dst)
                utils.info("copied {0} to {1}".format(src, dst))

        cmd = "{degrib} {data_dir}/VP.001-003/*.bin {data_dir}/VP.004-007/*.bin -Data -Index {data_dir}/all.ind -out {data_dir}/all.dat".format(
            degrib = degrib_path,
            data_dir = new_data_dir
        )
        utils.info(cmd)
        output = ""
        for line in os.popen(cmd).readlines():
            output += line
        utils.info(output)
    else:
        utils.info('No files downloaded - skipping cube')
    return files_downloaded

def xml(data_dir, latitude, longitude, elements=None, product='time-series', begin=None, end=None):
    """
    Generate XML file from grib2 data cube. Arguments are similar to what is
    expected in the NWS NDFD REST API for the ndfdXMLclient.php interface:
    http://graphical.weather.gov/xml/rest.php

    args:
        data_dir - Directory where grib2 data cube is located (required)
        latitude - Latitude (required)
        longitude - Longitude (required)
        elements - List of elements, or None to return all params
        product - time-series or glance
        begin - begin time, or None to mean beginning of available period
        end - end time, or None to mean end of available period

    returns - xml string
    """

    import os

    geodata = geodata_path if geodata_path else data_dir + '/geodata'

    # build and execute command
    cmd = "{degrib_path} {data_dir}/all.ind -DP -pnt {latitude},{longitude} -geoData {geodata}".format(
        degrib_path=degrib_path, data_dir=data_dir,
        latitude=latitude, longitude=longitude,
        geodata=geodata)
    if product == "time-series":
        cmd += " -XML 1"
        if elements:
            cmd += " -ndfdConven 1 -ndfdVars " + ",".join(elements)
    elif product == "glance":
        cmd += " -XML 2"

    if begin:
        cmd += " -startTime " + begin
    if end:
        cmd += " -endTime " + end

    utils.info(cmd)
    xml = ""
    for line in os.popen(cmd).readlines():

        xml += line

    # return output
    return xml

def xml_byday(data_dir, latitude, longitude, format='12 hourly'):
    """
    Generate XML file from grib2 data cube. Arguments are similar to what is
    expected in the NWS NDFD REST API for the ndfdBrowserClientByDay.php interface:
    http://graphical.weather.gov/xml/rest.php

    args:
        data_dir - Directory where grib2 data cube is located (required)
        latitude - Latitude (required)
        longitude - Longitude (required)
        format - "12 hourly" or "24 hourly"

    returns - xml string
    """

    import os

    geodata = geodata_path if geodata_path else data_dir + '/geodata'

    # build and execute command
    cmd = "{degrib_path} {data_dir}/all.ind -DP -pnt {latitude},{longitude} -geoData {geodata}".format(
        degrib_path=degrib_path, data_dir=data_dir,
        latitude=latitude, longitude=longitude,
        geodata=geodata)
    if format == "12 hourly":
        cmd += " -XML 3"
    elif format == "24 hourly":
        cmd += " -XML 4"

    utils.info(cmd)
    xml = ""
    for line in os.popen(cmd).readlines():

        xml += line

    # return output
    return xml

def _utc2local(utc):
    """ Convert utc datetime object to local datetime """
    from dateutil import tz
    import time
    from_zone = tz.tzutc()
    to_zone = tz.tzlocal()
    utc = utc.replace(tzinfo = from_zone)
    return time.mktime(utc.astimezone(to_zone).timetuple())

def _download_file(request, local_path):
    """ Download file given a urllib2 urlopen from remote path to local path """
    import sys
    content_length = int(request.info()['Content-Length'])
    chunk_size = 8192

    with open(local_path, 'wb') as f:
        #next_percentage = 5.0
        chunk = request.read(chunk_size)
        downloaded = 0
        while chunk:
            f.write(chunk)
            downloaded += chunk_size
            #if (float(downloaded) / float(content_length)) * 100.0 > next_percentage:
            #    sys.stdout.write("\r")
            #    sys.stdout.write("{1}% complete {0}".format('#' * (int(next_percentage) / 5), int(next_percentage)))
            #    next_percentage += 5.0
            #    #sys.stdout.write('#')
            #    sys.stdout.flush()
            chunk = request.read(chunk_size)
        #print('')
