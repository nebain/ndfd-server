#/usr/bin/env python

import sys, os
sys.path.insert(0, os.path.dirname(os.path.realpath(__file__)) + '/lib')

from flask import Flask, abort, request
from pysky import grib2
import threading
import argparse, random, re, shutil, string, sys

parser = argparse.ArgumentParser(description='Runs an NDFD server.')
parser.add_argument('--data', dest='data', required=True, help='Path to directory where local NDFD cache will be maintained.')
parser.add_argument('--degrib', dest='degrib', default='/usr/local/bin/degrib', help='Location of degrib executable. Default is %(default)s')
parser.add_argument('--geodata', dest='geodata', default='/usr/local/share/degrib/geodata', help='Location of degrib geodata directory. Default is %(default)s')
parser.add_argument('--sector', dest='sector', default='conus', help='Sector to use. Default is %(default)s. Specify a smaller region to reduce disk usage and to reduce update processing time. See http://www.nws.noaa.gov/ndfd/anonymous_ftp.htm for a list of available sectors.')

args = parser.parse_args()

grib2.degrib_path = args.degrib
grib2.base_url = 'http://weather.noaa.gov/pub/SL.us008001/ST.opnl/DF.gr2/DC.ndfd/AR.{}/'.format(args.sector)
grib2.geodata_path = args.geodata
grib2.noaa_params = 'ALL'
ndfd_grib_check_interval = 300

mutex = threading.Lock()
downloading_mutex = threading.Lock()
download_base = args.data
download_dir = download_base + '/active'

app = Flask(__name__)



def get_new_download_dir():
	rnd = ''.join(random.choice(string.ascii_letters + string.digits) for i in range(10))
	return download_base + '/' + rnd



@app.route('/ndfdXmlclient')
def ndfdXmlclient():
	try:
		# defaults
		product = 'time-series'
		begin = end = None
		elements = []
		for key, val in request.args.iteritems():
			key = key.lower()
			if key == 'lat':
				lat = float(val)
			elif key == 'lon':
				lon = float(val)
			elif key == 'product':
				if valid_product(val.lower()):
					product = val.lower()
			elif key == 'begin':
				if valid_datetime(val.upper()):
					begin = val.upper()
			elif key == 'end':
				if valid_datetime(val.upper()):
					end = val.upper()
			elif key == 'elements':
				elements = [e for e in val.lower().split(',') if valid_element(e)]

		if len(elements) == 0:
			elements = None
		with mutex:
			result = grib2.xml(download_dir, lat, lon, elements=elements, product=product, begin=begin, end=end)
		return result
	except:
		abort(400)



@app.route('/ndfdBrowserClientByDay')
def ndfdBrowserClientByDay():
	try:
		# defaults
		format = '12 hourly'
		for key, val in request.args.iteritems():
			key = key.lower()
			if key == 'lat':
				lat = float(val)
			elif key == 'lon':
				lon = float(val)
			elif key == 'format':
				if valid_format(val.lower()):
					format = val.lower()

		with mutex:
			result = grib2.xml_byday(download_dir, lat, lon, format=format)
		return result
	except:
		abort(400)



def valid_product(subject):
	return subject == 'time-series' or subject == 'glance'



def valid_format(subject):
	return re.match(r'^(12|24) hourly$', subject)



def valid_date(subject):
	return re.match(r'^\d\d\d\d-\d\d-\d\d$', subject)



def valid_datetime(subject):
	return re.match(r'^\d\d\d\d-\d\d-\d\dT\d\d:\d\d$', subject)



def valid_element(subject):
	return re.match(r'^[\w-]{2,15}$', subject)



@app.route('/update_cache')
def update_cache():
	new_download_dir = get_new_download_dir()
	lock_acquired = downloading_mutex.acquire(False)

	if not lock_acquired:
		print "Service Unavailable"
		abort(503) # Service Unavailable

	try:
		new_files = grib2.download(download_dir, new_download_dir)
		if new_files:
			old_download_dir = os.path.realpath(download_dir)
			with mutex:
				if os.path.exists(download_dir) and os.path.islink(download_dir):
					os.unlink(download_dir)
				elif os.path.exists(download_dir) and os.path.isdir(download_dir):
					shutil.rmtree(download_dir)
				os.symlink(new_download_dir, download_dir)
			if os.path.exists(old_download_dir) and not os.path.islink(old_download_dir):
				shutil.rmtree(old_download_dir)

		downloading_mutex.release()
	except:
		downloading_mutex.release()
		abort(500)

	if new_files:
		return ('Success - new files downloaded.', 200)
	else:
		return ('Success - no update needed.', 304)



def update_cache_timer():
	try:
		print "Automated NDFD grib update begin."
		msg, code = update_cache()
		sys.stdout.write("Automated NDFD grib update completed. ")
		if code == 200:
			print "New files downloaded."
		else:
			print "No new files."
	except:
		print "Automated NDFD grib update failed."

	update_timer = threading.Timer(ndfd_grib_check_interval, update_cache_timer)
	update_timer.daemon = True
	update_timer.start()



if __name__ == '__main__':
	update_cache_timer()
	app.run(host='0.0.0.0', threaded=True)
