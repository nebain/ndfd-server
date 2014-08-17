#/usr/bin/env python

import sys, os
sys.path.insert(0, os.path.dirname(os.path.realpath(__file__)) + '/lib')

from flask import Flask, abort
from pysky import grib2
from threading import Lock
import random, re, shutil, string

grib2.degrib_path = '/usr/local/bin/degrib'
grib2.noaa_params = ['maxt', 'mint', 'wspd', 'wdir']
grib2.geodata_path = '/usr/local/share/degrib/geodata'

mutex = Lock()
downloading_mutex = Lock()
download_base = '/home/nebain/ndfd/auto/'
download_dir = download_base + '/active'

def get_new_download_dir():
	rnd = ''.join(random.choice(string.ascii_letters + string.digits) for i in range(10))
	return download_base + '/' + rnd

app = Flask(__name__)

@app.route('/forecast/<lat>/<lon>', defaults={'params': None})
@app.route('/forecast/<lat>/<lon>/<params>')
def forecast(lat, lon, params):
	try:
		lat = float(lat)
		lon = float(lon)
		params_array = []
		if params:
			for p in params.split(','):
				if re.search(r'^[\w-]+$', p):
					params_array.append(p)
		if len(params_array) == 0:
			params_array = None
		mutex.acquire()
		result = grib2.xml(download_dir, lat, lon, params_array)
		mutex.release()
		return result
	except:
		mutex.release()
		abort(400)

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

if __name__ == '__main__':
	print "Updating local NDFD file cache..."
	new_download_dir = get_new_download_dir()
	new_files = grib2.download(download_dir, new_download_dir)
	app.run(host='0.0.0.0', threaded=True)#, port=80)
