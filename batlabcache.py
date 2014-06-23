"""This file implements an on-disk cache for batlab run data.  Top level 
data is pulled via month.
"""
from __future__ import print_function
import os
import io
import sys
import time

if sys.version_info[0] > 2:
    from urllib.parse import urlencode
    from urllib.request import urlopen
else:
    from urllib import urlencode
    from urllib2 import urlopen

def datespace(starty, startm, stopy, stopm):
    """generator of year and month tuples on inclusive bounds."""
    for year in range(starty, stopy+1):
        month1 = startm if year == starty else 1
        month12 = stopm if year == stopy else 12
        for month in range(month1, month12 + 1):
            yield year, month

class BatlabCache(object):

    overview_base_url = "http://submit-1.batlab.org/nmi/results/overview?"
    overview_base_query = {'storedSearch': 0}

    def __init__(self, year, month, username="", cachedir="cache"):
        self.username = username
        self.cachedir = cachedir = os.path.abspath(cachedir)
        if not os.path.isdir(cachedir):
            os.makedirs(cachedir)
        usr = username + "_" if username else ""
        self.month_filename = os.path.join(cachedir, usr + "{year}-{month:02}.html")

        now = time.gmtime()
        self.ensure_dates(year, month, now.tm_year, now.tm_mon)
        self.download_month(now.tm_year, now.tm_mon)

    def ensure_dates(self, start_year, start_month, stop_year, stop_month):
        """Ensures monthly data files have been downloaded."""
        for year, month in datespace(start_year, start_month, stop_year, stop_month):
            fname = self.month_filename.format(year=year, month=month)
            if os.path.exists(fname):
                continue
            self.download_month(year, month)

    def download_month(self, year, month, retry=3):
        """Downloads a month summary file."""
        url = self.overview_url(year, month)
        fname = self.month_filename.format(year=year, month=month)
        t1 = time.time()
        print("downloading " + url)
        try:
            with urlopen(url) as r:
                page = r.read().decode('utf-8')
        except ConnectionResetError:
            if retry > 0:
                print("...failed to download file, retying {0}".format(retry))
                self.download_month(year, month, retry=retry-1)
            else:
                print("...failed to download file. Maximum retries reached.")
                raise
        with io.open(fname, 'w') as f:
            f.write(page)
        print("...saved as {0} in {1:.1} s".format(fname, time.time() - t1))

    def overview_url(self, year, month):
        query = dict(self.overview_base_query)
        query['user'] = self.username
        date = 'between {year}-{month}-01 and {nextyear}-{nextmonth}-01'
        nextyear, nextmonth = (year+1, 1) if month == 12 else (year, month+1)
        query['date'] = date.format(year=year, month=month, 
                                    nextyear=nextyear, nextmonth=nextmonth)
        return self.overview_base_url + urlencode(query)

