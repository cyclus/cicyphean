"""This file implements an on-disk cache for batlab run data.  Top level 
data is pulled via month.
"""
from __future__ import print_function
import os
import io
import sys
import time
from datetime import datetime
from html.parser import HTMLParser

if sys.version_info[0] > 2:
    from urllib.parse import urlencode
    from urllib.request import urlopen
else:
    from urllib import urlencode
    from urllib2 import urlopen

import numpy as np
import pandas


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

    def download_this_month(self):
        now = time.gmtime()
        self.download_month(now.tm_year, now.tm_mon)

    def overview_url(self, year, month):
        query = dict(self.overview_base_query)
        query['user'] = self.username
        date = 'between {year}-{month}-01 and {nextyear}-{nextmonth}-01'
        nextyear, nextmonth = (year+1, 1) if month == 12 else (year, month+1)
        query['date'] = date.format(year=year, month=month, 
                                    nextyear=nextyear, nextmonth=nextmonth)
        return self.overview_base_url + urlencode(query)

    def overview(self, year, month, stop_year=None, stop_month=None):
        """Creates a pandas DataFrame for a month or a range of months."""
        dfs = []
        stop_year = stop_year or year
        stop_month = stop_month or month
        for y, m in datespace(year, month, stop_year, stop_month):
            fname = self.month_filename.format(year=y, month=m)
            op = OverviewParser(convert_charrefs=True)
            op.parse(fname)
            dfs.append(pandas.DataFrame(op.data))
        df = dfs[0]
        if len(dfs) > 1:
            for d in dfs[1:]:
                df = df.append(d)
        return df


null = lambda x: x
respace = lambda s: s.replace('\xa0', ' ')

class OverviewParser(HTMLParser):

    statuses = frozenset([
        'tableRow0',
        'tableRow1',
        'tableRow0StatusInProgress',
        'tableRow1StatusInProgress',
        'tableRow0StatusInternalError',
        'tableRow1StatusInternalError',
        'tableRow0StatusSucceeded',
        'tableRow1StatusSucceeded',
        'tableRow0StatusFailed',
        'tableRow1StatusFailed',
        'tableRow0StatusRemoved',
        'tableRow1StatusRemoved',
        'tableRow0StatusNotCompleted',
        'tableRow1StatusNotCompleted',
        'tableRow0StatusError',
        'tableRow1StatusError',
        'tableRow0StatusTimedOut',
        'tableRow1StatusTimedOut',
        ])

    fields = (None, 'id', 'result', 'user', 'type', 'project', None,
              #'project_version', 'component' , 'component_version', 
              'start', 'duration', 'description', 'platforms')

    colparsers = {
        'id': lambda s: int(s.split('>')[1].split('<')[0] if '>' in s else s),
        'result': respace,
        'user': null,
        'type': null,
        'project': null,
        'project_version': null, 
        'component': null,
        'component_version': null,
        'start': lambda s: datetime.strptime(s, "%Y-%m-%d %H:%M:%S"),
        'duration': lambda s: sum([int(x)*d for x, d in \
                                   zip(s.split(':'), [3600, 60, 1])]),
        'description': respace,
        'platforms': respace,
        }

    def reset(self):
        """Sort of a constructor."""
        super(OverviewParser, self).reset()
        self.data = {
            'id': [],
            'result': [],
            'user': [],
            'type': [],
            'project': [],
            #'project_version': [], 
            #'component': [],
            #'component_version': [],
            'start': [],
            'duration': [],
            'description': [],
            'platforms': [],
            }
        self.inrow = False
        self.col = -1
        self.incol = False
        self.nrows = 0
        self.coldata = None

    def parse(self, fname):
        """parses a file"""
        with io.open(fname) as f:
            s = f.read()
        self.feed(s)

    def handle_starttag(self, tag, attrs):
        if tag == 'tr':
            attrs = dict(attrs)
            if 'class' not in attrs:
                return
            if attrs['class'] not in self.statuses:
                return
            self.inrow = True
            self.nrows += 1
            self.col = -1
        elif self.inrow and tag == 'td':
            self.col += 1
            self.incol = True
            self.coldata = None

    def handle_data(self, data):
        if self.inrow and self.incol:
            col = self.col
            colname = self.fields[col]
            if colname is None:
                return
            if self.coldata is None:
                self.coldata = ''
            self.coldata += data

    def handle_endtag(self, tag):
        if tag == 'tr' and self.inrow:
            self.inrow = False
            self.col = -1
            return 
        elif self.inrow and tag == 'td':
            self.incol = False
            col = self.col
            colname = self.fields[col]
            if colname is None:
                return
            coldata = None if self.coldata is None \
                           else self.colparsers[colname](self.coldata)
            self.data[colname].append(coldata)
