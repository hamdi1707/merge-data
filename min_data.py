# -*- coding: utf-8 -*-
"""
Created on Mon May  4 09:50:50 2020

@author: hamdi
"""


# --------------------------------------------------------------------
# -- my_data.py                                                     --
# --     Reads and merges NordPool data with temperature data       --
# --     Olvar Bergland, jan 2020                                   --
# --------------------------------------------------------------------

import sys
import os
from datetime import datetime
import numpy  as np
import pandas as pd


# --------------------------------------------------------------------
# -- price, consumption and production                              --
# --------------------------------------------------------------------

fn = '../data/np_data.csv'
if os.path.isfile(fn):
    dnpool = pd.read_csv(fn,header=[0],parse_dates=[0])
   
else:
    sys.exit('Could not open data file {}̈́'.format(fn))


# --------------------------------------------------------------------
# -- temperature data                                               --
# --------------------------------------------------------------------

fn = '../data/temp.csv'
if os.path.isfile(fn):
    dtemp = pd.read_csv(fn,header=[0],parse_dates=[0])
   
else:
    sys.exit('Could not open data file {}̈́'.format(fn))


# --------------------------------------------------------------------
# -- energy price data                                              --
# --   first date: 2014-01-13                                       --
# --   last  date: 2020-02-01                                       --
# --------------------------------------------------------------------

fn = '../data/eprice.csv'
if os.path.isfile(fn):
    eprice = pd.read_csv(fn,header=[0])
   
else:
    sys.exit('Could not open data file {}̈́'.format(fn))


# --------------------------------------------------------------------
# -- combine dataframes (and save as CSV file)                      --
# --------------------------------------------------------------------

#
# NordPool and temperature data

#Getting rid of error-code by doing df['Time'] = pd.to_datetime(df['Time'], utc = True) 
#on both the time columns before joining (or rather the one without UTC needs to go through this!)
dnpool['time'] = pd.to_datetime(dnpool['time'], utc = True)
dtemp['time'] = pd.to_datetime(dtemp['time'], utc = True)


df= pd.merge(dnpool,dtemp, on='time',how='left')

print(df.info())
print(eprice.info())

#
# add energy prices
df = df.merge(eprice, on='date', how='left')

#
# energy price available on trading days
#   fills in missing values, last observation is used
df = df.fillna(method='ffill')

#
# keep only the relevant time period
df = df[df.date > '2014-01-23']
df = df[df.date < '2020-02-01']


df.to_csv('../data/my_data.csv',index=False)