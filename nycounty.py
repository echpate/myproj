#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Oct 20 18:20:19 2021

@author: hp
#grep 'row-' row.json | sed 's/[][]//g; s/"data" :  //g; ' |  sed 's/^...//' > row.csv
#n=`echo \`wc -l < row.csv\`/5 | bc`
#split -l $n row.csv countydata_part_
numFiles=`ls countyd* | wc -l`
"""
import json
import pandas as pd
import sqlite3
from sqlite3 import Error
from urllib.request import urlopen
import multiprocessing as mp
from datetime import datetime
import os

#conn = None
_dsqb='countycases.db'

def connect_to_db(dbpath): 
    conn = None
    try:
        conn = sqlite3.connect(dbpath)
        return conn
    except Error as err:
        print(err)
        if conn is not None:
            conn.close()

def getcountydata(df):
    countyset = df['county'].unique()
    countyset = [ s.replace(" ","").replace(".","") for s in countyset]
    conn = sqlite3.connect(_dsqb)
    cntProcessed=0
    for county in countyset: 
        thisCountydf=df[df['county']==county].copy()
        df.drop( df[df['county']==county].index, inplace = True) 
        thisCountydf.to_sql(name=county, con=conn, if_exists='append', index=False) 
        cntProcessed+=len(thisCountydf)
        del thisCountydf

    print("pid={} \t\t Processed={}".format(os.getpid(), cntProcessed)) 
    

def df_chunking(df, chunksize): 
    count = 0 # Counter for chunks
    while len(df):
        count += 1
        print('Preparing chunk {}'.format(count))
        # Return df chunk
        yield df.iloc[:chunksize].copy()
        # Delete data in place because it is no longer needed
        df.drop(df.index[:chunksize], inplace=True)


def main():
    
    # Can convert below to arguments
    n_jobs = 5  # Poolsize
    chunksize = 5000  # Maximum size of Frame Chunk
    #input_file='inputfile_part_aa'
    url = "https://health.data.ny.gov/api/views/xdss-u53e/rows.json?accessType=DOWNLOAD"
    #sqlite_db = 'countycases.db'
    
    col_list=['tdate','county','newPositives','cumPositives','TotalTests','TotalCumTests']
    response = urlopen(url)
    df = json.loads(response.read())['data']

    #input_file = open (input_file)
    #json_array = json.load(input_file)
    ##df = json_array['data']  
    
    df = pd.DataFrame([ [x[8], x[9], x[10], x[11], x[12], x[13]] for x in df], columns=col_list )
    
    start = datetime.now()
    #conn=connect_to_db('countycases.db')
    
    ctx = mp.get_context('spawn')
    pool = ctx.Pool(n_jobs)

    print('Starting...')

    pool.imap(getcountydata, df_chunking(df, chunksize))
    
    pool.close()
    pool.join()

    print('Elapsed: {}'.format(datetime.now()-start))

if __name__ == '__main__':
    main()


#---- Output

#Starting...
#Preparing chunk 1
#Preparing chunk 2
#Preparing chunk 3
#Preparing chunk 4
#Preparing chunk 5
#Preparing chunk 6
#Preparing chunk 7
#Preparing chunk 8
#pid=37704                Processed=4402
#pid=37704                Processed=2076
#pid=37705                Processed=5000
#pid=37705                Processed=5000
#pid=37701                Processed=5000
#pid=37701                Processed=4402
#pid=37703                Processed=5000
#pid=37702                Processed=5000
#Elapsed: 0:00:01.299750