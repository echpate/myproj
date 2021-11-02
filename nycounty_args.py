#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Oct 20 18:20:19 2021

@author: hp
#grep 'row-' row.json | sed 's/[][]//g; s/"data" :  //g; ' |  sed 's/^...//' > row.csv
#n=`echo \`wc -l < row.csv\`/5 | bc`
#split -l $n row.csv countydata_part_
numFiles=`ls countyd* | wc -l`
for r in `ls countyd*`
do 
     ( python nycounty_args.py "$r" >> status.log ) &
     pid=$!
     pids+=($pid)
done & 
wait
echo "${pids[*]}" >> status.log
echo "All done.." >> status.log
"""
import pandas as pd
import sqlite3
from datetime import datetime
import sys
from os.path import exists

#conn = None
_dsqb='countycases2.db'

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

    print("Processed={}".format(cntProcessed)) 


def main():
    
    if(len(sys.argv)<=1):
        sys.exit("Data file not inlcuded in the argument. Inlcude Datafilename as argument")  
    
    if (not exists(sys.argv[1])):
        sys.exit("{} file does not exists".format(sys.argv[1]))
    
    print("Starting...{}".format(sys.argv[1])) 

    _col_list=['tdate','county','newPositives','cumPositives','TotalTests','TotalCumTests']
    lastNcolumns=-6
    
    df = pd.read_csv(sys.argv[1], skipinitialspace=True)    
    #df = pd.read_csv("countydata_part_aa", skipinitialspace=True)  
 
    df  = df.iloc[: , lastNcolumns:]
    df.columns = _col_list

    start = datetime.now()
    
    getcountydata(df)

    print('Elapsed: {}'.format(datetime.now()-start))

if __name__ == '__main__':
    main()


