#!/usr/bin/python
import csv
from glob import glob
import codecs
import os
import sys

if len(sys.argv) > 1:
        directory = sys.argv[1]
else:
        print('--------------------------------')
        print('Please provide an output folder.')
        print('--------------------------------')
        sys.exit(0)

if not os.path.exists(directory):
    os.makedirs(directory)

paths = glob('*/')
filenames = [('division_definition.csv',True),('model_definition.csv',True),('subdivision_definition.csv', True),('category_definition.csv', True),('engine.csv', False),('style.csv', False),('vehicle_description.csv',False),('body_type.csv', False)]
for filename in filenames:
        print('---- Sanitizing all ' + filename[0] + ' files ----')
        index = 0
        outfile = open(directory + '/'+ filename[0] , 'wb')
        print('##### Writing to file ' + directory + '/'+ filename[0])
        csvoutputfile = csv.writer(outfile, delimiter=',')
        for filenum, folder in enumerate(paths):
                if filenum > 0 and filename[1]:
                        continue
                skipcount = 0
                print('Processing folder ' + folder)
                curfile = open(folder + filename[0])
                if curfile is None:
                        continue
                csvfile = csv.reader(curfile)
                try:
                        if csvfile is None:
                                continue
                        for rownum, row in enumerate(csvfile):
                                if rownum is 0:
                                        continue
                                else:
                                        index+=1
                                newrow = []
                                for i, col in  enumerate(row):
                                        newcol = col.replace("~", "")
                                        if i is 0 and rownum > 0 and filename[1] is False:
                                                newrow.append(index)
                                        else:
                                                newrow.append(newcol)
                                newrow.append(folder)
                                csvoutputfile.writerow(newrow)
                except csv.Error:
                        skipcount += 1
                        continue
        print('--- Skipped ' + str(skipcount) + ' ---')
        print('--- Processed ' + str(index) + ' rows ---')

