#!/usr/bin/python
import csv
from glob import glob
import codecs
import os
import sys

def parse_line(line):
        line_array=[]
        token_accumulator=''
        is_inside=False
        for char in line:
                if char == '~':
                        is_inside= not is_inside;
                elif char == ',' and  not is_inside:
                        line_array.append(token_accumulator)
                        token_accumulator=''
                elif char=='\n' or char=='\r':
			line_array.append(token_accumulator)
			token_accumulator=''
			return line_array
		else:
                        token_accumulator+=char
	return line_array

if len(sys.argv) > 1:
        directory = sys.argv[2]
	inputdir = sys.argv[1]
else:
        print('--------------------------------')
        print('Please provide input and output folders.')
	print('python sanitize_chrome.py <input dir> <output dir>')
        print('--------------------------------')
        sys.exit(0)

if not os.path.exists(directory):
    os.makedirs(directory)

paths = glob(inputdir + '*/')
filenames = [('vehicle_description.csv',False),('division_definition.csv',True),('model_definition.csv',True),('subdivision_definition.csv', True),('category_definition.csv', True),('engine.csv', False),('style.csv', False),('body_type.csv', False)]
for filename in filenames:
	index =0
        print('---- Sanitizing all ' + filename[0] + ' files ----')
        outfile = open(directory + '/'+ filename[0] , 'wb')
        print('##### Writing to file ' + directory + '/'+ filename[0])
        csvoutputfile = csv.writer(outfile, delimiter='|')
        for filenum, folder in enumerate(paths):
                if filenum > 0 and filename[1]:
                        continue
                skipcount = 0
                print('Processing folder ' + folder)
                curfile = open(folder + filename[0])
                if curfile is None:
                        continue
                tsplit = folder.split('/')
		topfolder = tsplit[len(tsplit) - 2]
		try:
			rownum = 0
			for line in curfile.readlines():
                                row = parse_line(line) 
				if rownum is 0:
					rownum += 1
                                        continue
				rownum += 1
                                row.append(topfolder)
                                csvoutputfile.writerow(row)
                except csv.Error:
                        skipcount += 1
                        continue
		index += rownum
        print('--- Skipped ' + str(skipcount) + ' ---')
        print('--- Processed ' + str(index) + ' rows ---')

