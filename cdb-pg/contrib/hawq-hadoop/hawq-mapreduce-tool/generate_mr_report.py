#!/usr/bin/env python

############################################################################
# A script to generate pulse readable report.

infile = file('inputformat.logs')
outfile = file('inputformat.report','w')

testsuitename = ''
outline = ''
success = False

line = infile.readline()
while line:
    if line.find('Executing test case: ')!=-1:
        if len(outline) != 0:
            if success:
                outline = outline + "|Test Status|PASS"
            else:
                outline = outline + "|Test Status|FAILED"
            outfile.write(outline+'\n')

    	outline="Test Suite Name|" + testsuitename + "|Test Case Name|"
        success = False
        startIndex = line.find('Executing test case: ') + len('Executing test case: ')
    	endIndex=len(line)
        testcasename = line[startIndex:endIndex-1]
        outline = outline + testcasename + "|Test Detail|" + testcasename + " (0.00 ms)"

    elif line.find('Executing test suite: ')!=-1:
        startIndex = line.find('Executing test suite: ') + len('Executing test suite: ')
        endIndex = len(line)
        testsuitename = line[startIndex:endIndex-1]
 
    elif line.find('Successfully finish test case: ')!=-1:
        success = True
  
    line = infile.readline()

if len(outline) != 0:
    if success:
        outline = outline + "|Test Status|PASS"
    else:
        outline = outline + "|Test Status|FAILED"
    outfile.write(outline+'\n')

outfile.flush()
infile.close()
outfile.close()

