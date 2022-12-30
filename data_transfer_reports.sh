#!/bin/bash
mkdir /mnt/c/Users/Second/Desktop/project/reports/$(date '+%F')
mv /mnt/c/Users/Second/Desktop/project/reports/resultsfile.csv /mnt/c/Users/Second/Desktop/project/reports/$(date '+%F')/resultsfile$(date '+%F').csv
mv /mnt/c/Users/Second/Desktop/project/reports/resultsfile_test.csv /mnt/c/Users/Second/Desktop/project/reports/$(date '+%F')/resultsfile_test$(date '+%F').csv
