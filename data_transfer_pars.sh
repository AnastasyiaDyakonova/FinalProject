#!/bin/bash
mkdir /mnt/c/Users/Second/Desktop/project/data_end/$(date '+%F')
mv /mnt/c/Users/Second/Desktop/project/lenta_ru.csv /mnt/c/Users/Second/Desktop/project/data_end/$(date '+%F')/lenta_ru$(date '+%F').csv
mv /mnt/c/Users/Second/Desktop/project/kommersant_ru.csv /mnt/c/Users/Second/Desktop/project/data_end/$(date '+%F')/kommersant_ru$(date '+%F').csv
mv /mnt/c/Users/Second/Desktop/project/vedomosti_ru.csv /mnt/c/Users/Second/Desktop/project/data_end/$(date '+%F')/vedomosti_ru$(date '+%F').csv