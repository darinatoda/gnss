DIR=/jail/dari/project/src/RTKLIB

detect_gnss_timestamp: detect_gnss_timestamp.c
	gcc -Wall -O3 -ansi -pedantic -I$(DIR)/src -DTRACE -DENAGLO -DENAQZS   -o detect_gnss_timestamp detect_gnss_timestamp.c $(DIR)/src/rtkcmn.c $(DIR)/src/rinex.c $(DIR)/src/ionex.c $(DIR)/src/preceph.c  -lm
