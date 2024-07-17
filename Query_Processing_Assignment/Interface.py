#!/usr/bin/python3


import psycopg2
import os
import sys


DATABASE_NAME='dds_assignment'
RATINGS_tableName='ratings'
RANGE_TABLE_PREFIX='range_part'
RROBIN_TABLE_PREFIX='rrobin_part'
RANGE_QUERY_OUTPUT_FILE='RangeQueryOut.txt'
PONT_QUERY_OUTPUT_FILE='PointQueryOut.txt'
RANGE_RATINGS_METADATA_TABLE ='rangeratingsmetadata'
RROBIN_RATINGS_METADATA_TABLE='roundrobinratingsmetadata'

def execute_and_append1(cursor, tableName, ratingMinValue, ratingMaxValue, lines):
    cursor.execute(f"SELECT * FROM {tableName} WHERE rating>={ratingMinValue} AND rating<={ratingMaxValue}")
    for row in cursor.fetchall():
        lines.append(f"{tableName},{row[0]},{row[1]},{row[2]}")

def execute_and_append2(cursor, tableName, ratingValue, lines):
    cursor.execute(f"SELECT * FROM {tableName} WHERE rating = {ratingValue}")
    for row in cursor.fetchall():
        lines.append(f"{tableName},{row[0]},{row[1]},{row[2]}")

# Donot close the connection inside this file i.e. do not perform openconnection.close()
def RangeQuery(ratingsTableName, ratingMinValue, ratingMaxValue, openconnection):
    cursor = openconnection.cursor()
    lines = []
    
    cursor.execute(f"SELECT partitionnum FROM {RANGE_RATINGS_METADATA_TABLE} WHERE maxrating>={ratingMinValue} AND minrating<={ratingMaxValue}")
    for row in cursor.fetchall():
        execute_and_append1(cursor, f"{RANGE_TABLE_PREFIX}{row[0]}", ratingMinValue, ratingMaxValue, lines)
    
    cursor.execute(f"SELECT partitionnum From {RROBIN_RATINGS_METADATA_TABLE}")
    for j in range(cursor.fetchone()[0]):
        execute_and_append1(cursor, f"{RROBIN_TABLE_PREFIX}{j}", ratingMinValue, ratingMaxValue, lines)

    writeToFile(RANGE_QUERY_OUTPUT_FILE, lines)

def PointQuery(ratingsTableName, ratingValue, openconnection):
    cursor = openconnection.cursor()
    lines = []

    cursor.execute(f"SELECT partitionnum FROM {RANGE_RATINGS_METADATA_TABLE} WHERE maxrating>={ratingValue} AND minrating<={ratingValue}")
    for row in cursor.fetchall():
        execute_and_append2(cursor, f"{RANGE_TABLE_PREFIX}{row[0]}", ratingValue, lines)

    cursor.execute(f"SELECT partitionnum From {RROBIN_RATINGS_METADATA_TABLE}")
    for j in range(cursor.fetchone()[0]):
        execute_and_append2(cursor, f"{RROBIN_TABLE_PREFIX}{j}", ratingValue, lines)

    writeToFile(PONT_QUERY_OUTPUT_FILE, lines)

def writeToFile(filename, rows):
    f = open(filename, 'w')
    for line in rows:
        f.write(line)
        f.write('\n')
    f.close()
