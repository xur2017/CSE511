#!/usr/bin/python3
#
# Interface for the assignement
#

import psycopg2

def getOpenConnection(user='postgres', password='1234', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadRatings(ratingstablename, ratingsfilepath, openconnection):
    with open(ratingsfilepath, 'r') as f:
        lines = f.readlines()
        cur = openconnection.cursor()
        cur.execute(f"CREATE TABLE IF NOT EXISTS {ratingstablename} (userid INT, movieid INT, rating NUMERIC)")
        for line in lines:
            row = line.split('::')
            cur.execute(f"INSERT INTO {ratingstablename} (userid, movieid, rating) VALUES ({int(row[0])}, {int(row[1])}, {float(row[2])})")
        
        cur.close()


def rangePartition(ratingstablename, numberofpartitions, openconnection):
    with openconnection.cursor() as cur:
        delta = 5.0 / numberofpartitions
        for i in range(0, numberofpartitions):
            lb = i*delta
            ub = (i+1)*delta
            if i == 0: 
                cur.execute(f"CREATE TABLE range_part{i} AS SELECT * FROM {ratingstablename} WHERE Rating >= {lb} AND Rating <= {ub}")
            else:
                cur.execute(f"CREATE TABLE range_part{i} AS SELECT * FROM {ratingstablename} WHERE Rating > {lb} AND Rating <= {ub}")
        
        cur.close()


def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    with openconnection.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name LIKE 'range_part%'")
        numberofpartitions = cur.fetchone()[0]
        delta = 5.0 / numberofpartitions
        i = int(rating / delta)
        if rating % delta == 0 and i != 0:
            i -= 1
        cur.execute(f"INSERT INTO range_part{i} (userid, movieid, rating) VALUES ({userid}, {itemid}, {rating})")
        
        cur.close()


def roundRobinPartition(ratingstablename, numberofpartitions, openconnection):
    with openconnection.cursor() as cur:
        i_list = list(range(numberofpartitions))
        for i in i_list:
            cur.execute(f"CREATE TABLE rrobin_part{i} (userid INT, movieid INT, rating NUMERIC)")
            cur.execute(f"INSERT INTO rrobin_part{i} SELECT userid, movieid, rating FROM (SELECT *, row_number() OVER() as rnum FROM {ratingstablename}) WHERE rnum % {numberofpartitions} = {i}")

        cur.close()


def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    with openconnection.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name LIKE 'rrobin_part%'")
        numberofpartitions = cur.fetchone()[0]
        cur.execute(f"SELECT COUNT(*) FROM {ratingstablename};")
        numberofrecords = cur.fetchone()[0]
        i = numberofrecords % numberofpartitions
        cur.execute(f"INSERT INTO rrobin_part{i} (userid, movieid, rating) VALUES ({userid}, {itemid}, {rating})")

        cur.close()


def createDB(dbname='dds_assignment'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getOpenConnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print('A database named {0} already exists'.format(dbname))

    # Clean up
    cur.close()
    con.close()

def deletepartitionsandexit(openconnection):
    cur = openconnection.cursor()
    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
    l = []
    for row in cur:
        l.append(row[0])
    for tablename in l:
        cur.execute("drop table if exists {0} CASCADE".format(tablename))

    cur.close()

def deleteTables(ratingstablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if ratingstablename.ub() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (ratingstablename))
        openconnection.commit()
    except psycopg2.DatabaseError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
    except IOError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
    finally:
        if cursor:
            cursor.close()