#!/usr/bin/python2.7
#
# Assignment3 Interface
#

import psycopg2
import os
import sys
import threading

##################### This needs to changed based on what kind of table we want to sort. ##################
##################### To know how to change this, see Assignment 3 Instructions carefully #################
FIRST_TABLE_NAME = 'table1'
SECOND_TABLE_NAME = 'table2'
SORT_COLUMN_NAME_FIRST_TABLE = 'column1'
SORT_COLUMN_NAME_SECOND_TABLE = 'column2'
JOIN_COLUMN_NAME_FIRST_TABLE = 'column1'
JOIN_COLUMN_NAME_SECOND_TABLE = 'column2'
##########################################################################################################


def rangepartition(ratingstablename, columnname, numberofpartitions, openconnection):
    if isinstance(numberofpartitions, int) and numberofpartitions > 0:
        cur = openconnection.cursor();
        cur.execute("SELECT min("+columnname+") FROM "+ratingstablename+";")
        minval = float(cur.fetchone()[0])
        # minval = 0.0;
        print "min: " +str(minval)
        cur.execute("SELECT max("+columnname+") FROM "+ratingstablename+";")
        maxval = float(cur.fetchone()[0])
        # maxval = 5.0;
        print "max: " +str(maxval)
        interval = (maxval-minval)/(numberofpartitions)
        print "interval:" +str(interval)
        for i in range(0,numberofpartitions):
            cur.execute("CREATE TABLE "+ratingstablename+"range"+str(i)+" (like "+ratingstablename+");")
            lowerlimit = minval+i*interval
            print(lowerlimit)
            upperlimit = minval+(i+1)*interval
            print(upperlimit)
            if i==0:
                cur.execute("INSERT INTO "+ratingstablename+"range"+str(i)+" "
                                +"SELECT * FROM "+ratingstablename+" "
                                +"WHERE "+columnname+" >= "+str(lowerlimit)+" AND "+columnname+" <= "+str(upperlimit))
                print(i)
            else:
                cur.execute("INSERT INTO "+ratingstablename+"range"+str(i)+" "
                                +"SELECT * FROM "+ratingstablename+" "
                                +"WHERE "+columnname+" > "+str(lowerlimit)+" AND "+columnname+" <= "+str(upperlimit))
                print(i)
        # Create metadata
        cur.execute("select exists(select * from information_schema.tables where table_name=%s)", ("range_metadata",))
        if(not cur.fetchone()[0]):
            cur.execute("CREATE TABLE range_metadata (tablename text, columnname text, partitions integer, minvalue integer, maxvalue integer);")
        else:
            cur.execute("TRUNCATE TABLE range_metadata;");
        cur.execute("INSERT INTO range_metadata (tablename, columnname, partitions, minvalue, maxvalue) VALUES (%s, %s, %s, %s, %s)",
                    (ratingstablename, columnname, numberofpartitions, minval, maxval))
        openconnection.commit()

def rangepartition2(ratingstablename, columnname, numberofpartitions, openconnection):
    if isinstance(numberofpartitions, int) and numberofpartitions > 0:
        cur = openconnection.cursor();
        cur.execute("select minvalue from range_metadata;");
        minval = float(cur.fetchone()[0])
        cur.execute("select maxvalue from range_metadata;");
        maxval = float(cur.fetchone()[0])
        print "max: " +str(maxval)
        interval = (maxval-minval)/(numberofpartitions)
        print "interval:" +str(interval)
        for i in range(0,numberofpartitions):
            cur.execute("CREATE TABLE "+ratingstablename+"range"+str(i)+" (like "+ratingstablename+");")
            lowerlimit = minval+i*interval
            print(lowerlimit)
            upperlimit = minval+(i+1)*interval
            print(upperlimit)
            if i==0:
                cur.execute("INSERT INTO "+ratingstablename+"range"+str(i)+" "
                                +"SELECT * FROM "+ratingstablename+" "
                                +"WHERE "+columnname+" >= "+str(lowerlimit)+" AND "+columnname+" <= "+str(upperlimit))
                print(i)
            else:
                cur.execute("INSERT INTO "+ratingstablename+"range"+str(i)+" "
                                +"SELECT * FROM "+ratingstablename+" "
                                +"WHERE "+columnname+" > "+str(lowerlimit)+" AND "+columnname+" <= "+str(upperlimit))
                print(i)
        openconnection.commit()


def sorttable (tablename, columnname, results, partnum, openconnection):
    cur = openconnection.cursor();
    cur.execute("select * from "+tablename+" order by "+columnname)
    results[partnum] = cur.fetchall()

# Donot close the connection inside this file i.e. do not perform openconnection.close()
def ParallelSort (InputTable, SortingColumnName, OutputTable, openconnection):
    num = 5
    rangepartition(InputTable, SortingColumnName, num, openconnection)
    results = {}
    jobs = []
    for i in range(0,num):
        thread = threading.Thread(target=sorttable(InputTable+"range"+str(i), SortingColumnName, results, i, openconnection))
        jobs.append(thread)
    # Start the threads
    for job in jobs:
        job.start()
    # Join the threads
    for job in jobs:
        job.join()
    # Put results in one table
    cur = openconnection.cursor();
    cur.execute("CREATE TABLE "+OutputTable+" (like "+InputTable+");")
    for i in range(0,num):
        rows = results[i]
        for row in rows:
            cur.execute("INSERT INTO "+OutputTable+" values "+str(row))
    openconnection.commit()

def jointables (tablename1, columnname1, tablename2, columnname2, results, partnum, openconnection):
    cur = openconnection.cursor();
    cur.execute("select * from "+tablename1+" inner join "+tablename2+" on "+tablename1+"."+columnname1+" = "+tablename2+"."+columnname2)
    results[partnum] = cur.fetchall()

def ParallelJoin (InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection):
    num = 5
    rangepartition(InputTable1, Table1JoinColumn, num, openconnection)
    rangepartition2(InputTable2, Table2JoinColumn, num, openconnection)
    results = {}
    jobs = []
    for i in range(0,num):
        thread = threading.Thread(target=jointables(InputTable1+"range"+str(i), Table1JoinColumn, InputTable2, Table2JoinColumn, results, i, openconnection))
        jobs.append(thread)
    # Start the threads
    for job in jobs:
        job.start()
    # Join the threads
    for job in jobs:
        job.join()
    # Put results in one table
    cur = openconnection.cursor();
    cur.execute("create table "+OutputTable+" as select * from "+InputTable1+","+InputTable2+" where false;")
    for i in range(0,num):
        rows = results[i]
        for row in rows:
            cur.execute("INSERT INTO "+OutputTable+" values "+str(row))
    openconnection.commit()


################### DO NOT CHANGE ANYTHING BELOW THIS #############################


# Donot change this function
def getOpenConnection(user='postgres', password='1234', dbname='ddsassignment3'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")

# Donot change this function
def createDB(dbname='ddsassignment3'):
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
        print 'A database named {0} already exists'.format(dbname)

    # Clean up
    cur.close()
    con.commit()
    con.close()

# Donot change this function
def deleteTables(ratingstablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if ratingstablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (ratingstablename))
        openconnection.commit()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    except IOError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()

# Donot change this function
def saveTable(ratingstablename, fileName, openconnection):
    try:
        cursor = openconnection.cursor()
        cursor.execute("Select * from %s" %(ratingstablename))
        data = cursor.fetchall()
        openFile = open(fileName, "w")
        for row in data:
            for d in row:
                openFile.write(`d`+",")
            openFile.write('\n')
        openFile.close()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    except IOError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()

if __name__ == '__main__':
    try:
        # Creating Database ddsassignment2
        print "Creating Database named as ddsassignment2"
        createDB();

        # Getting connection to the database
        print "Getting connection from the ddsassignment2 database"
        con = getOpenConnection();

        # Calling ParallelSort
        print "Performing Parallel Sort"
        ParallelSort(FIRST_TABLE_NAME, SORT_COLUMN_NAME_FIRST_TABLE, 'parallelSortOutputTable', con);

        # Calling ParallelJoin
        print "Performing Parallel Join"
        ParallelJoin(FIRST_TABLE_NAME, SECOND_TABLE_NAME, JOIN_COLUMN_NAME_FIRST_TABLE, JOIN_COLUMN_NAME_SECOND_TABLE, 'parallelJoinOutputTable', con);

        # Saving parallelSortOutputTable and parallelJoinOutputTable on two files
        saveTable('parallelSortOutputTable', 'parallelSortOutputTable.txt', con);
        saveTable('parallelJoinOutputTable', 'parallelJoinOutputTable.txt', con);

        # Deleting parallelSortOutputTable and parallelJoinOutputTable
        deleteTables('parallelSortOutputTable', con);
        deleteTables('parallelJoinOutputTable', con);

        if con:
            con.close()

    except Exception as detail:
        print "Something bad has happened!!! This is the error ==> ", detail
