from __future__ import division

#!/usr/bin/env python
""" This file pulls from a sql database of 5 minute tracker angles
and write them to a csv file of hourly data"""
__author__ = "Andrew Seitz"
__status__ = "v1.0"

import pymssql
from datetime import datetime, timedelta
from sunCalc import SunCalc as SC
import pandas as pd
import numpy as np
import csv


class AngleToCSV(object):

    def __init__(self, sunCalc_dict, save_location):
        self.sunCalc_dict = sunCalc_dict
        self.sunCalc_location = SC(location = sunCalc_dict)
        self.save_location = save_location
    def serverConnect(self, user, password, server):
        #general function to connect to a server    
        self.connection = pymssql.connect(server, user, password)
        self.cursor = self.connection.cursor()

    def serverDisconnect(self):
        self.connection.close()
        
    def convertToUTC(self,timeString,sunCalc_object):
        #function that use the timezone in the sunCalc object to convert to UTC time
        converted_date = datetime.strptime(timeString, '%m/%d/%y %I:%M')
        converted_date = converted_date + timedelta(hours = -sunCalc_object.TimeZone)
        return datetime.strftime(converted_date,'%m/%d/%y %H:%M')

    def dateToIndex(self,date):
        #algorithm to score a date on a 0 to 8759 hour scale
        index = 0
        if isinstance(date,datetime) == True:
            #date.hour is equal to a score of 0 to 23
            year = datetime(date.year,1,1,0,0)
            difference = date - year
            index = index + difference.days*24
            index = index + difference.seconds/3600
            return int(index)
        else:
            print "please pass a datetime object"

    def pullMinuteData(self,date_start,date_end,database,DB_location,row):
        #function that pulls angle data from a database on the loaded server
        #you specify a location (a column in the database), a row (location in field)
        #and a date range to pull from
        #data will then be converted to hourly data and set a class attribute in lists
        
        #write the attributes
        self.date_start = date_start
        self.date_end = date_end
        self.database = database
        self.DB_location = DB_location
        self.row = row
        
        date_start_UTC = self.convertToUTC(date_start,self.sunCalc_location)
        date_end_UTC = self.convertToUTC(date_end,self.sunCalc_location)
        sqlQuery = """select TimeStamp, Angle from %s where Location ='%s' and TimeStamp >= '%s' and TimeStamp <= '%s' \
                    and Name = '%s' order by timestamp asc""" % (database, DB_location, date_start_UTC,date_end_UTC,self.row)
        self.cursor.execute(sqlQuery)
        #temp storage
        self.timestamp = []
        self.angles = []
        #store from cursor
        for row in self.cursor:
            self.timestamp.append(row[0] + timedelta(hours = self.sunCalc_location.TimeZone)) #convert back to local time
            self.angles.append(row[1])
            
    def constructBetweenDates(self):
        #construct an angle file between the two dates given by hour (so if 1 day given you get 24 data points)
        #missing data is supplemented by angles provided by SAM NREL backtracking algorithm
        
        #algorthim for converting 5 minute date to hourly averages
        #need numpy arrays in column format (need to transpose the list)
        #making sure not to modify self.timestamp
        timestamp = np.array(self.timestamp)
        timestamp = np.transpose(timestamp)
        #create a 2d array 6 columns by x rows
        min_matrix = np.array(self.angles)
        min_matrix = np.transpose(min_matrix)
        #create data fram and set my index to my time numpy array
        min_matrix = pd.DataFrame(min_matrix,index = timestamp, columns= ['angles'])
        #average the 5 minute data by taking the mean over an hour (very flexible)
        hour_matrix = min_matrix.resample('H', how='mean')

        #pull data from pandas DataFrame Object
        datetime_list = hour_matrix.index.to_pydatetime()
        angles_list = [item for item in hour_matrix['angles'].get_values()]

        #365 days of data
        SAM_angles = []
        with open('1axisRotValues.csv', 'r') as f:
            csv_read = csv.reader(f,delimiter = ',')
            for row in csv_read:
                SAM_angles.append(row[1])

        #segment of code accounts for the possibility of missing data
        #added precalculated SAM data in place of NaN type given by pandas
        filled_angles = []
        i = 0
        for angle in angles_list:
            if pd.isnull(angle):
                #angles_list has variable number of days so we need to determine from where in SAM_angle to pull from
                filled_angles.append(SAM_angles[self.dateToIndex(datetime_list[i])])
                i = i + 1
            else:
                filled_angles.append(float(angles_list[i]))
                i = i + 1
 
        self.data = []
        for i in range(len(datetime_list)):
            self.data.append([datetime_list[i],filled_angles[i]])
        
        save_date_start = datetime.strptime(self.date_start, '%m/%d/%y %I:%M')
        save_date_start = datetime.strftime(save_date_start,'%m%d%y%H')
        save_date_end = datetime.strptime(self.date_end, '%m/%d/%y %I:%M')
        save_date_end = datetime.strftime(save_date_end,'%m%d%y%H')

        file_name = r"Angles from %s at %s(%s)_(%s - %s).csv" % (self.row, self.sunCalc_dict['Name'],self.DB_location,save_date_start,save_date_end)
        print file_name

        with open(file_name,'w') as f:
            csv_write = csv.writer(f,lineterminator = '\n',delimiter = ',')
            for row in self.data:
                csv_write.writerow(row)

    def constructYear(self):
        #smartly constructs a year length file of angles for NREL SAM simulations
        #this file contains values from the database from the data range given
        #rest of values supplemented from previous SAM calculation
        
        #check to see if the data given exceeds a 1 year range (i.e. from 2013-2014)
        #this is maintain compliance with SAM
        upperBound = datetime.strptime(self.date_start, '%m/%d/%y %I:%M')
        lowerBound = datetime.strptime(self.date_end, '%m/%d/%y %I:%M')

        if upperBound.year == lowerBound.year:
            #algorthim for converting 5 minute date to hourly averages
            #need numpy arrays in column format (need to transpose the list)
            #making sure not to modify self.timestamp
            timestamp = np.array(self.timestamp)
            timestamp = np.transpose(timestamp)
            #create a 2d array 6 columns by x rows
            min_matrix = np.array(self.angles)
            min_matrix = np.transpose(min_matrix)
            #create data fram and set my index to my time numpy array
            min_matrix = pd.DataFrame(min_matrix,index = timestamp, columns= ['angles'])
            #average the 5 minute data by taking the mean over an hour (very flexible)
            hour_matrix = min_matrix.resample('H', how='mean')

            #fill in empty data for the year from which you are collecting
            #doesn't matter which you choose and lower and upper are equal in this case
            year_value = upperBound.year
            stringLower = '%s-01-01 00:00:00' % (year_value)
            stringUpper = '%s-12-31 23:00:00' % (year_value)
            idx = pd.date_range(stringLower,stringUpper, freq = 'H')
            idx = pd.DatetimeIndex(idx)
            year_matrix = hour_matrix.reindex(index=idx)
            
            #pull data from pandas DataFrame Object
            datetime_list = year_matrix.index.to_pydatetime()
            angles_list = [item for item in year_matrix['angles'].get_values()]

            #365 days of data
            SAM_angles = []
            with open('1axisRotValues.csv', 'r') as f:
                csv_read = csv.reader(f,delimiter = ',')
                for row in csv_read:
                    SAM_angles.append(row[1])

            #segment of code accounts for the possibility of missing data
            #added precalculated SAM data in place of NaN type given by pandas
            filled_angles = []
            i = 0
            for angle in angles_list:
                if pd.isnull(angle):
                    filled_angles.append(SAM_angles[i])
                    i = i + 1
                else:
                    filled_angles.append(float(angles_list[i]))
                    i = i + 1
     
            self.data = []
            for i in range(len(datetime_list)):
                self.data.append([datetime_list[i],filled_angles[i]])
            
            save_date_start = datetime.strptime(self.date_start, '%m/%d/%y %I:%M')
            save_date_start = datetime.strftime(save_date_start,'%m%d%y%H')
            save_date_end = datetime.strptime(self.date_end, '%m/%d/%y %I:%M')
            save_date_end = datetime.strftime(save_date_end,'%m%d%y%H')

            file_name = r"Angles from %s at %s(%s)_(%s).csv" % (self.row, self.sunCalc_dict['Name'],self.DB_location,year_value)
            print file_name

            with open(file_name,'w') as f:
                csv_write = csv.writer(f,lineterminator = '\n',delimiter = ',')
                for row in self.data:
                    csv_write.writerow(row)

        else:
            print "please enter a date range with the same calendar year"
            print "This is to insure compliance with NREL SAM weather files"
        
def example():
    #example code of how you would generate a yearly angle file from a few days of field data
    #inputs
    date_start = '03/10/15 07:00'         #enter your time in local military time as as string (example: 12 /01/14 07:00)
    date_end = '06/23/15 07:00'           #enter your time in local military time as as string (example: 12/01/14 15:00)
    server = 'monitoring.cogenra.com'     #enter server name
    user = 'IBOSWWW'                      #enter user name
    password = 'Bossy777'                 #enter password
    database = 'solardb.dbo.SunBase4t'    #enter database name within server
    DB_location = 'SolarZone'             #enter location ID for relevant area (DB parameter)
    row = 'TEP 53'                        #enter the Name (as said in database) or row 
    
    #inputs for sunCalc_dict
    name_SC = 'Tucson'
    latitude_SC = '32.1025'
    longitude_SC = '-110.8142'
    TimeZone_SC = -7
    DST_SC = False
    elevation_SC = 8
    albedo_SC = .2
    source_SC = 'field data'
    state_SC = 'Arizona'
    country_SC = 'USA'
    locationID_SC = '123456'
    sunCalc_dict = {'Name':name_SC,'latitude':latitude_SC,'longitude':longitude_SC,'TimeZone':-7, 'DST':DST_SC, \
                    'Elevation':elevation_SC, 'Albedo':albedo_SC, 'Source':source_SC,'State':state_SC, \
                    'Country':country_SC, 'LocationID':locationID_SC}

    angler = AngleToCSV(sunCalc_dict, "save_location")
    angler.serverConnect(user,password,server)
    angler.pullMinuteData(date_start,date_end,database,DB_location,row)
    angler.constructYear()
    angler.serverDisconnect()










        
