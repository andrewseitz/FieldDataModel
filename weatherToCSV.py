from __future__ import division

#!/usr/bin/env python
""" This file pulls from a sql database of solar data
and converts it into a csv weather file the NREL SAM can use"""
__author__ = "Andrew Seitz"
__status__ = "v1.0"


from sunCalc import SunCalc as SC
import pymssql
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import ephem
import csv

class WeatherToCSV(object):

    def __init__(self, sunCalc_dict, save_location):
        self.sunCalc_dict = sunCalc_dict
        self.sunCalc_location = SC(location = sunCalc_dict)
        
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

    def pullMinuteData(self,date_start,date_end,database,DB_location):
        #function that pulls weather data from a database on the loaded server
        #you specify a location (a column in the database) and a date range to pull from
        #data will then be converted to hourly data and set a class attribute in lists

        #write the attributes
        self.date_start = date_start
        self.date_end = date_end
        self.database = database
        self.DB_location = DB_location
        
        date_start_UTC = self.convertToUTC(date_start,self.sunCalc_location)
        date_end_UTC = self.convertToUTC(date_end,self.sunCalc_location)
        sqlQuery = """select TimeStamp, GlobalSolar_Avg, DiffuseSolar_Avg, AirTemp_C_Avg, RH_Avg, WindSpd_ms_WVc1, WindSpd_ms_WVc2 from %s
                    where Location ='%s' and TimeStamp >= '%s' and TimeStamp <= '%s' order by timestamp asc""" % (database, DB_location, date_start_UTC,date_end_UTC)
        self.cursor.execute(sqlQuery)

        #temp storage
        self.timestamp = []
        self.GHI = []
        self.Diff = []        
        self.Tamb = []
        self.RH = []
        self.Wspd = []
        self.Wdir = []
        #store from curson
        for row in self.cursor:
            self.timestamp.append(row[0] + timedelta(hours = self.sunCalc_location.TimeZone)) #convert back to local time
            self.GHI.append(row[1])
            self.Diff.append(row[2])
            self.Tamb.append(row[3])
            self.RH.append(row[4])
            self.Wspd.append(row[5])
            self.Wdir.append(row[6])

    def constructBetweenDates(self):
        #essentially a test function
        #makes a weather file between the dates you added to view data 
        #will not work with SAM unless you give it a years worth of data
        #please use constructYear for data sets with less than a years worth of data

        #algorthim for converting 5 minute date to hourly averages
        #need numpy arrays in column format (need to transpose the list)
        #making sure not to modify self.timestamp
        timestamp = np.array(self.timestamp)
        timestamp = np.transpose(timestamp)
        #create a 2d array 6 columns by x rows
        min_matrix = np.array([self.GHI,self.Diff,self.Tamb,self.RH,self.Wspd,self.Wdir])
        min_matrix = np.transpose(min_matrix)
        #create data fram and set my index to my time numpy array
        min_matrix = pd.DataFrame(min_matrix,index = timestamp, columns= ['GHI','Diff','Tamb','RH','Wspd','Wdir'])
        #average the 5 minute data by taking the mean over an hour (very flexible)
        hour_matrix = min_matrix.resample('H', how='mean')

        #pull data from pandas DataFrame Object
        datetime_list = hour_matrix.index.to_pydatetime()
        GHI_list = [float(item) for item in hour_matrix['GHI'].get_values()]
        Diff_list = [float(item) for item in hour_matrix['Diff'].get_values()]
        Tamb_list = [float(item) for item in hour_matrix['Tamb'].get_values()]
        RH_list = [float(item) for item in hour_matrix['RH'].get_values()]
        Wspd_list = [float(item) for item in hour_matrix['Wspd'].get_values()]
        Wdir_list = [float(item) for item in hour_matrix['Wdir'].get_values()]

        #storage for writing to csv file
        self.weather = []
        pressure = 1013.25 #in mBar (sea-level)
        ###inputs for csv will be [Year, Month,Day,Hour,GHI,DNI,DHI,Tdry,Twet,RH,PRES,Wspd,Wdir,Albedo]
        for i in range(len(datetime_list)):
            info = self.sunCalc_location.point_calc(dt1 = ephem.Date(datetime_list[i]))
            #from Wet-Bulb Temperature from Relative Humidity and Air Temperature by Roland Stull
            #valid for 101.325 kPa (doesnt account for elevation)
            wet_temp = Tamb_list[i]*np.arctan(0.151977*(RH_list[i]+8.313659)**(1/2)) + np.arctan(RH_list[i]+Tamb_list[i]) \
                       - np.arctan(RH_list[i]-1.6767331) + .00391838*(RH_list[i]**(3/2))*np.arctan(.023101*RH_list[i]) - 4.686035
            if info[2]<0 or info[3]<-70 or info[3]>70:
                self.weather.append([datetime_list[i].year, datetime_list[i].month,datetime_list[i].day, \
                                datetime_list[i].hour,0,0,0,Tamb_list[i],wet_temp,RH_list[i], pressure, \
                                Wspd_list[i], Wdir_list[i],self.sunCalc_dict['Albedo']])
            else:
                Ze = 90-info[2]
                DNI = (GHI_list[i]-Diff_list[i])/np.cos(np.radians(Ze))
                self.weather.append([datetime_list[i].year, datetime_list[i].month, datetime_list[i].day, \
                                datetime_list[i].hour, GHI_list[i], DNI, Diff_list[i], Tamb_list[i], wet_temp, \
                                RH_list[i], pressure, Wspd_list[i], Wdir_list[i], self.sunCalc_dict['Albedo']])
                
        #open csv writer, write the headers
        save_date_start = datetime.strptime(self.date_start, '%m/%d/%y %I:%M')
        save_date_start = datetime.strftime(save_date_start,'%m%d%y%H')
        save_date_end = datetime.strptime(self.date_end, '%m/%d/%y %I:%M')
        save_date_end = datetime.strftime(save_date_end,'%m%d%y%H')

        file_name = r"%s(%s)_(%s - %s).csv" % (self.sunCalc_dict['Name'],self.DB_location,save_date_start,save_date_end)
        print file_name
        header1 = ['Source','Location ID', 'City','State','Country','Latitude','Longitude','Time Zone', 'Elevation']
        header1data = [self.sunCalc_dict['Source'],self.sunCalc_dict['LocationID'],self.sunCalc_dict['Name'], \
                       self.sunCalc_dict['State'], self.sunCalc_dict['Country'],self.sunCalc_dict['latitude'], \
                       self.sunCalc_dict['longitude'], self.sunCalc_dict['TimeZone'], self.sunCalc_dict['Elevation']]
        header2 = ['Year', 'Month','Day','Hour','GHI','DNI','DHI','Tdry','Twet','RH','Pres','Wspd','Wdir','Albedo']

        with open(file_name,'w') as f:
            csv_write = csv.writer(f,lineterminator = '\n',delimiter = ',')
            csv_write.writerow(header1)
            csv_write.writerow(header1data)
            csv_write.writerow(header2)
            for row in self.weather:
                csv_write.writerow(row)


    def constructYear(self):
        #smartly constructs a yearly weather file for NREL SAM simulations

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
            min_matrix = np.array([self.GHI,self.Diff,self.Tamb,self.RH,self.Wspd,self.Wdir])
            min_matrix = np.transpose(min_matrix)
            #create data fram and set my index to my time numpy array
            min_matrix = pd.DataFrame(min_matrix,index = timestamp, columns= ['GHI','Diff','Tamb','RH','Wspd','Wdir'])
            #average the 5 minute data by taking the mean over an hour (very flexible)
            hour_matrix = min_matrix.resample('H', how='mean')

            #fill in empty data for the year from which you are collecting
            #doesn't matter which you choose and lower and upper are equal in this case
            year_value = upperBound.year
            stringLower = '%s-01-01 00:00:00' % (year_value)
            stringUpper = '%s-12-31 23:00:00' % (year_value)
            idx = pd.date_range(stringLower,stringUpper, freq = 'H')
            idx = pd.DatetimeIndex(idx)
            year_matrix = hour_matrix.reindex(index=idx, fill_value=0)
            
            #pull data from pandas DataFrame Object
            datetime_list = year_matrix.index.to_pydatetime()
            GHI_list = [float(item) for item in year_matrix['GHI'].get_values()]
            Diff_list = [float(item) for item in year_matrix['Diff'].get_values()]
            Tamb_list = [float(item) for item in year_matrix['Tamb'].get_values()]
            RH_list = [float(item) for item in year_matrix['RH'].get_values()]
            Wspd_list = [float(item) for item in year_matrix['Wspd'].get_values()]
            Wdir_list = [float(item) for item in year_matrix['Wdir'].get_values()]                                                                                                  

            #storage for writing to csv file
            self.weather = []
            pressure = 1013.25 #in mBar (sea-level)
            ###inputs for csv will be [Year, Month,Day,Hour,GHI,DNI,DHI,Tdry,Twet,RH,PRES,Wspd,Wdir,Albedo]
            for i in range(len(datetime_list)):
                info = self.sunCalc_location.point_calc(dt1 = ephem.Date(datetime_list[i]))
                #from Wet-Bulb Temperature from Relative Humidity and Air Temperature by Roland Stull
                #valid for 101.325 kPa (doesnt account for elevation)
                wet_temp = Tamb_list[i]*np.arctan(0.151977*(RH_list[i]+8.313659)**(1/2)) + np.arctan(RH_list[i]+Tamb_list[i]) \
                           - np.arctan(RH_list[i]-1.6767331) + .00391838*(RH_list[i]**(3/2))*np.arctan(.023101*RH_list[i]) - 4.686035
                if info[2]<0 or info[3]<-70 or info[3]>70:
                    self.weather.append([datetime_list[i].year, datetime_list[i].month,datetime_list[i].day, \
                                    datetime_list[i].hour,0,0,0,Tamb_list[i],wet_temp,RH_list[i], pressure, \
                                    Wspd_list[i], Wdir_list[i],self.sunCalc_dict['Albedo']])
                else:
                    Ze = 90-info[2]
                    DNI = (GHI_list[i]-Diff_list[i])/np.cos(np.radians(Ze))
                    self.weather.append([datetime_list[i].year, datetime_list[i].month, datetime_list[i].day, \
                                    datetime_list[i].hour, GHI_list[i], DNI, Diff_list[i], Tamb_list[i], wet_temp, \
                                    RH_list[i], pressure, Wspd_list[i], Wdir_list[i], self.sunCalc_dict['Albedo']])
                    


            file_name = r"%s(%s)_%s.csv" % (self.sunCalc_dict['Name'],self.DB_location,year_value)
            print file_name
            header1 = ['Source','Location ID', 'City','State','Country','Latitude','Longitude','Time Zone', 'Elevation']
            header1data = [self.sunCalc_dict['Source'],self.sunCalc_dict['LocationID'],self.sunCalc_dict['Name'], \
                           self.sunCalc_dict['State'], self.sunCalc_dict['Country'],self.sunCalc_dict['latitude'], \
                           self.sunCalc_dict['longitude'], self.sunCalc_dict['TimeZone'], self.sunCalc_dict['Elevation']]
            header2 = ['Year', 'Month','Day','Hour','GHI','DNI','DHI','Tdry','Twet','RH','Pres','Wspd','Wdir','Albedo']

            with open(file_name,'w') as f:
                csv_write = csv.writer(f,lineterminator = '\n',delimiter = ',')
                csv_write.writerow(header1)
                csv_write.writerow(header1data)
                csv_write.writerow(header2)
                for row in self.weather:
                    csv_write.writerow(row)
        else:
            print "please enter a date range with the same calendar year"
            print "This is to insure compliance with NREL SAM weather files"
 



def example():
    #example code of how you would generate a yearly weather file from a few days of field data
    #inputs
    date_start = '01/01/15 07:00'         #enter your time in local military time as as string (example: 12 /01/14 07:00)
    date_end = '06/15/15 08:00'           #enter your time in local military time as as string (example: 12/01/14 15:00)
    server = 'monitoring.cogenra.com'     #enter server name
    user = 'IBOSWWW'                      #enter user name
    password = 'Bossy777'                 #enter password
    database = 'solardb.dbo.Weather'      #enter database name within server
    DB_location = 'SolarZone'             #enter location ID for relevant area (DB parameter)

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

    #main part of program
    converter = WeatherToCSV(sunCalc_dict, "save location")
    converter.serverConnect(user,password,server)
    converter.pullMinuteData(date_start,date_end,database,DB_location)
    converter.constructYear()
    connverter.serverDisconnect()









                        

   
