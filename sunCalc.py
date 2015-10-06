'''
Created on May 1, 2014

@author: tamir.lance
'''

import time
import math
import ephem
import datetime
import numpy as np
import pandas as pd

class SunCalc(object):

    def __init__(self, location = {'Name':'Mountain View','latitude':'37.395946','longitude':'-122.058075','TimeZone':-8, 'DST':True}):

        self.RAD = math.pi/180.
        self.system = ephem.Observer()
        self.system.lat = location['latitude']
        self.system.long = location['longitude']   
        self.TimeZone = location['TimeZone']
        self.name = location['Name']
        
        if location['DST']:
            self.DST = time.daylight
        else:
            self.DST = 0

    def point_calc(self,dt1 = ephem.Date('2014-1-1 00:00:00')):       
        datelocal = ephem.Date(dt1)
        self.system.date = ephem.Date(datelocal-self.TimeZone*ephem.hour-self.DST*ephem.hour)  #Convert to UTC time
        #Calculate where the sun is
        s = ephem.Sun(self.system)
        azimuth = s.az/self.RAD
        elevation = s.alt/self.RAD
        
        roll = np.round(np.arctan(np.sin(azimuth*self.RAD)/np.tan(elevation*self.RAD))/self.RAD,2) #use the azimuth and elevation to calculate the theoretical roll angle for a N-S horizontal one axis tracked system
        n_track = np.array([np.sin(roll*self.RAD),np.cos(roll*self.RAD),0.]) #calculate the vector for the normal to the module
        n_sun = np.array([np.cos(elevation*self.RAD)*np.sin(azimuth*self.RAD),np.sin(elevation*self.RAD),np.cos(elevation*self.RAD)*np.cos(azimuth*self.RAD)]) #calculate the vector for the sun
        AOI = np.arccos(np.dot(n_track,n_sun))/self.RAD #Calculate the angle of incidence (AOI) on the module 
        if azimuth<90. or azimuth>270.:
            AOI = -AOI
        
        return self.system.date, azimuth, elevation, roll, AOI
    
    def vector_calc(self, times = [], dt1 = ephem.Date('2014-1-1 00:00:00')):
        sun_pos = {'time':np.zeros(len(times)), 'azimuth':np.zeros(len(times)), 'elevation':np.zeros(len(times)), 'roll':np.zeros(len(times)), 'AOI':np.zeros(len(times))}
        for i in range(len(times)):
            datelocal = ephem.Date(dt1 + (times[i,0]*24 + times[i,1])*ephem.hour + times[i,2]*ephem.minute + times[i,3]*ephem.second)
            sun_pos['time'][i], sun_pos['azimuth'][i], sun_pos['elevation'][i], sun_pos['roll'][i], sun_pos['AOI'][i] = self.point_calc(dt1 = datelocal)
        
        return sun_pos
    
    def DNI_weighted(self, DNI, sun_pos, interval):
        pass
                
if __name__=='__main__':
    
    location = {'Name':'Phoenix','latitude':'33.96 deg','longitude':'-112.02','TimeZone':-7,'DST':False}
    fileread = '\\\\SWEFS01\\SWEdata\\System Modeling\\Phoenix 15min DNI.csv'
    DNI_read = open(fileread,'rb')
    lines = DNI_read.readlines()
    DNI = {'times':[],'values':np.zeros(len(lines))}
    for i in range(len(lines)):       
        x = lines[i].split(',')
        DNI['values'][i] = float(x[1])
        x = datetime.datetime.strptime(x[0], '%Y-%m-%d %H:%M:%S')
        DNI['times'].append(x)
    DNI = pd.DataFrame(DNI['values'],index=DNI['times'],columns=list('A'))
    DNI = DNI.resample('1min')
    DNI = DNI.interpolate()
    
    #===========================================================================
    # SC = SunCalc(dt1 = ephem.Date('2014-5-1 13:05:18'))
    # print SC.AOI, SC.roll, SC.azimuth, SC.elevation
    #===========================================================================
