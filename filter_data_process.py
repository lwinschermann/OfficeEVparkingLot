#    Data analysis in OfficeEVparkingLot
#    Filter, process and analyze EV data collected at Dutch office building parking lot
#    Statistical analysis of EV data at ASR facilities - GridShield project - developed by 
#    Leoni Winschermann, University of Twente, l.winschermann@utwente.nl
#    Nataly Bañol Arias, University of Twente, m.n.banolarias@utwente.nl
#    
#    Copyright (C) 2022 CAES and MOR Groups, University of Twente, Enschede, The Netherlands
#
#    This library is free software; you can redistribute it and/or
#    modify it under the terms of the GNU Lesser General Public
#    License as published by the Free Software Foundation; either
#    version 2.1 of the License, or (at your option) any later version.
#
#    This library is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
#    Lesser General Public License for more details.
#
#    You should have received a copy of the GNU Lesser General Public
#    License along with this library; if not, write to the Free Software
#    Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301
#    USA

import pandas as pd
import datetime as dt
import numpy as np

""""
========================== Filtering Data =========================
"""

class FilteringData:
    def __init__(self):
        #import data from Excel sheets
        #we receive our data via two channels that will be combined here. 
        #self.dataFiles = ['transactions_asr_all_2022-09-12T15_55_00.csv', '20220912transactions_asr.csv'] #--> Used in Value of Information study
        self.dataFiles = ['asrData1.csv', 'asrData2.csv'] #--> Used in PHC study

    #energyCutoOff in kWh
    def filter_data(self, startTimeFilter = True, afterStartDate = dt.datetime(2021, 1, 1, 0, 0), beforeEndDate = dt.datetime(2021, 12, 31, 23, 59), energyFilter = True, energyCutOff = 0, defaultCapacity = None, defaultPower = None, maxDwellTime = None, minDwellTime = None, overnightStays = True, managersFilter = None,  listmanagersFilter = None, idFilter = None):
        dataFiles = self.dataFiles
        self.defaultPower = defaultPower
        self.defaultCapacity = defaultCapacity
        
        #Data has columns ['transaction', 'chargepoint_id', 'start_datetime_utc', 'end_datetime_utc', 'card_id', 'total_energy']
        self.rawData = pd.read_csv(dataFiles[0])
        #data has columns ['session_id','session_start_datetime','session_end_datetime','session_kwh','session_auth_id','session_auth_method','connector_id','connector_standard','connector_format','evse_uid','evse_id','location_id','location_name','location_address','location_city','location_postal_code','location_latitude','location_longitude]
        self.rawData2 = pd.read_csv(dataFiles[1])
        
        rawData = self.rawData
        rawData2 = self.rawData2
        
        #selected filters to delete incomplete data points. 
        rawData = rawData[rawData.start_datetime_utc.notnull()] #filters out rows where column start_datetime_utc has no value
        rawData = rawData[rawData.end_datetime_utc.notnull()] #filters out rows where column end_datetime_utc has no value
        rawData = rawData[rawData.total_energy.notnull()] #filters out rows where column total_energy has no value
        rawData = rawData[rawData.card_id.notnull()] #filters out rows where card_id has no value

        rawData2 = rawData2[rawData2.session_start_datetime.notnull()] #filters out rows where column start_datetime_utc has no value
        rawData2 = rawData2[rawData2.session_end_datetime.notnull()] #filters out rows where column end_datetime_utc has no value
        rawData2 = rawData2[rawData2.session_kwh.notnull()] #filters out rows where column total_energy has no value
        rawData2 = rawData2[rawData2.session_auth_id.notnull()] #filters out rows where column session_auth_id has no value
        
        #dates have type string and format YYYY-MM-DDTHH:MM:SSZ.
        #Convert to class pandas._libs.tslibs.timestamps.Timestamp and do utc correction        
        #Dutch European Daylight Saving Time as is since 1977 (+1 CET in winter, +2 CEST in summer). 
        #CEST 29 March 2020 - 25 October 2020; 28 March 2021 - 31 October 2021; 27 March 2022 - 30 October 2022; 26 March 2023 - 29 October 2023; 31 March 2024 - 27 October 2024
        #Where condition == False, CET (where-function applies). Else CEST
        rawData['start_datetime_utc'] = pd.to_datetime(rawData['start_datetime_utc'],format = '%Y-%m-%dT%H:%M:%SZ') + dt.timedelta(hours = 2)
        rawData['start_datetime_utc'] = rawData['start_datetime_utc'].where((rawData['start_datetime_utc'] >= dt.datetime(2020, 3, 29, 1, 0)) , rawData['start_datetime_utc'] - dt.timedelta(hours = 1))
        rawData['start_datetime_utc'] = rawData['start_datetime_utc'].where((rawData['start_datetime_utc'] > dt.datetime(2020, 10, 25, 1, 0)) & (rawData['start_datetime_utc'] <= dt.datetime(2021,3,21,1,0))==False , rawData['start_datetime_utc'] - dt.timedelta(hours = 1))
        rawData['start_datetime_utc'] = rawData['start_datetime_utc'].where((rawData['start_datetime_utc'] > dt.datetime(2021,10,31,1,0)) & (rawData['start_datetime_utc'] <= dt.datetime(2022,3,27,1,0))==False , rawData['start_datetime_utc'] - dt.timedelta(hours = 1))
        rawData['start_datetime_utc'] = rawData['start_datetime_utc'].where( rawData['start_datetime_utc'] < dt.datetime(2022,10,30,1,0) , rawData['start_datetime_utc'] - dt.timedelta(hours = 1))

        rawData['end_datetime_utc'] = pd.to_datetime(rawData['end_datetime_utc'],format = '%Y-%m-%dT%H:%M:%SZ') + dt.timedelta(hours = 2)
        rawData['end_datetime_utc'] = rawData['end_datetime_utc'].where((rawData['end_datetime_utc'] >= dt.datetime(2020, 3, 29, 1, 0)) , rawData['end_datetime_utc'] - dt.timedelta(hours = 1))
        rawData['end_datetime_utc'] = rawData['end_datetime_utc'].where((rawData['end_datetime_utc'] > dt.datetime(2020, 10, 25, 1, 0)) & (rawData['end_datetime_utc'] <= dt.datetime(2021,3,21,1,0))==False , rawData['end_datetime_utc'] - dt.timedelta(hours = 1))
        rawData['end_datetime_utc'] = rawData['end_datetime_utc'].where((rawData['end_datetime_utc'] > dt.datetime(2021,10,31,1,0)) & (rawData['end_datetime_utc'] <= dt.datetime(2022,3,27,1,0))==False , rawData['end_datetime_utc'] - dt.timedelta(hours = 1))
        rawData['end_datetime_utc'] = rawData['end_datetime_utc'].where( rawData['end_datetime_utc'] < dt.datetime(2022,10,30,1,0) , rawData['end_datetime_utc'] - dt.timedelta(hours = 1))

        #dates have type string and format YYYY-MM-DD HH:MM:SSZZ:ZZ.
        #Convert to class pandas._libs.tslibs.timestamps.Timestamp. Data already utc corrected. Save offset in x['start_utc_offset'] and x['end_utc_offset'].
        rawData2['start_utc_offset'] = rawData2.apply(lambda x: dt.timedelta(hours = int(x['session_start_datetime'][-5:-3])),axis =1)
        rawData2['session_start_datetime'] = rawData2.apply(lambda x: x['session_start_datetime'][:-6],axis =1)
        rawData2['end_utc_offset'] = rawData2.apply(lambda x: dt.timedelta(hours = int(x['session_end_datetime'][-5:-3])),axis =1)
        rawData2['session_end_datetime'] = rawData2.apply(lambda x: x['session_end_datetime'][:-6],axis =1)

        rawData2['start_datetime_utc'] = pd.to_datetime(rawData2['session_start_datetime'],format = '%Y-%m-%d %H:%M:%S') 
        rawData2['end_datetime_utc'] = pd.to_datetime(rawData2['session_end_datetime'],format = '%Y-%m-%d %H:%M:%S')

        rawData2['total_energy'] = rawData2['session_kwh']
        #to make comparable, we use symbols only, not asterixes or dashes in the card_ids.
        rawData2['card_id'] = rawData2['session_auth_id'].apply(lambda x: str(x).replace("-", ""))
        rawData['card_id'] = rawData['card_id'].apply(lambda x: x.replace("-", ""))        
        rawData2['card_id'] = rawData2['card_id'].apply(lambda x: str(x).replace("*", ""))
        rawData['card_id'] = rawData['card_id'].apply(lambda x: x.replace("*", ""))

        # added this to remove *1 and *2 from evse_uid!
        rawData2['chargepoint_id'] = rawData2['evse_uid'].astype(str)
        rawData2['chargepoint_id'] = rawData2['evse_uid'].str.split('*').str[0]

        # added this to avoid having NaN Values in the cluster analysis!
        rawData2['transaction'] = rawData2['session_id'].apply(lambda x: str(x).replace("NLLMS", ""))
        
        # concatenate both datasets and reset panda indices
        rawData = pd.concat([rawData, rawData2], join = 'outer')
        rawData.reset_index(drop=True,inplace=True)

        # added this to correct faulty user IDs. Backend sometimes drops last digit of ID. We compare them here and correct for that. 
        # till Oct 2022 manually checked that those are likely the same IDs based on non-overlapping times and energy consumption.
        for id in pd.unique(rawData['card_id']):
            if len(rawData[rawData['card_id']==id[:-1]]) > 0:
                rawData.loc[rawData['card_id']==id[:-1],'card_id'] = id

        # Added this for drop_duplicates to work properly. 
        # Since the field 'transaction' was an python object, the drop was not working! =( - hated it
        rawData['transaction'] = rawData['transaction'].astype('int')  
        # Since the field 'chargepoint_id' was an python object, the drop was not working! =( - hated it
        rawData['chargepoint_id'] = rawData['chargepoint_id'].astype('str')  

        # Check and remove duplicated sessions, only keep the first instance 
        rawData.drop_duplicates(subset=['transaction'], keep='first', inplace=True)

        rawData.to_csv('rawDataConcatenated.csv', index=False) 
        
        # Calculate dwell time
        rawData['dwell_time_utc'] = rawData['end_datetime_utc'] - rawData['start_datetime_utc']
        
        # If applicable, define default capacity
        if defaultCapacity is not None:
            rawData['capacity'] = defaultCapacity

        # If applicable, define default power limit per car
        if defaultPower is not None:
            rawData['maxPower'] = defaultPower

        # Transform start/end/dwell time into hours! Only relative to day time, not time since start.
        rawData['start_datetime_hours'] = rawData['start_datetime_utc'].dt.hour + rawData['start_datetime_utc'].dt.minute/60 + rawData['start_datetime_utc'].dt.second /3600
        rawData['end_datetime_hours'] = rawData['end_datetime_utc'].dt.hour + rawData['end_datetime_utc'].dt.minute/60 + rawData['end_datetime_utc'].dt.second /3600
        rawData['dwell_time_hours'] = rawData['dwell_time_utc']/ np.timedelta64(1, 'h') 

        # Transform start/end/dwell time into seconds! Only relative to day time, not time since start.
        rawData['start_datetime_seconds'] = rawData['start_datetime_utc'].dt.hour*3600 + rawData['start_datetime_utc'].dt.minute*60 + rawData['start_datetime_utc'].dt.second
        rawData['end_datetime_seconds'] = rawData['end_datetime_utc'].dt.hour*3600 + rawData['end_datetime_utc'].dt.minute*60 + rawData['end_datetime_utc'].dt.second 
        rawData['dwell_time_seconds'] = rawData['dwell_time_utc']/ np.timedelta64(1, 's')

        # Transform start/end/dwell time into seconds till start of filter! Only relevant for test data conversion, not for statistical analysis
        rawData['start_secondsSinceStart'] = (rawData['start_datetime_utc'] - afterStartDate).dt.total_seconds()
        rawData['end_secondsSinceStart'] = (rawData['end_datetime_utc'] - afterStartDate).dt.total_seconds()

        # Transform energy from kWh to Wh
        rawData['total_energy_Wh'] = rawData['total_energy']*1000
        rawData['average_power_W'] = rawData['total_energy_Wh']/rawData['dwell_time_hours']

        print(rawData.dtypes)

        #initialize here before applying filters
        self.filteredData = rawData

        #filter data to be within certain (start) time period.
        #note cutoff value <= 6 January means midnight between 5 and 6 January. Included time to make it more intuitive. 
        if startTimeFilter == True:
            filterArgument = 'start_datetime_utc'
            self.filteredData = self.filteredData[(self.filteredData[filterArgument] >= afterStartDate)&(self.filteredData[filterArgument] <= beforeEndDate)]
        #filter data where the charged energy < 1 kW 
        if energyFilter == True:
            filterArgument = 'total_energy'
            self.filteredData = self.filteredData[self.filteredData[filterArgument] >= energyCutOff]
        #filter data where the dwell time is larger than one day (> 24 hours) 
        if maxDwellTime is not None:
            filterArgument = 'dwell_time_hours'
            self.filteredData = self.filteredData[self.filteredData[filterArgument] <= maxDwellTime]  
        #filter data where the dwell time is less than 10 min (< 0.16666 hours) 
        if minDwellTime is not None:
            filterArgument = 'dwell_time_hours'
            self.filteredData = self.filteredData[self.filteredData[filterArgument] >= minDwellTime]  
        #filter data where EVs stay past midnight if overnightStays = False
        if not overnightStays:
            self.filteredData = self.filteredData[self.filteredData['start_datetime_utc'].dt.day == self.filteredData['end_datetime_utc'].dt.day]

        #filter data where non-unique card ids (e.g. Plug & Charge)
        if idFilter is not None:
            for aspect in idFilter:
                self.filteredData = self.filteredData[self.filteredData['card_id'] != aspect]
        
        #filter data for only managers MENNEKES CHARGERS
        # If managersFilter == NONE, the dataframe is not touched. So, the analysis is done with all data!!! 
        if managersFilter == True: # Create a dataframe without Mennekes managers
            print('manager filter true')
            for aspect in listmanagersFilter:
                self.filteredData = self.filteredData[self.filteredData['chargepoint_id'] != aspect]
            #self.filteredData.to_csv('datawithoutmanagers.csv', index=True)
        if managersFilter == False: # Create a dataframe ONLY for Mennekes managers 
            print('manager filter false')
            self.filteredData = self.filteredData[self.filteredData['chargepoint_id'].isin(listmanagersFilter)]
            #self.filteredData.to_csv('onlydatamanagers.csv', index=True)

        #sort by start date. Relevant for sampling process in ProvideHomeCommute study where we take the last x sessions in the dataset
        self.filteredData = self.filteredData.sort_values(by=['start_secondsSinceStart'],ignore_index=True)
        
        self.rawData = rawData
        self.filteredData.to_excel("filteredData.xlsx")
    
        return self.filteredData

    #dwell is in seconds
    def online_estimate_end(self, carStats = None, dwell = 0, constant = 6*3600):
        if carStats is not None:
            self.filteredData = self.filteredData.merge(carStats, on = 'card_id', how = 'left')
            self.filteredData['count'] = self.filteredData['count'].fillna(0)
            for aspect in ['mean', 'max', 'min', '75', '50', '25']:
                self.filteredData['end_sreal_d{}'.format(aspect)] = self.filteredData['start_secondsSinceStart'] + self.filteredData['dwell_time_{}'.format(aspect)].fillna(dwell).apply(lambda x: max(x,dwell))
                #dwell times of less than 15 min gives errors in DENKit for timeBase = 15 min. Also do for 30 min in extreme cases, but usually 15 min filters suffice in our data. 
        self.filteredData['end_sreal_dconstant_sinceStart'] = self.filteredData['start_secondsSinceStart'] + constant
        self.filteredData['end_sreal_dconstant_sinceMidnight'] = self.filteredData['start_datetime_seconds'] + constant
        return self.filteredData
 