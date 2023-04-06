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

import requests
import numpy as np
import pandas as pd
from datetime import datetime
from pytz import timezone

#define a class to swipe simulation data from influxDB to a dataframe in python.
#FIXME add save to Excel

class DataSwipe:
    def __init__(self, startTime, endTime, timeBase, url = "http://localhost:8086/query", dbname = "dem"):
        # Database settings
        self.url = url
        self.dbname = dbname
        self.username = 	""	# Note, default this can remain empty! It is not the same as in Grafana!
        self.password = 	""	# Note, default this can remain empty! It is not the same as in Grafana!

        # What time
        self.timeZone = timezone('Europe/Amsterdam')
        self.startTime = startTime 	#YYYY, MM, DD
        self.endTime = endTime 		#YYYY, MM, DD
        # How to retrieve the data
        self.timeBase = timeBase 

        #create list and dataframe for data
        self.result = []
        self.df = {}  # used for data per component
        self.dfAgg = {} # used for data aggregated per component type

    def dataSwipe(self, field, measurement, condition, operator = 'mean', wipeResult = True):
        # Query (what we request)
        query = 	'SELECT ' + operator + '(\"' + field + '\") FROM \"' + measurement + '\" WHERE ' + condition + ' AND time >= ' + str(self.startTime) + '000000000 AND time < ' + str(self.endTime) + '000000000 GROUP BY time(' + str(self.timeBase) + 's) fill(previous) ORDER BY time ASC'

        # Request the data
        payload = {}
        payload['db'] = self.dbname
        payload['u'] = self.username
        payload['p'] = self.password
        payload['q'] = query

        r = requests.get(self.url, params=payload)

        if wipeResult:
            self.result = []

        # Handle the data
        if('series' in r.json()['results'][0]):
            d = r.json()['results'][0]['series'][0]['values']
            for value in d:
                self.result.append(value[1])
        else:  
            print("No data!!!\n",query)
            return


    #add result of one query to an entry in a dataframe
    #only do so if result non-empty
    def dataAppend(self, key, nonZero = True):
        if nonZero == False:
            self.df[key] = np.array(self.result)
        else:
            if len(self.result) != 0:
                self.df[key] = np.array(self.result)
            else:
                #print('[MESSAGE:] empty result in {}'.format(key))
                return

    def dataEnergy(self,carStats,case):
        eReal = []
        ePlan = []
        for car in enumerate(carStats['card_id']):
            eReal = eReal + [round(sum(self.df["{}W-power.real.c.ELECTRICITY_ElectricVehicle-{}".format(case,car[0])]/4),5)]
            if "{}W-power.plan.real.c.ELECTRICITY_ElectricVehicle-{}".format(case,car[0]) in self.df:
                ePlan = ePlan + [round(sum(self.df["{}W-power.plan.real.c.ELECTRICITY_ElectricVehicle-{}".format(case,car[0])]/4),5)]
            else:
                print("no data for {}W-power.plan.real.c.ELECTRICITY_ElectricVehicle-{}".format(case,car[0]))
                ePlan = ePlan + [float("nan")]
        carStats['{}eReal'.format(case)] = eReal
        carStats['{}ePlan'.format(case)] = ePlan
        return carStats

    def dataENS(self,carStats,case,baseCase):
        #ENS of cars in given case realization compared to baseline (e.g. greedy). Absolute and relative.
        carStats["{}ENSrealAbs".format(case)] = carStats["{}eReal".format(baseCase)] - carStats["{}eReal".format(case)]
        carStats["{}ENSrealAbs".format(case)] = carStats["{}ENSrealAbs".format(case)].mask(carStats["{}ENSrealAbs".format(case)]<0,0)
        carStats["{}ENSrealRelative".format(case)] = carStats["{}ENSrealAbs".format(case)]/carStats["{}eReal".format(baseCase)]

        #ENS of cars in given case offline planning compared to baseline (e.g. greedy). Absolute and relative.
        carStats["{}ENSplanAbs".format(case)] = carStats["{}eReal".format(baseCase)] - carStats["{}ePlan".format(case)] #0 for greedy cases
        carStats["{}ENSplanAbs".format(case)] = carStats["{}ENSplanAbs".format(case)].mask(carStats["{}ENSplanAbs".format(case)]<0,0)
        carStats["{}ENSplanRelative".format(case)] = carStats["{}ENSplanAbs".format(case)]/carStats["{}eReal".format(baseCase)]

        #ENS of cars in given case realization compared to offline planning. Absolute and relative.
        carStats["{}ENScaseInternalAbs".format(case)] = carStats["{}ePlan".format(case)] - carStats["{}eReal".format(case)] #0 for greedy cases
        carStats["{}ENScaseInternalAbs".format(case)] = carStats["{}ENScaseInternalAbs".format(case)].mask(carStats["{}ENScaseInternalAbs".format(case)]<0,0)
        carStats["{}ENScaseInternalRelative".format(case)] = carStats["{}ENScaseInternalAbs".format(case)]/carStats["{}ePlan".format(case)]
        return carStats

    def dataPowerAgg(self,case,realCase = None):
        if realCase is not None:
            field = 		"W-power.real.c.ELECTRICITY"	            # Field you'd like to read
            measurement = 	"{}devices".format(realCase)				# Either: devices, controllers, host, flows
            condition = 	"\"devtype\" = 'BufferTimeshiftable'"		# Name of the element you want to have the data of
    
            self.dataSwipe(field,measurement,condition,'sum', True)
            #save sweeped data to dataframe
            self.dataAppend('{}W-power.real.c.ELECTRICITY_BufferTimeshiftable'.format(case))

            field = 		"W-power.real.c.ELECTRICITY"	            # Field you'd like to read
            measurement = 	"{}devices".format(case)				    # Either: devices, controllers, host, flows
            condition = 	"\"devtype\" = 'BufferTimeshiftable'"		# Name of the element you want to have the data of

            self.dataSwipe(field,measurement,condition,'sum', True)
            #save sweeped data to dataframe
            self.dataAppend('{}W-power.plan.real.c.ELECTRICITY_BufferTimeshiftable'.format(case))
            
        else:
            field = 		"W-power.real.c.ELECTRICITY"	            # Field you'd like to read
            measurement = 	"{}devices".format(case)				    # Either: devices, controllers, host, flows
            condition = 	"\"devtype\" = 'BufferTimeshiftable'"		# Name of the element you want to have the data of

            self.dataSwipe(field,measurement,condition,'sum', True)
            #save sweeped data to dataframe
            self.dataAppend('{}W-power.real.c.ELECTRICITY_BufferTimeshiftable'.format(case))

            field = 		"W-power.plan.real.c.ELECTRICITY"	        # Field you'd like to read
            measurement = 	"{}controllers".format(case)				# Either: devices, controllers, host, flows
            condition = 	"\"ctrltype\" = 'BufferTimeshiftableController'"		# Name of the element you want to have the data of
    
            self.dataSwipe(field,measurement,condition,'sum', True)
            #save sweeped data to dataframe
            self.dataAppend('{}W-power.plan.real.c.ELECTRICITY_BufferTimeshiftable'.format(case))    
    
    def dataGetEVdata(self,filteredData,evs,cases,extendedCases,baseCase):
        #per EV and scenario, sweep data
        for case in cases:
            if case + "realized_" in extendedCases:
                for ev in evs:
                    #realization of charging power
                    #since we have two cases (Cx_ and Cx_realized_), we read the real data from the Cx_realized_ simulation
                    field = "W-power.real.c.ELECTRICITY"
                    measurement = "{}devices".format(case + "realized_")
                    condition = "\"name\" = 'ElectricVehicle-{}'".format(ev)
                    self.dataSwipe(field, measurement, condition, 'mean', True)
                    self.dataAppend('{}{}_ElectricVehicle-{}'.format(case, field, ev))
                
                    #offline planning of charging power based on estimations
                    #since we have two cases (Cx_ and Cx_realized_), we read the real data from the Cx_device instead of the plan from the Cx_controller
                    field = "W-power.real.c.ELECTRICITY"
                    measurement = "{}devices".format(case)
                    condition = "\"name\" = 'ElectricVehicle-{}'".format(ev)
                    self.dataSwipe(field, measurement, condition, 'mean', True)
                    self.dataAppend('{}{}_ElectricVehicle-{}'.format(case, "W-power.plan.real.c.ELECTRICITY", ev),True)
                #calculate the energy served for all cars in current case. Absolute and Relative
                filteredData = self.dataEnergy(filteredData,case)
                #calculate the energy NOT served for all cars in current case in offline planning and realization, compared to baseline (e.g. greedy). Both absolute and relative ENS
                filteredData = self.dataENS(filteredData,case,baseCase)
                #swipe aggregated BTS power planned and realized
                self.dataPowerAgg(case,case + "realized_")

            elif case == baseCase:
                for ev in evs:
                    #realization of charging power
                    field = "W-power.real.c.ELECTRICITY"
                    measurement = "{}devices".format(case)
                    condition = "\"name\" = 'ElectricVehicle-{}'".format(ev)
                    self.dataSwipe(field, measurement, condition, 'mean', True)
                    self.dataAppend('{}{}_ElectricVehicle-{}'.format(case, field, ev))

                    #offline planning of charging power based on estimations
                    self.dataAppend('{}{}_ElectricVehicle-{}'.format(case, "W-power.plan.real.c.ELECTRICITY", ev),True)        
                filteredData = self.dataEnergy(filteredData,case)
                #calculate the energy NOT served for all cars in current case in offline planning and realization, compared to baseline (perfect information). Both absolute and relative ENS
                filteredData = self.dataENS(filteredData,case,baseCase)    
                #swipe aggregated BTS power planned and realized
                self.dataPowerAgg(case, case)

            else:
                for ev in evs:
                    #realization of charging power
                    field = "W-power.real.c.ELECTRICITY"
                    measurement = "{}devices".format(case)
                    condition = "\"name\" = 'ElectricVehicle-{}'".format(ev)
                    self.dataSwipe(field, measurement, condition, 'mean', True)
                    self.dataAppend('{}{}_ElectricVehicle-{}'.format(case, field, ev))

                    #offline planning of charging power based on estimations
                    field = "W-power.plan.real.c.ELECTRICITY"
                    measurement = "{}controllers".format(case)
                    condition = "\"name\" = 'ElectricVehicleController{}'".format(ev)
                    self.dataSwipe(field, measurement, condition, 'mean', True)
                    self.dataAppend('{}{}_ElectricVehicle-{}'.format(case, field, ev),True)
                #calculate the energy served for all cars in current case. Absolute and Relative
                filteredData = self.dataEnergy(filteredData,case)
                #calculate the energy NOT served for all cars in current case in offline planning and realization, compared to baseline (perfect information). Both absolute and relative ENS
                filteredData = self.dataENS(filteredData,case,baseCase)
                #swipe aggregated BTS power planned and realized
                self.dataPowerAgg(case)
        return filteredData

    def dataGlobalMeasures(self,carStats, cases, baseCase):
        ENS = ['ENSrealAbs', 'ENSrealRelative','ENSplanAbs', 'ENSplanRelative', 'ENScaseInternalAbs', 'ENScaseInternalRelative']
        #number of cars (to take average over)
        n = len(carStats)
        #self.dfAggHeaders = ["ENSrealAbs_sum", "ENSrealAbs_average", "ENSrealAbs_max","ENSrealAbs_min", "ENSrealRelative_sum", "ENSrealRelative_average","ENSrealRelative_max","ENSrealRelative_min",  "ENSplanAbs_sum", "ENSplanAbs_average", "ENSplanAbs_max","ENSplanAbs_min", "ENSplanRelative_sum", "ENSplanRelative_average", "ENSplanRelative_max","ENSplanRelative_min", "ENScaseInternalAbs_sum", "ENScaseInternalAbs_average", "ENScaseInternalAbs_max","ENScaseInternalAbs_min", "ENScaseInternalRelative_sum", "ENScaseInternalRelative_average", "ENScaseInternalRelative_max","ENScaseInternalRelative_min", "powerPeakReal", "powerPeakReal_CompC0Abs", "powerPeakReal_CompC0Rel", "powerPeakReal_CompC8Abs", "powerPeakReal_CompC8Rel", "powerPeakPlan", "powerPeakPlan_CompC0Abs", "powerPeakPlan_CompC0Rel", "powerPeakPlan_CompC8Abs", "powerPeakPlan_CompC8Rel", "eRealAgg", "ePlanAgg"]
        self.dfAggHeaders = ["ENSrealAbs_sum", "ENSrealAbs_average", "ENSrealAbs_max","ENSrealAbs_min", "ENSrealRelative_sum", "ENSrealRelative_average","ENSrealRelative_max","ENSrealRelative_min",  "ENSplanAbs_sum", "ENSplanAbs_average", "ENSplanAbs_max","ENSplanAbs_min", "ENSplanRelative_sum", "ENSplanRelative_average", "ENSplanRelative_max","ENSplanRelative_min", "ENScaseInternalAbs_sum", "ENScaseInternalAbs_average", "ENScaseInternalAbs_max","ENScaseInternalAbs_min", "ENScaseInternalRelative_sum", "ENScaseInternalRelative_average", "ENScaseInternalRelative_max","ENScaseInternalRelative_min", "powerPeakReal", "powerPeakReal_CompC0Abs", "powerPeakReal_CompC0Rel", "powerPeakPlan", "powerPeakPlan_CompC0Abs", "powerPeakPlan_CompC0Rel", "eRealAgg", "ePlanAgg"]
        for case in cases:
            temp = []
            #get global ENS averages (both absolute and relative over offline planning and online realization)
            for ens in ENS:
                temp = temp + [sum(carStats['{}{}'.format(case,ens)])] #ENSxxx_sum
                temp = temp + [temp[-1]/n] #ENSxxx_average
                temp = temp + [carStats['{}{}'.format(case,ens)].max()] #ENSxxx_max
                temp = temp + [carStats['{}{}'.format(case,ens)].min()] #ENSxxx_min

            #get global power peaks powerPeakReal and powerPeakPlan (if applicable), both absolute and relative to baseCase
            temp = temp + [max(self.df['{}W-power.real.c.ELECTRICITY_BufferTimeshiftable'.format(case)]), max(self.df['{}W-power.real.c.ELECTRICITY_BufferTimeshiftable'.format(baseCase)]) - max(self.df['{}W-power.real.c.ELECTRICITY_BufferTimeshiftable'.format(case)]), max(self.df['{}W-power.real.c.ELECTRICITY_BufferTimeshiftable'.format(case)]) / max(self.df['{}W-power.real.c.ELECTRICITY_BufferTimeshiftable'.format(baseCase)])]#max(self.df['{}W-power.real.c.ELECTRICITY_BufferTimeshiftable'.format(baseCase)])/max(self.df['{}W-power.real.c.ELECTRICITY_BufferTimeshiftable'.format(case)])]
            #temp = temp +[max(self.df['{}W-power.real.c.ELECTRICITY_BufferTimeshiftable'.format(case)]) - max(self.df['{}W-power.real.c.ELECTRICITY_BufferTimeshiftable'.format("C8_")]), max(self.df['{}W-power.real.c.ELECTRICITY_BufferTimeshiftable'.format(case)]) / max(self.df['{}W-power.real.c.ELECTRICITY_BufferTimeshiftable'.format("C8_")])]
            if '{}W-power.plan.real.c.ELECTRICITY_BufferTimeshiftable'.format(case) in self.df:
                temp = temp + [max(self.df['{}W-power.plan.real.c.ELECTRICITY_BufferTimeshiftable'.format(case)]), max(self.df['{}W-power.real.c.ELECTRICITY_BufferTimeshiftable'.format(baseCase)]) - max(self.df['{}W-power.real.c.ELECTRICITY_BufferTimeshiftable'.format(case)]), max(self.df['{}W-power.plan.real.c.ELECTRICITY_BufferTimeshiftable'.format(case)])/ max(self.df['{}W-power.real.c.ELECTRICITY_BufferTimeshiftable'.format(baseCase)])]#max(self.df['{}W-power.plan.real.c.ELECTRICITY_BufferTimeshiftable'.format(baseCase)])/ max(self.df['{}W-power.plan.real.c.ELECTRICITY_BufferTimeshiftable'.format(case)])]
                #temp = temp + [max(self.df['{}W-power.plan.real.c.ELECTRICITY_BufferTimeshiftable'.format(case)]) - max(self.df['{}W-power.real.c.ELECTRICITY_BufferTimeshiftable'.format("C8_")]), max(self.df['{}W-power.plan.real.c.ELECTRICITY_BufferTimeshiftable'.format(case)])/ max(self.df['{}W-power.real.c.ELECTRICITY_BufferTimeshiftable'.format("C8_")])]
            else:
                temp = temp + [float("nan"), float("nan"), float("nan"), float("nan"), float("nan")]
                print('[MESSAGE:] Exceptions as above only for greedy uncontrolled case C1_. Current case = ', case)

            #get global energy volumes (planned and realized):
            temp = temp + [round(sum(self.df['{}W-power.real.c.ELECTRICITY_BufferTimeshiftable'.format(case)])/4,5)]
            if '{}W-power.plan.real.c.ELECTRICITY_BufferTimeshiftable'.format(case) in self.df:
                temp = temp + [round(sum(self.df['{}W-power.plan.real.c.ELECTRICITY_BufferTimeshiftable'.format(case)])/4,5)]
            else: 
                temp = temp + [float("nan")]
            self.dfAgg[case] = temp


    #print to excel
    def dataSaveExcel(self, fileName, write=True):
        outputDataDf = pd.DataFrame.from_dict(self.df).transpose()
        try:
            outputDataDfAgg = pd.DataFrame.from_dict(self.dfAgg, orient='index', columns = self.dfAggHeaders)
            self.outputDataDfAgg = outputDataDfAgg
        except:
            print('[MESSAGE]: No dataframe with aggregated metrics')
        if write:
            with pd.ExcelWriter(fileName + ".xlsx") as writer:
                outputDataDf.to_excel(writer, sheet_name='timeSeries')
                try:
                    outputDataDfAgg.to_excel(writer, sheet_name='globalMeasures')
                except:print('[MESSAGE]: Written time series to Excel.\n')

    def dataRead(self,fileName):
        self.outputDataDf = pd.read_excel(fileName + '.xlsx', sheet_name='timeSeries', index_col=0).transpose()        
        self.outputDataDfAgg = pd.read_excel(fileName + '.xlsx', sheet_name='globalMeasures', index_col=0)