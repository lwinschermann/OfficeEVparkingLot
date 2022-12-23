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
 
""""
========================== EV data - ASR - Statistical Analysis  =========================
"""

import itertools
import os, sys
import numpy as np
from filter_data_process import FilteringData
from statistical_analysis import StatisticalAnalysis
from demkit_sessions_datainput import DemkitSessions
import datetime
import math
from config_path import Config

Config.initializePath()

""""
========================== EV data - ASR - Filtering process   =========================
"""

# Instanciating FilteringData
data = FilteringData()

print("Done filtering data!")
print("===================================================================================================================================")


""""
========================== EV data - ASR - Statistical Analysis  =========================
"""
# Training data
trainingData = data.filter_data(startTimeFilter = True, 
                                    afterStartDate = datetime.datetime(2020, 1, 1, 0, 0), 
                                    beforeEndDate = datetime.datetime(2022, 8, 31, 23, 59), 
                                    energyFilter = True, 
                                    energyCutOff = 1,
                                    maxDwellTime = 24,
                                    minDwellTime = 10/60,
                                    defaultCapacity = 100000,
                                    defaultPower = 7400,
                                    managersFilter = None, # True: analysis without manangers | False: analysis ONLY managers | None: Analysis considering all together! 
                                    listmanagersFilter = ['1000019032','1000019086','1000019033','1000019034','1000019030','1000019031','1000019036','1000019029','1000019035','1000019037','1000019040','1000019038','1000019039','1000011271','1000011271','1000011272','1000011272','1000011273','1000011273','1000011274','1000011255','1000011255','1000011275','1000011275','1000011276','1000011276','1000011277','1000011277','1000011278','1000011278','1000011317'],
                                    idFilter = ['Plug & charge', '1 (chip)', '2 (chip)', '3 (chip)', '4 (chip)', '5 (chip)', '6 (chip)', '7 (chip)', '8 (chip)', '9 (chip)', '10 (chip)', '11 (chip)', '12 (chip)', '13 (chip)', '14 (chip)', '15 (chip)', '16 (chip)', '17 (chip)', '18 (chip)', '19 (chip)', '20 (chip)', '21 (chip)', '22 (chip)', 'Anoniem'])

print(trainingData)

#simple code to get max number of sessions per day
#init = datetime.datetime(2020, 1, 1, 0, 0)
#maxParallelSessions = 0
#while init < datetime.datetime(2022, 8, 31, 23, 59):
#    maxParallelSessions = max(maxParallelSessions, len(trainingData[(trainingData["start_datetime_utc"]>init)&(trainingData["start_datetime_utc"]<init + datetime.timedelta(hours = 24))]))
#    init = init + datetime.timedelta(hours=24)
#print("max number of sessions per day = ",maxParallelSessions)

# for paramtersweep, instantiate percentiles you wanna check.
percentiles = np.arange(21)
steps = math.ceil(100/percentiles[-1])
percentiles = percentiles*steps

# Instanciating statistical analysis
aggstats = StatisticalAnalysis(trainingData)

# Energy demand stats
aggstats.energy_demand_plots()
carStats = aggstats.energy_demand_stats(percentiles = percentiles)
energyGlobalAverage = round(np.mean(carStats['energy_mean']))

#estimate maximum charging power
carStats = aggstats.power_estimation_stats(carStats,data.defaultPower,data.defaultCapacity)

# starting time stats
aggstats.start_time_plots()
carStats = aggstats.start_time_stats(carStats, percentiles)

## end time stats
aggstats.end_time_plots()
carStats = aggstats.end_time_stats(carStats)

# dwell time stats
aggstats.dwell_time_plots()
carStats = aggstats.dwell_time_stats(carStats,dwell=30*60, percentiles=percentiles)

# correlation analysis
aggstats.correlation()

# individual stats per car 
indstats = aggstats.stats_per_car(carStats)

print("Done stats!")
print("===================================================================================================================================")

""""
========================== EV data - ASR - Generating data sessions for DEMKit  =========================
"""
## Test data
filteredData = data.filter_data(startTimeFilter = True, 
                                    afterStartDate = datetime.datetime(2022, 8, 31, 0, 0), 
                                    beforeEndDate = datetime.datetime(2022, 8, 31, 23, 59), 
                                    energyFilter = True, 
                                    energyCutOff = 1,
                                    maxDwellTime = 24,
                                    minDwellTime = 30/60, #increased filter to 30 min due to feasibility constraints in DEMKit
                                    defaultCapacity = 100000,
                                    defaultPower = 7400,
                                    managersFilter = None, # True: analysis without manangers | False: analysis ONLY managers | None: Analysis considering all together! 
                                    listmanagersFilter = ['1000019032','1000019086','1000019033','1000019034','1000019030','1000019031','1000019036','1000019029','1000019035','1000019037','1000019040','1000019038','1000019039','1000011271','1000011271','1000011272','1000011272','1000011273','1000011273','1000011274','1000011255','1000011255','1000011275','1000011275','1000011276','1000011276','1000011277','1000011277','1000011278','1000011278','1000011317'],
                                    idFilter = ['Plug & charge', '1 (chip)', '2 (chip)', '3 (chip)', '4 (chip)', '5 (chip)', '6 (chip)', '7 (chip)', '8 (chip)', '9 (chip)', '10 (chip)', '11 (chip)', '12 (chip)', '13 (chip)', '14 (chip)', '15 (chip)', '16 (chip)', '17 (chip)', '18 (chip)', '19 (chip)', '20 (chip)', '21 (chip)', '22 (chip)', 'Anoniem'])



#add columns with end times based on real start times and estimated dwell times
filteredData = data.online_estimate_end(carStats, dwell = 30*60, constant = 8*3600)
#FIXME untested. Check if edit of where to apply lambda mask is still working in online estimate end.Then clean up.
""""
========================== C7_ parameter sweep =========================
"""
#information on estimated start time and dwell time and energy. We sweep over percentiles of energy and the combination start/dwell. 
# Instanciating demkit sessions data input

sessions = DemkitSessions(dataStats=filteredData, dataReal=filteredData, ScenarioPrefix="C7_")

# make txt files for DEMKit to define sessions. We take count and specs from S07_, and generate a whole bunch of energy/start/end files.
#FIXME error if multiple sessions per car with default value/estimated value! Make default dynamic (consequtive days with same times for example)

for aspect in percentiles:#itertools.product([percentiles, percentiles]):

    sessions.generateDemkitSessionInput(keyStats = 'e{}'.format(aspect), 
                        keyReal = 'total_energy_Wh', 
                        filePrefix = '{}'+'e{}_ElectricVehicle_RequiredCharge'.format(aspect),
                        rounding = 'up',
                        staticDefault = energyGlobalAverage)

    sessions.generateDemkitSessionInput(keyStats = 'end_s{}d{}'.format(aspect,aspect), 
                        keyReal = 'end_secondsSinceStart', 
                        filePrefix = '{}'+'s{}d{}_ElectricVehicle_Endtimes'.format(aspect,aspect),
                        rounding = 'down',
                        staticDefault = 17*3600)

    sessions.generateDemkitSessionInput(keyStats = 's{}'.format(aspect), 
                        keyReal = 'start_secondsSinceStart', 
                        filePrefix = '{}'+'s{}_ElectricVehicle_Starttimes'.format(aspect),
                        rounding = 'up',
                        staticDefault = 9*3600)


print("Done parameter sweep DEMkit input!")
print("===================================================================================================================================")

""""
========================== C0_ =========================
"""
#no information
# Instanciating demkit sessions data input
sessions = DemkitSessions(dataStats=filteredData, dataReal=filteredData, ScenarioPrefix="C0_")

# make txt files for DEMKit to define sessions
#FIXME error if multiple sessions per car with default value/estimated value! Make default dynamic (consequtive days with same times for example)

sessions.generateDemkitSessionInput(keyStats = 'count', 
                    keyReal = 'count', 
                    filePrefix = '{}ElectricVehicle_Count',
                    rounding = None,
                    staticDefault = 0)

#we only use the realized values for perfect information scheduling
sessions.generateDemkitSessionInput(keyStats = None, 
                    keyReal = 'total_energy_Wh', 
                    filePrefix = '{}ElectricVehicle_RequiredCharge',
                    rounding = 'up',
                    staticDefault = energyGlobalAverage)

sessions.generateDemkitSessionInput(keyStats = None, 
                    keyReal = 'end_secondsSinceStart', 
                    filePrefix = '{}ElectricVehicle_Endtimes',
                    rounding = 'down',
                    staticDefault = 17*3600)

sessions.generateDemkitSessionInput(keyStats = None, 
                    keyReal = 'start_secondsSinceStart', 
                    filePrefix = '{}ElectricVehicle_Starttimes',
                    rounding = 'up',
                    staticDefault = 9*3600)

sessions.generateDemkitEVSpecsInput(keyStatsCap = 'capacity', 
                    keyStatsPower = 'maxPower', 
                    keyRealCap = 'capacity', 
                    keyRealPower = 'maxPower', 
                    filePrefix = '{}ElectricVehicle_Specs',
                    rounding = 'up')
print("===================================================================================================================================")

""""
========================== C1_ =========================
"""
#information on real start and estimated energy requirement. Offline planning modelled as greedy with estimated energy as target. Online is scenario C1.
#FIXME might have to compare to C1 intersection C2. Define in data_swipe
# Instanciating demkit sessions data input
sessions = DemkitSessions(dataStats=filteredData, dataReal=filteredData, ScenarioPrefix="C1_")

# make txt files for DEMKit to define sessions
#FIXME error if multiple sessions per car with default value/estimated value! Make default dynamic (consequtive days with same times for example)

sessions.generateDemkitSessionInput(keyStats = 'count', 
                    keyReal = 'count', 
                    filePrefix = '{}ElectricVehicle_Count',
                    rounding = None,
                    staticDefault = 0)

#we only use the realized values for perfect information scheduling
sessions.generateDemkitSessionInput(keyStats = 'energy_mean', 
                    keyReal = 'total_energy_Wh', 
                    filePrefix = '{}ElectricVehicle_RequiredCharge',
                    rounding = 'up',
                    staticDefault = energyGlobalAverage)

sessions.generateDemkitSessionInput(keyStats = None, 
                    keyReal = 'end_secondsSinceStart', 
                    filePrefix = '{}ElectricVehicle_Endtimes',
                    rounding = 'down',
                    staticDefault = 17*3600)

#uses real start times in DEMKit
sessions.generateDemkitSessionInput(keyStats = None, 
                    keyReal = 'start_secondsSinceStart', 
                    filePrefix = '{}ElectricVehicle_Starttimes',
                    rounding = 'up',
                    staticDefault = 9*3600)

sessions.generateDemkitEVSpecsInput(keyStatsCap = 'capacity', 
                    keyStatsPower = 'maxPower', 
                    keyRealCap = 'capacity', 
                    keyRealPower = 'maxPower', 
                    filePrefix = '{}ElectricVehicle_Specs',
                    rounding = 'up')
print("===================================================================================================================================")


""""
========================== C2_ =========================
"""
#information on real start and estimated energy dwell time. Offline planning modelled as greedy with estimated dwell as maximum dwell and 100.001kWh energy requirement.
#FIXME might have to compare to C1 intersection C3. Define in data_swipe
# Instanciating demkit sessions data input
sessions = DemkitSessions(dataStats=filteredData, dataReal=filteredData, ScenarioPrefix="C2_")

# make txt files for DEMKit to define sessions
#FIXME error if multiple sessions per car with default value/estimated value! Make default dynamic (consequtive days with same times for example)

sessions.generateDemkitSessionInput(keyStats = 'count', 
                    keyReal = 'count', 
                    filePrefix = '{}ElectricVehicle_Count',
                    rounding = None,
                    staticDefault = 0)

#use full battery capacity as static default.
sessions.generateDemkitSessionInput(keyStats = None, 
                    keyReal = 'total_energy_Wh', 
                    filePrefix = '{}ElectricVehicle_RequiredCharge',
                    rounding = 'up',
                    staticDefault = energyGlobalAverage)

sessions.generateDemkitSessionInput(keyStats = 'end_sreal_d50', 
                    keyReal = 'end_secondsSinceStart', 
                    filePrefix = '{}ElectricVehicle_Endtimes',
                    rounding = 'down',
                    staticDefault = 17*3600)

sessions.generateDemkitSessionInput(keyStats = None, 
                    keyReal = 'start_secondsSinceStart', 
                    filePrefix = '{}ElectricVehicle_Starttimes',
                    rounding = 'up',
                    staticDefault = 9*3600)

sessions.generateDemkitEVSpecsInput(keyStatsCap = 'capacity', 
                    keyStatsPower = 'maxPower', 
                    keyRealCap = 'capacity', 
                    keyRealPower = 'maxPower', 
                    filePrefix = '{}ElectricVehicle_Specs',
                    rounding = 'up')
print("===================================================================================================================================")


""""
========================== C3_ =========================
"""
#information on real start and estimated energy dwell time. Assume that from that info can estimate better lower bound on power max.
# Instanciating demkit sessions data input
sessions = DemkitSessions(dataStats=filteredData, dataReal=filteredData, ScenarioPrefix="C3_")

# make txt files for DEMKit to define sessions
#FIXME error if multiple sessions per car with default value/estimated value! Make default dynamic (consequtive days with same times for example)

sessions.generateDemkitSessionInput(keyStats = 'count', 
                    keyReal = 'count', 
                    filePrefix = '{}ElectricVehicle_Count',
                    rounding = None,
                    staticDefault = 0)

#use full battery capacity as static default.
sessions.generateDemkitSessionInput(keyStats = 'energy_mean', 
                    keyReal = 'total_energy_Wh', 
                    filePrefix = '{}ElectricVehicle_RequiredCharge',
                    rounding = 'up',
                    staticDefault = energyGlobalAverage)

sessions.generateDemkitSessionInput(keyStats = 'end_sreal_d50', 
                    keyReal = 'end_secondsSinceStart', 
                    filePrefix = '{}ElectricVehicle_Endtimes',
                    rounding = 'down',
                    staticDefault = 17*3600)

sessions.generateDemkitSessionInput(keyStats = None, 
                    keyReal = 'start_secondsSinceStart', 
                    filePrefix = '{}ElectricVehicle_Starttimes',
                    rounding = 'up',
                    staticDefault = 9*3600)

sessions.generateDemkitEVSpecsInput(keyStatsCap = 'capacity', 
                    keyStatsPower = 'maxPower', 
                    keyRealCap = 'capacity', 
                    keyRealPower = 'maxPower', 
                    filePrefix = '{}ElectricVehicle_Specs',
                    rounding = 'up')
print("===================================================================================================================================")




""""
========================== C4_ =========================
"""
#information on estimated start time.
# Instanciating demkit sessions data input
sessions = DemkitSessions(dataStats=filteredData, dataReal=filteredData, ScenarioPrefix="C4_")

# make txt files for DEMKit to define sessions
#FIXME error if multiple sessions per car with default value/estimated value! Make default dynamic (consequtive days with same times for example)

sessions.generateDemkitSessionInput(keyStats = 'count', 
                    keyReal = 'count', 
                    filePrefix = '{}ElectricVehicle_Count',
                    rounding = None,
                    staticDefault = 0)

#use full battery capacity as static default.
sessions.generateDemkitSessionInput(keyStats = None, 
                    keyReal = 'total_energy_Wh', 
                    filePrefix = '{}ElectricVehicle_RequiredCharge',
                    rounding = 'up',
                    staticDefault = energyGlobalAverage)

sessions.generateDemkitSessionInput(keyStats = None, 
                    keyReal = 'end_secondsSinceStart', 
                    filePrefix = '{}ElectricVehicle_Endtimes',
                    rounding = 'down',
                    staticDefault = 17*3600)

sessions.generateDemkitSessionInput(keyStats = 'start_time_mean', 
                    keyReal = 'start_secondsSinceStart', 
                    filePrefix = '{}ElectricVehicle_Starttimes',
                    rounding = 'up',
                    staticDefault = 9*3600)

sessions.generateDemkitEVSpecsInput(keyStatsCap = 'capacity', 
                    keyStatsPower = 'maxPower', 
                    keyRealCap = 'capacity', 
                    keyRealPower = 'maxPower', 
                    filePrefix = '{}ElectricVehicle_Specs',
                    rounding = 'up')
print("===================================================================================================================================")



""""
========================== C5_ =========================
"""
#information on estimated start time and energy. 
# Instanciating demkit sessions data input
sessions = DemkitSessions(dataStats=filteredData, dataReal=filteredData, ScenarioPrefix="C5_")

# make txt files for DEMKit to define sessions
#FIXME error if multiple sessions per car with default value/estimated value! Make default dynamic (consequtive days with same times for example)

sessions.generateDemkitSessionInput(keyStats = 'count', 
                    keyReal = 'count', 
                    filePrefix = '{}ElectricVehicle_Count',
                    rounding = None,
                    staticDefault = 0)

#use full battery capacity as static default.
sessions.generateDemkitSessionInput(keyStats = 'energy_mean', 
                    keyReal = 'total_energy_Wh', 
                    filePrefix = '{}ElectricVehicle_RequiredCharge',
                    rounding = 'up',
                    staticDefault = energyGlobalAverage)

sessions.generateDemkitSessionInput(keyStats = None, 
                    keyReal = 'end_secondsSinceStart', 
                    filePrefix = '{}ElectricVehicle_Endtimes',
                    rounding = 'down',
                    staticDefault = 17*3600)

sessions.generateDemkitSessionInput(keyStats = 'start_time_mean', 
                    keyReal = 'start_secondsSinceStart', 
                    filePrefix = '{}ElectricVehicle_Starttimes',
                    rounding = 'up',
                    staticDefault = 9*3600)

sessions.generateDemkitEVSpecsInput(keyStatsCap = 'capacity', 
                    keyStatsPower = 'maxPower', 
                    keyRealCap = 'capacity', 
                    keyRealPower = 'maxPower', 
                    filePrefix = '{}ElectricVehicle_Specs',
                    rounding = 'up')
print("===================================================================================================================================")




""""
========================== C6_ =========================
"""
#information on estimated start time and dwell time.
# Instanciating demkit sessions data input
sessions = DemkitSessions(dataStats=filteredData, dataReal=filteredData, ScenarioPrefix="C6_")

# make txt files for DEMKit to define sessions
#FIXME error if multiple sessions per car with default value/estimated value! Make default dynamic (consequtive days with same times for example)

sessions.generateDemkitSessionInput(keyStats = 'count', 
                    keyReal = 'count', 
                    filePrefix = '{}ElectricVehicle_Count',
                    rounding = None,
                    staticDefault = 0)

#use full battery capacity as static default.
sessions.generateDemkitSessionInput(keyStats = None, 
                    keyReal = 'total_energy_Wh', 
                    filePrefix = '{}ElectricVehicle_RequiredCharge',
                    rounding = 'up',
                    staticDefault = energyGlobalAverage)

sessions.generateDemkitSessionInput(keyStats = 'end_smean_dmean', 
                    keyReal = 'end_secondsSinceStart', 
                    filePrefix = '{}ElectricVehicle_Endtimes',
                    rounding = 'down',
                    staticDefault = 17*3600)

sessions.generateDemkitSessionInput(keyStats = 'start_time_mean', 
                    keyReal = 'start_secondsSinceStart', 
                    filePrefix = '{}ElectricVehicle_Starttimes',
                    rounding = 'up',
                    staticDefault = 9*3600)

sessions.generateDemkitEVSpecsInput(keyStatsCap = 'capacity', 
                    keyStatsPower = 'maxPower', 
                    keyRealCap = 'capacity', 
                    keyRealPower = 'maxPower', 
                    filePrefix = '{}ElectricVehicle_Specs',
                    rounding = 'up')
print("===================================================================================================================================")

""""
========================== C7_ =========================
"""
#information on estimated start time and dwell time and energy.
# Instanciating demkit sessions data input
sessions = DemkitSessions(dataStats=filteredData, dataReal=filteredData, ScenarioPrefix="C7_")

# make txt files for DEMKit to define sessions
#FIXME error if multiple sessions per car with default value/estimated value! Make default dynamic (consequtive days with same times for example)

sessions.generateDemkitSessionInput(keyStats = 'count', 
                    keyReal = 'count', 
                    filePrefix = '{}ElectricVehicle_Count',
                    rounding = None,
                    staticDefault = 0)

#use full battery capacity as static default.
sessions.generateDemkitSessionInput(keyStats = 'energy_mean', 
                    keyReal = 'total_energy_Wh', 
                    filePrefix = '{}ElectricVehicle_RequiredCharge',
                    rounding = 'up',
                    staticDefault = energyGlobalAverage)

sessions.generateDemkitSessionInput(keyStats = 'end_smean_dmean', 
                    keyReal = 'end_secondsSinceStart', 
                    filePrefix = '{}ElectricVehicle_Endtimes',
                    rounding = 'down',
                    staticDefault = 17*3600)

sessions.generateDemkitSessionInput(keyStats = 'start_time_mean', 
                    keyReal = 'start_secondsSinceStart', 
                    filePrefix = '{}ElectricVehicle_Starttimes',
                    rounding = 'up',
                    staticDefault = 9*3600)

sessions.generateDemkitEVSpecsInput(keyStatsCap = 'capacity', 
                    keyStatsPower = 'maxPower', 
                    keyRealCap = 'capacity', 
                    keyRealPower = 'maxPower', 
                    filePrefix = '{}ElectricVehicle_Specs',
                    rounding = 'up')
print("===================================================================================================================================")

""""
========================== C8_ =========================
"""
#perfect information baseline
# Instanciating demkit sessions data input
sessions = DemkitSessions(dataStats=filteredData, dataReal=filteredData, ScenarioPrefix="C8_") #formerly dataStats = carStats. But online_estimate_end() adds all carStats data to filteredData (to deal with end_sReal_dXX)

# make txt files for DEMKit to define sessions
#FIXME error if multiple sessions per car with default value/estimated value! Make default dynamic (consequtive days with same times for example)

sessions.generateDemkitSessionInput(keyStats = 'count', 
                    keyReal = 'count', 
                    filePrefix = '{}ElectricVehicle_Count',
                    rounding = None,
                    staticDefault = 0)

#we only use the realized values for perfect information scheduling
sessions.generateDemkitSessionInput(keyStats = None, 
                    keyReal = 'total_energy_Wh', 
                    filePrefix = '{}ElectricVehicle_RequiredCharge',
                    rounding = 'up',
                    staticDefault = energyGlobalAverage)

sessions.generateDemkitSessionInput(keyStats = None, 
                    keyReal = 'end_secondsSinceStart', 
                    filePrefix = '{}ElectricVehicle_Endtimes',
                    rounding = 'down',
                    staticDefault = 17*3600)

sessions.generateDemkitSessionInput(keyStats = None, 
                    keyReal = 'start_secondsSinceStart', 
                    filePrefix = '{}ElectricVehicle_Starttimes',
                    rounding = 'up',
                    staticDefault = 9*3600)

#FIXME put the realized power!! Dus de max average including test days. Or survey input
sessions.generateDemkitEVSpecsInput(keyStatsCap = 'capacity', 
                    #keyStatsPower = 'power_max_average_W', 
                    keyStatsPower = 'maxPower',
                    keyRealCap = 'capacity', 
                    keyRealPower = 'maxPower', 
                    filePrefix = '{}ElectricVehicle_Specs',
                    rounding = 'up',
                    staticDefaultPower = data.defaultPower,
                    staticDefaultCapacity = data.defaultCapacity)
print("===================================================================================================================================")

print("Done Sessions DEMKit!")
print("===================================================================================================================================")

#write carStats of simulated day to Excel to be able to call for data sweep and measure calculation after DEMKit simulation
activeCarStats = carStats.loc[carStats['card_id'].isin(sessions.dataReal['card_id'])]
activeCarStats.to_excel("carStats.xlsx")
filteredData.to_excel("filteredData.xlsx")
carStats.to_excel("allCarStats.xlsx")
# End 
print('made it till the end daaaahmn')