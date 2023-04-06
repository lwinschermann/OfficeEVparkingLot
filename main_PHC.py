#    Data analysis in OfficeEVparkingLot
#    Filter, process and analyze EV data collected at Dutch office building parking lot
#    Statistical analysis of EV data at ASR facilities - GridShield project - developed by 
#    Leoni Winschermann, University of Twente, l.winschermann@utwente.nl
#    Nataly Bañol Arias, University of Twente, m.n.banolarias@utwente.nl
#    
#    Copyright (C) 2023 CAES and MOR Groups, University of Twente, Enschede, The Netherlands
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
from survey_data_process import SurveyData
from statistical_analysis import StatisticalAnalysis
from demkit_sessions_datainput import DemkitSessions
import datetime
import math
from config_path import Config
import pandas as pd

Config.initializePath()

""""
========================== EV data - ASR - Filtering process   =========================
"""

# Instanciating SurveyData
# If done before, cleaning and processing can be skipped, and the data read from Excel.
# FIXME doubleDeal cannot handle skipProcess yet.
skipClean = False
skipProcess = False
survey = SurveyData(skipClean,skipProcess)
if not skipClean:
    surveyData = survey.clean_data()
if not skipProcess:
    surveyData = survey.process_data()

survey.doubleDeal()
survey.commuteEnergy(efficiencyDefault=199)

print("Done preprocessing survey data!")
print("===================================================================================================================================")

#survey.basicFigures()

data = FilteringData()

""""
========================== EV data - ASR - Generating data sessions for DEMKit  =========================
"""
## Test data
#FIXME we later sample the latest sessions, but not necessarily ordered by date.
filteredData = data.filter_data(startTimeFilter = True, 
                                    afterStartDate = datetime.datetime(2021, 8, 31, 0, 0), #31.08.2021 for ICT sims
                                    beforeEndDate = datetime.datetime(2023, 8, 31, 23, 59), 
                                    energyFilter = True, 
                                    energyCutOff = 1,
                                    maxDwellTime = 24,
                                    minDwellTime = 30/60, #increased filter to 30 min due to feasibility constraints in DEMKit
                                    overnightStays = False,
                                    defaultCapacity = 100000,
                                    defaultPower = 11000,#7400,
                                    managersFilter = None, # True: analysis without manangers | False: analysis ONLY managers | None: Analysis considering all together! 
                                    listmanagersFilter = ['1000019032','1000019086','1000019033','1000019034','1000019030','1000019031','1000019036','1000019029','1000019035','1000019037','1000019040','1000019038','1000019039','1000011271','1000011271','1000011272','1000011272','1000011273','1000011273','1000011274','1000011255','1000011255','1000011275','1000011275','1000011276','1000011276','1000011277','1000011277','1000011278','1000011278','1000011317'],
                                    idFilter = ['Plug & charge', '1 (chip)', '2 (chip)', '3 (chip)', '4 (chip)', '5 (chip)', '6 (chip)', '7 (chip)', '8 (chip)', '9 (chip)', '10 (chip)', '11 (chip)', '12 (chip)', '13 (chip)', '14 (chip)', '15 (chip)', '16 (chip)', '17 (chip)', '18 (chip)', '19 (chip)', '20 (chip)', '21 (chip)', '22 (chip)', 'Anoniem']
                                    )

# statistical analysis of mergedData. We use the mean energy demand in the veto button mask.
#NOTE do this based on trainingData set. Not mergedData = same as simulation input!
aggstats = StatisticalAnalysis(filteredData)
carStats = aggstats.energy_demand_stats()
carStats = aggstats.start_time_stats(carStats)
carStats = aggstats.end_time_stats(carStats)
carStats = aggstats.dwell_time_stats(carStats)
#indstats = aggstats.stats_per_car(carStats)

filteredData = data.online_estimate_end(carStats, constant = 6*3600)

filteredData, mergedData = survey.anonymize(filteredData)

# mask that determines whether veto button for fast charging would be pressed. 
# 1 = smart charging, 0 = veto, apply fast charging
# mask_hc: if we know home commute smart, if not veto
# mask_energy: if realized energy demand > x% van mean, veto
# mask_dwell: if stay < y hours, veto
mergedData = survey.veto(mergedData,energyThreshold=1.1,dwellThreshold = 6*3600)
mergedData = survey.hybridData(mergedData,'energy_vetoHcXreal_wh','veto_hc','commuteEffDefault_wh','total_energy_Wh')
mergedData = survey.hybridData(mergedData,'end_veto6hXreal_seconds','veto_hc','end_sreal_dconstant_sinceMidnight','end_datetime_seconds')



sampleSize = 400
sampleDataSmart, sampleDataFast = survey.sample(mergedData, n=sampleSize)

# for i in range(100,101):
#     sampleCombined = survey.sampleCombined(sampleDataSmart,sampleDataFast,percentSmart = i,n=sampleSize)

#     # Instanciating demkit sessions data input
#     sessions = DemkitSessions(dataStats=sampleCombined, dataReal=sampleCombined, ScenarioPrefix="PHC_sweep_11kW_"+str(i))

#     print(sampleCombined['end_veto6hXreal_seconds'])
#     sessions.generateDemkitSessionInput(keyStats = 'end_veto6hXreal_seconds', 
#                         keyReal = 'end_veto6hXreal_seconds', 
#                         filePrefix = '{}ElectricVehicle_Endtimes_TEST',
#                         rounding = 'down',
#                         staticDefault = 17*3600,
#                         perCar = False)


print("Done processing test data!")
print("===================================================================================================================================")

# sample last sessions of EVs sampled in survey
sampleData = []
for i in range(0,46):
    if len(mergedData[mergedData['EV_id']=='EV'+str(i)])>0:
        sampleData = sampleData + [mergedData[mergedData['EV_id']=='EV'+str(i)].iloc[-1]]
    else:
        print('no session data for EV',i)
sampleData = pd.DataFrame(sampleData,columns=list(mergedData.columns))

#filter out EVs without commute_wh
sampleData = sampleData[sampleData['commute_wh'] > 0 ]
print(sampleData)

print("Done sampling sessions!")
print("===================================================================================================================================")

""""
========================== PHC_ =========================
"""
# Instanciating demkit sessions data input
sessions = DemkitSessions(dataStats=sampleData, dataReal=sampleData, ScenarioPrefix="PHC_11kW")

# make txt files for DEMKit to define sessions

#use full battery capacity as static default.
sessions.generateDemkitSessionInput(keyStats = 'commute_wh', 
                    keyReal = 'total_energy_Wh', 
                    filePrefix = '{}ElectricVehicle_RequiredCharge',
                    rounding = 'up',
                    staticDefault = 20000)

sessions.generateDemkitSessionInput(keyStats = 'end_sreal_dconstant_sinceMidnight', 
                    keyReal = 'end_datetime_seconds', 
                    filePrefix = '{}ElectricVehicle_Endtimes',
                    rounding = 'down',
                    staticDefault = 17*3600)

sessions.generateDemkitSessionInput(keyStats = 'start_datetime_seconds', 
                    keyReal = 'start_datetime_seconds', 
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
========================== PHC_2_ =========================
"""
# Instanciating demkit sessions data input
sessions = DemkitSessions(dataStats=sampleData, dataReal=sampleData, ScenarioPrefix="PHC_2_11kW_")

# make txt files for DEMKit to define sessions

#use full battery capacity as static default.
sessions.generateDemkitSessionInput(keyStats = 'commuteEffDefault_wh', 
                    keyReal = 'total_energy_Wh', 
                    filePrefix = '{}ElectricVehicle_RequiredCharge',
                    rounding = 'up',
                    staticDefault = 20000)

sessions.generateDemkitSessionInput(keyStats = 'end_sreal_dconstant_sinceMidnight', 
                    keyReal = 'end_datetime_seconds', 
                    filePrefix = '{}ElectricVehicle_Endtimes',
                    rounding = 'down',
                    staticDefault = 17*3600)

sessions.generateDemkitSessionInput(keyStats = 'start_datetime_seconds', 
                    keyReal = 'start_datetime_seconds', 
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
========================== veto_ =========================
"""
# Instanciating demkit sessions data input
sessions = DemkitSessions(dataStats=sampleData, dataReal=sampleData, ScenarioPrefix="veto_11kW_")

# make txt files for DEMKit to define veto button opt-in: If True, add controller. If False, don't (=fast charging)

sessions.generateDemkitSessionInput(keyStats = 'veto_hc', 
                    keyReal = 'veto_hc', 
                    filePrefix = '{}hc',
                    rounding = None,
                    staticDefault = 1)

sessions.generateDemkitSessionInput(keyStats = 'veto_energy', 
                    keyReal = 'veto_energy', 
                    filePrefix = '{}energy',
                    rounding = None,
                    staticDefault = 1)

sessions.generateDemkitSessionInput(keyStats = 'veto_dwell', 
                    keyReal = 'veto_dwell', 
                    filePrefix = '{}dwell',
                    rounding = None,
                    staticDefault = 1)


print("===================================================================================================================================")

""""
========================== PHC_sweep_ =========================
"""
sampleSize = 400
sampleDataSmart, sampleDataFast = survey.sample(mergedData, n=sampleSize)

for i in range(0,101):
    sampleCombined = survey.sampleCombined(sampleDataSmart,sampleDataFast,percentSmart = i,n=sampleSize)

    # Instanciating demkit sessions data input
    sessions = DemkitSessions(dataStats=sampleCombined, dataReal=sampleCombined, ScenarioPrefix="PHC_sweep_11kW_"+str(i))

    # make txt files for DEMKit to define sessions
    sessions.generateDemkitSessionInput(keyStats = 'commuteEffDefault_wh', 
                        keyReal = 'total_energy_Wh', 
                        filePrefix = '{}ElectricVehicle_RequiredCharge',
                        rounding = 'up',
                        staticDefault = 20000,
                        perCar = False)
    sessions.generateDemkitSessionInput(keyStats = 'energy_vetoHcXreal_wh', 
                        keyReal = 'energy_vetoHcXreal_wh', 
                        filePrefix = '{}ElectricVehicle_RequiredCharge_vetoXsmart',
                        rounding = 'up',
                        staticDefault = 20000,
                        perCar = False)

    sessions.generateDemkitSessionInput(keyStats = 'end_sreal_dconstant_sinceMidnight', 
                        keyReal = 'end_datetime_seconds', 
                        filePrefix = '{}ElectricVehicle_Endtimes',
                        rounding = 'down',
                        staticDefault = 17*3600,
                        perCar = False)
    sessions.generateDemkitSessionInput(keyStats = 'end_veto6hXreal_seconds', 
                        keyReal = 'end_veto6hXreal_seconds', 
                        filePrefix = '{}ElectricVehicle_Endtimes_vetoXsmart',
                        rounding = 'down',
                        staticDefault = 17*3600,
                        perCar = False)

    sessions.generateDemkitSessionInput(keyStats = 'start_datetime_seconds', 
                        keyReal = 'start_datetime_seconds', 
                        filePrefix = '{}ElectricVehicle_Starttimes',
                        rounding = 'up',
                        staticDefault = 9*3600,
                        perCar = False)

    sessions.generateDemkitEVSpecsInput(keyStatsCap = 'capacity', 
                        keyStatsPower = 'maxPower', 
                        keyRealCap = 'capacity', 
                        keyRealPower = 'maxPower', 
                        filePrefix = '{}ElectricVehicle_Specs',
                        rounding = 'up',
                        perCar = False)

    sessions.generateDemkitSessionInput(keyStats = 'veto_hc', 
                        keyReal = 'veto_hc', 
                        filePrefix = '{}hc',
                        rounding = None,
                        staticDefault = 1,
                        perCar = False)

print("===================================================================================================================================")


# frequency count in mergedData per surveyed id

print("Done generating DEMKit input run!")
print("===================================================================================================================================")

# anonymize and save anonymization legend to some file

# save survey data anonymized and processed to file
# surveyData.to_excel

#write carStats of simulated day to Excel to be able to call for data sweep and measure calculation after DEMKit simulation
#activeCarStats = carStats.loc[carStats['card_id'].isin(sessions.dataReal['card_id'])]
#activeCarStats.to_excel("carStats.xlsx")
sampleData.to_excel("sampleData_11kW.xlsx")
mergedData.to_excel("mergedData_11kW.xlsx")
sampleDataSmart.append(sampleDataFast).to_excel('sampleDataSmartFast_11kW.xlsx')
carStats.to_excel("allCarStats_11kW.xlsx")
# End 
print('made it till the end daaaahmn')