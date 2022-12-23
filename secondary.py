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

import os, sys
import numpy as np
import pandas as pd
from influxdb_swipe import DataSwipe
import math
import itertools
from matplotlib import pyplot as plt
from config_path import Config
from datetime import datetime
from pytz import timezone


""""
========================== initialize dataSwipe   =========================
"""
Config.initializePath()
folder = "simOutput/"
figures_dir = os.path.join('Figures/',folder)

timeBase = 60*15 	#quarters
timeZone = timezone('Europe/Amsterdam')
startTime = int(timeZone.localize(datetime(2022, 1, 1)).timestamp())	#YYYY, MM, DD
endTime = int(timeZone.localize(datetime(2022, 1, 15)).timestamp())	    #YYYY, MM, DD

outputData = DataSwipe(startTime, endTime, timeBase, url = "http://localhost:8086/query", dbname = "dem")

#If False, expect sim has been run, swiped, evaluated and saved to Excel before. Then just recovers those results.
swipe = False

# for paramtersweep, instantiate percentiles you wanna check.
steps = 21
percentiles = np.arange(steps)
stepsize = math.ceil(100/percentiles[-1])
percentiles = percentiles*stepsize
# run once sweep=False and once sweep=True to generate all outputs
# cannot have swipe = False and sweep + True since the amount of data is never saved in Excel. Swipe results have to be rerun.
sweep = False
sweepCase = 'C7_'
#excluded cases 1-3 from paper. Did rename the existing cases 4-8 to 1-5. Did not adapt to that in DEMKit simulation. So just bookkeeping here.
sweepName = 'C4'

#read carStats from before the DEMKit simulations to append with ENS per case.
#filteredData is appended by carStats in main.py. CarStats' order differs from the DEMkit input. Using filteredData instead.
filteredData = pd.read_excel("filteredDataVoi.xlsx",index_col=0)

print('Done Initialization')
print("===================================================================================================================================")

#NOTE basecase has to be the first entry, or already have some entries in carStats
extendedCases = ["C1_realized_", "C2_realized_", "C4_realized_", "C5_realized_", "C6_realized_", ]
cases = ["C0_", "C1_","C2_", "C3_","C4_", "C5_","C6_", "C7_","C8_"]
baseCase = "C0_"
evs = [*range(0,len(pd.unique(filteredData['card_id'])),1)]

idxList = []

if swipe == True:
    """"
    ==========================  example: swipe data from influxDB  =========================
    """

    field = 		"W-power.real.c.ELECTRICITY"	        # Field you'd like to read
    measurement = 	"C1_devices"						    # Either: devices, controllers, host, flows
    condition = 	"\"name\" = 'ElectricVehicle-0'"		# Name of the element you want to have the data of

    outputData.dataSwipe(field,measurement,condition,'mean', True)

    #save sweeped data to dataframe
    #outputData.dataAppend('C1_W-power.real.c.ELECTRICITY_ElectricVehicle-0')

    """"
    ==========================  swipe EV/power data from influxDB and get measures =========================
    """

    #per EV and scenario, sweep data
    for case in cases:
        if case + "realized_" in extendedCases:
            for ev in evs:
                #realization of charging power
                #since we have two cases (Cx_ and Cx_realized_), we read the real data from the Cx_realized_ simulation
                field = "W-power.real.c.ELECTRICITY"
                measurement = "{}devices".format(case + "realized_")
                condition = "\"name\" = 'ElectricVehicle-{}'".format(ev)
                outputData.dataSwipe(field, measurement, condition, 'mean', True)
                outputData.dataAppend('{}{}_ElectricVehicle-{}'.format(case, field, ev))
            
                #offline planning of charging power based on estimations
                #since we have two cases (Cx_ and Cx_realized_), we read the real data from the Cx_device instead of the plan from the Cx_controller
                field = "W-power.real.c.ELECTRICITY"
                measurement = "{}devices".format(case)
                condition = "\"name\" = 'ElectricVehicle-{}'".format(ev)
                outputData.dataSwipe(field, measurement, condition, 'mean', True)
                outputData.dataAppend('{}{}_ElectricVehicle-{}'.format(case, "W-power.plan.real.c.ELECTRICITY", ev),True)
            #calculate the energy served for all cars in current case. Absolute and Relative
            filteredData = outputData.dataEnergy(filteredData,case)
            #calculate the energy NOT served for all cars in current case in offline planning and realization, compared to baseline (e.g. greedy). Both absolute and relative ENS
            filteredData = outputData.dataENS(filteredData,case,baseCase)
            #swipe aggregated BTS power planned and realized
            outputData.dataPowerAgg(case,case + "realized_")

        elif case == baseCase:
            for ev in evs:
                #realization of charging power
                field = "W-power.real.c.ELECTRICITY"
                measurement = "{}devices".format(case)
                condition = "\"name\" = 'ElectricVehicle-{}'".format(ev)
                outputData.dataSwipe(field, measurement, condition, 'mean', True)
                outputData.dataAppend('{}{}_ElectricVehicle-{}'.format(case, field, ev))

                #offline planning of charging power based on estimations
                outputData.dataAppend('{}{}_ElectricVehicle-{}'.format(case, "W-power.plan.real.c.ELECTRICITY", ev),True)        
            filteredData = outputData.dataEnergy(filteredData,case)
            #calculate the energy NOT served for all cars in current case in offline planning and realization, compared to baseline (perfect information). Both absolute and relative ENS
            filteredData = outputData.dataENS(filteredData,case,baseCase)    
            #swipe aggregated BTS power planned and realized
            outputData.dataPowerAgg(case, case)

        else:
            for ev in evs:
                #realization of charging power
                field = "W-power.real.c.ELECTRICITY"
                measurement = "{}devices".format(case)
                condition = "\"name\" = 'ElectricVehicle-{}'".format(ev)
                outputData.dataSwipe(field, measurement, condition, 'mean', True)
                outputData.dataAppend('{}{}_ElectricVehicle-{}'.format(case, field, ev))

                #offline planning of charging power based on estimations
                field = "W-power.plan.real.c.ELECTRICITY"
                measurement = "{}controllers".format(case)
                condition = "\"name\" = 'ElectricVehicleController{}'".format(ev)
                outputData.dataSwipe(field, measurement, condition, 'mean', True)
                outputData.dataAppend('{}{}_ElectricVehicle-{}'.format(case, field, ev),True)
            #calculate the energy served for all cars in current case. Absolute and Relative
            filteredData = outputData.dataEnergy(filteredData,case)
            #calculate the energy NOT served for all cars in current case in offline planning and realization, compared to baseline (perfect information). Both absolute and relative ENS
            filteredData = outputData.dataENS(filteredData,case,baseCase)
            #swipe aggregated BTS power planned and realized
            outputData.dataPowerAgg(case)

    #get data for sweep case:
    if sweep==True:
        for aspect in itertools.product(percentiles,percentiles):
            case = sweepCase + 't' + str(aspect[0]) + 'e' + str(aspect[1])
            idxList = idxList +[case]
            print(case)
            for ev in evs:
                #realization of charging power
                field = "W-power.real.c.ELECTRICITY"
                measurement = "{}devices".format(case)
                condition = "\"name\" = 'ElectricVehicle-{}'".format(ev)
                outputData.dataSwipe(field, measurement, condition, 'mean', True)
                outputData.dataAppend('{}{}_ElectricVehicle-{}'.format(case, field, ev))

                #offline planning of charging power based on estimations
                field = "W-power.plan.real.c.ELECTRICITY"
                measurement = "{}controllers".format(case)
                condition = "\"name\" = 'ElectricVehicleController{}'".format(ev)
                outputData.dataSwipe(field, measurement, condition, 'mean', True)
                outputData.dataAppend('{}{}_ElectricVehicle-{}'.format(case, field, ev),True)
            #calculate the energy served for all cars in current case. Absolute and Relative
            filteredData = outputData.dataEnergy(filteredData,case)
            #calculate the energy NOT served for all cars in current case in offline planning and realization, compared to baseline (perfect information). Both absolute and relative ENS
            filteredData = outputData.dataENS(filteredData,case,baseCase)
            #swipe aggregated BTS power planned and realized
            outputData.dataPowerAgg(case)

    print("Done Data Sweep InfluxDB!")
    print("===================================================================================================================================")

    #manually checked agg realize power sweep for C0 = sum of individual realized car powers for C0
    outputData.dataGlobalMeasures(filteredData, cases + idxList, baseCase)

    print("Done Global Measures!")
    print("===================================================================================================================================")

    #save to Excel
    outputData.dataSaveExcel('resultExport',write=not sweep)
    if not sweep:
        print("Done Saving to Excel!")
        print("===================================================================================================================================")
    else:
        print("NOT saved to Excel!")
        print("===================================================================================================================================")
#read from file where results were saved in first simulation run. Only to reproduce graphs on same machine.
else:
    outputData.dataRead('resultExport')
    outputData.df = outputData.outputDataDf
    #per EV and scenario, get ENS values
    for case in cases:
        for ev in evs:
            #calculate the energy served for all cars in current case. Absolute and Relative
            filteredData = outputData.dataEnergy(filteredData,case)
            #calculate the energy NOT served for all cars in current case in offline planning and realization, compared to baseline (perfect information). Both absolute and relative ENS
            filteredData = outputData.dataENS(filteredData,case,baseCase)

scaling = 1000
scalingRelative = 1/100

#frame limits for ENS and power plots:
maxX = 96
minX = 0
maxY = 400000/scaling
minY = 0

#We decided to not include cases with realized start times in paper. Naming in paper changed. See also READMEcases.txt
coreCases = ['C4_', 'C5_', 'C6_', 'C7_', 'C8_']
coreNames = ['C1', 'C2', 'C3', 'C4', 'C5']
baseName = 'C0'

evs = [*range(0,len(filteredData.index),1)]
cs = pd.DataFrame.from_dict(filteredData)

if swipe:
    df = pd.DataFrame.from_dict(outputData.df)
else: 
    df = outputData.outputDataDf

def makeEnsLDC(data, divisor):
    ENSy = data.sort_values()
    ENSy = ENSy/divisor
    ENSy.reset_index(drop=True, inplace=True)
    return ENSy

'''============================================= illustrative example single EV ========================================================================='''
#For VoI experiments, observed the following EV cases that are interesting to elaborate in illustrative example:
#ENS values in Wh, based on simulation results from November 14th, 2022, comparing C7 to C0.
#52 planning is very dominant in morning, little charging in afternoon. Realized availability is opposite - About 85% ENS (8917.3/10090)
#48 estimated and real availability disjoint - ENS = 4666/4666
#64 premature charging stop due to battery being full (energy requirement overestimated) - ENS = 0
#41 realized availability earlier and later than estimated, still ENS stince follows profile - ENS = 5893/18980
#59 flat example of how at most follows until estimated departure time - ENS = 16650/31450
ExEV = [59,64,48,41,52]
for EV in ExEV:
    plt.figure("C7 aggregated power profile single EV")
    ax = plt.gca()
    ax.set_xlim([minX, maxX])
    ax.set_ylim([minY, 8])
    plt.plot(df.index.values.tolist(),df["{}W-power.plan.real.c.ELECTRICITY_ElectricVehicle-{}".format("C7_",EV)]/scaling,label = "plan EV" +str(EV), linewidth=2.5)
    plt.plot(df.index.values.tolist(),df["{}W-power.real.c.ELECTRICITY_ElectricVehicle-{}".format("C7_",EV)]/scaling,label = "real EV" + str(EV), linewidth=2.5,ls = '--')
    temp = filteredData[filteredData["card_id"]==filteredData.iloc[EV]["card_id"]].iloc[0]
    plt.axvspan(temp["start_time_mean"]/timeBase, temp["end_smean_dmean"]/timeBase, facecolor='C0',label = "plan availability EV" + str(EV), alpha = 0.2)
    plt.axvspan(temp["start_secondsSinceStart"]/timeBase, temp["end_secondsSinceStart"]/timeBase, facecolor='C1',label = "real availability EV"+str(EV), alpha = 0.2)
    plt.xlabel('Time [15m since simulation start]')
    plt.ylabel('Power [kW]')
    plt.legend(loc = 1)
    plt.grid()
    plt.savefig(figures_dir +"PowerAggIllustrationEV{}_.pdf".format(EV))
    plt.savefig(figures_dir +"PowerAggIllustrationEV{}_.jpeg".format(EV))
    plt.close()
'''============================================== parameter sweep figures ================================================================================='''

if sweep==True:
    labels = [str(s) + "-percentile" for s in percentiles]
    energyIdx = np.hstack([np.arange(steps)]*steps)
    timeIdx = np.empty([steps*steps])
    for i in range(0,steps):
        timeIdx[steps*i:steps*(i+1)] = i

    plt.figure("Energy sweep aggregated power profile")
    ax = plt.gca()
    ax.set_xlim([minX, maxX])
    ax.set_ylim([minY, maxY-100])
    plt.plot(df.index.values.tolist(),df["{}W-power.real.c.ELECTRICITY_BufferTimeshiftable".format('C0_')]/scaling,label = "C0_real")
    plt.plot(df.index.values.tolist(),df["{}W-power.real.c.ELECTRICITY_BufferTimeshiftable".format('C7_t50e40')]/scaling,label = "{}real".format('C7_t50e40'))
    plt.plot(df.index.values.tolist(),df["{}W-power.real.c.ELECTRICITY_BufferTimeshiftable".format('C7_t50e45')]/scaling,label = "{}real".format('C7_t50e45'))
    plt.plot(df.index.values.tolist(),df["{}W-power.real.c.ELECTRICITY_BufferTimeshiftable".format('C7_t50e50')]/scaling,label = "{}real".format('C7_t50e50'))
    plt.plot(df.index.values.tolist(),df["{}W-power.real.c.ELECTRICITY_BufferTimeshiftable".format('C7_t50e55')]/scaling,label = "{}real".format('C7_t50e55'))
    plt.plot(df.index.values.tolist(),df["{}W-power.real.c.ELECTRICITY_BufferTimeshiftable".format('C7_t50e60')]/scaling,label = "{}real".format('C7_t50e60'))
    plt.plot(df.index.values.tolist(),df["{}W-power.real.c.ELECTRICITY_BufferTimeshiftable".format('C8_')]/scaling,label = "{}real".format('C8_'))
    plt.xlabel('time [15m since simulation start]')
    plt.ylabel('aggregated power [kW]')
    plt.legend()
    plt.grid()
    plt.savefig(figures_dir +"PowerAggSweepE.pdf")
    plt.close()     

    plt.figure("Time sweep aggregated power profile")
    ax = plt.gca()
    ax.set_xlim([minX, maxX])
    ax.set_ylim([minY, maxY-100])
    plt.plot(df.index.values.tolist(),df["{}W-power.real.c.ELECTRICITY_BufferTimeshiftable".format('C0_')]/scaling,label = "C0_real")
    plt.plot(df.index.values.tolist(),df["{}W-power.real.c.ELECTRICITY_BufferTimeshiftable".format('C7_t40e50')]/scaling,label = "{}real".format('C7_t40e50'))
    plt.plot(df.index.values.tolist(),df["{}W-power.real.c.ELECTRICITY_BufferTimeshiftable".format('C7_t45e50')]/scaling,label = "{}real".format('C7_t45e50'))
    plt.plot(df.index.values.tolist(),df["{}W-power.real.c.ELECTRICITY_BufferTimeshiftable".format('C7_t50e50')]/scaling,label = "{}real".format('C7_t50e50'))
    plt.plot(df.index.values.tolist(),df["{}W-power.real.c.ELECTRICITY_BufferTimeshiftable".format('C7_t55e50')]/scaling,label = "{}real".format('C7_t55e50'))
    plt.plot(df.index.values.tolist(),df["{}W-power.real.c.ELECTRICITY_BufferTimeshiftable".format('C7_t60e50')]/scaling,label = "{}real".format('C7_t60e50'))
    plt.plot(df.index.values.tolist(),df["{}W-power.real.c.ELECTRICITY_BufferTimeshiftable".format('C8_')]/scaling,label = "{}real".format('C8_'))
    plt.xlabel('time [15m since simulation start]')
    plt.ylabel('aggregated power [kW]')
    plt.legend()
    plt.grid()
    plt.savefig(figures_dir +"PowerAggSweepT.pdf")
    plt.close()

    plt.figure("Time sweep aggregated power profile")
    ax = plt.gca()
    ax.set_xlim([minX, maxX])
    ax.set_ylim([minY, maxY-100])
    plt.plot(df.index.values.tolist(),df["{}W-power.real.c.ELECTRICITY_BufferTimeshiftable".format('C0_')]/scaling,label = "C0_real")
    plt.plot(df.index.values.tolist(),df["{}W-power.real.c.ELECTRICITY_BufferTimeshiftable".format('C7_t40e40')]/scaling,label = "{}real".format('C7_t40e40'))
    plt.plot(df.index.values.tolist(),df["{}W-power.real.c.ELECTRICITY_BufferTimeshiftable".format('C7_t45e45')]/scaling,label = "{}real".format('C7_t45e45'))
    plt.plot(df.index.values.tolist(),df["{}W-power.real.c.ELECTRICITY_BufferTimeshiftable".format('C7_t50e50')]/scaling,label = "{}real".format('C7_t50e50'))
    plt.plot(df.index.values.tolist(),df["{}W-power.real.c.ELECTRICITY_BufferTimeshiftable".format('C7_t55e55')]/scaling,label = "{}real".format('C7_t55e55'))
    plt.plot(df.index.values.tolist(),df["{}W-power.real.c.ELECTRICITY_BufferTimeshiftable".format('C7_t60e60')]/scaling,label = "{}real".format('C7_t60e60'))
    plt.plot(df.index.values.tolist(),df["{}W-power.real.c.ELECTRICITY_BufferTimeshiftable".format('C8_')]/scaling,label = "{}real".format('C8_'))
    plt.xlabel('time [15m since simulation start]')
    plt.ylabel('aggregated power [kW]')
    plt.legend()
    plt.grid()
    plt.savefig(figures_dir +"PowerAggSweepTE.pdf")
    plt.close()       

    plt.figure("Energy sweep aggregated power profile")
    ax = plt.gca()
    ax.set_xlim([minX, maxX])
    ax.set_ylim([minY, maxY-100])
    plt.plot(df.index.values.tolist(),df["{}W-power.real.c.ELECTRICITY_BufferTimeshiftable".format('C0_')]/scaling,label = "C0_real")
    plt.plot(df.index.values.tolist(),df["{}W-power.real.c.ELECTRICITY_BufferTimeshiftable".format('C7_t50e30')]/scaling,label = "{}real".format('C7_t50e30'))
    plt.plot(df.index.values.tolist(),df["{}W-power.real.c.ELECTRICITY_BufferTimeshiftable".format('C7_t50e40')]/scaling,label = "{}real".format('C7_t50e40'))
    plt.plot(df.index.values.tolist(),df["{}W-power.real.c.ELECTRICITY_BufferTimeshiftable".format('C7_t50e50')]/scaling,label = "{}real".format('C7_t50e50'))
    plt.plot(df.index.values.tolist(),df["{}W-power.real.c.ELECTRICITY_BufferTimeshiftable".format('C7_t50e60')]/scaling,label = "{}real".format('C7_t50e60'))
    plt.plot(df.index.values.tolist(),df["{}W-power.real.c.ELECTRICITY_BufferTimeshiftable".format('C7_t50e70')]/scaling,label = "{}real".format('C7_t50e70'))
    plt.plot(df.index.values.tolist(),df["{}W-power.real.c.ELECTRICITY_BufferTimeshiftable".format('C8_')]/scaling,label = "{}real".format('C8_'))
    plt.xlabel('time [15m since simulation start]')
    plt.ylabel('aggregated power [kW]')
    plt.legend()
    plt.grid()
    plt.savefig(figures_dir +"PowerAggSweepBigStepE.pdf")
    plt.close()     

    plt.figure("Time sweep aggregated power profile")
    ax = plt.gca()
    ax.set_xlim([minX, maxX])
    ax.set_ylim([minY, maxY-100])
    plt.plot(df.index.values.tolist(),df["{}W-power.real.c.ELECTRICITY_BufferTimeshiftable".format('C0_')]/scaling,label = "C0_real")
    plt.plot(df.index.values.tolist(),df["{}W-power.real.c.ELECTRICITY_BufferTimeshiftable".format('C7_t30e50')]/scaling,label = "{}real".format('C7_t30e50'))
    plt.plot(df.index.values.tolist(),df["{}W-power.real.c.ELECTRICITY_BufferTimeshiftable".format('C7_t40e50')]/scaling,label = "{}real".format('C7_t40e50'))
    plt.plot(df.index.values.tolist(),df["{}W-power.real.c.ELECTRICITY_BufferTimeshiftable".format('C7_t50e50')]/scaling,label = "{}real".format('C7_t50e50'))
    plt.plot(df.index.values.tolist(),df["{}W-power.real.c.ELECTRICITY_BufferTimeshiftable".format('C7_t60e50')]/scaling,label = "{}real".format('C7_t60e50'))
    plt.plot(df.index.values.tolist(),df["{}W-power.real.c.ELECTRICITY_BufferTimeshiftable".format('C7_t70e50')]/scaling,label = "{}real".format('C7_t70e50'))
    plt.plot(df.index.values.tolist(),df["{}W-power.real.c.ELECTRICITY_BufferTimeshiftable".format('C8_')]/scaling,label = "{}real".format('C8_'))
    plt.xlabel('time [15m since simulation start]')
    plt.ylabel('aggregated power [kW]')
    plt.legend()
    plt.grid()
    plt.savefig(figures_dir +"PowerAggSweepBigStepT.pdf")
    plt.close()

    plt.figure("Time sweep aggregated power profile")
    ax = plt.gca()
    ax.set_xlim([minX, maxX])
    ax.set_ylim([minY, maxY-100])
    plt.plot(df.index.values.tolist(),df["{}W-power.real.c.ELECTRICITY_BufferTimeshiftable".format('C0_')]/scaling,label = "C0_real")
    plt.plot(df.index.values.tolist(),df["{}W-power.real.c.ELECTRICITY_BufferTimeshiftable".format('C7_t30e30')]/scaling,label = "{}real".format('C7_t30e30'))
    plt.plot(df.index.values.tolist(),df["{}W-power.real.c.ELECTRICITY_BufferTimeshiftable".format('C7_t40e40')]/scaling,label = "{}real".format('C7_t40e40'))
    plt.plot(df.index.values.tolist(),df["{}W-power.real.c.ELECTRICITY_BufferTimeshiftable".format('C7_t50e50')]/scaling,label = "{}real".format('C7_t50e50'))
    plt.plot(df.index.values.tolist(),df["{}W-power.real.c.ELECTRICITY_BufferTimeshiftable".format('C7_t60e60')]/scaling,label = "{}real".format('C7_t60e60'))
    plt.plot(df.index.values.tolist(),df["{}W-power.real.c.ELECTRICITY_BufferTimeshiftable".format('C7_t70e70')]/scaling,label = "{}real".format('C7_t70e70'))
    plt.plot(df.index.values.tolist(),df["{}W-power.real.c.ELECTRICITY_BufferTimeshiftable".format('C8_')]/scaling,label = "{}real".format('C8_'))
    plt.xlabel('time [15m since simulation start]')
    plt.ylabel('aggregated power [kW]')
    plt.legend()
    plt.grid()
    plt.savefig(figures_dir +"PowerAggSweepBigStepTE.pdf")
    plt.close()       


    for aspect in itertools.product(["powerPeakReal", "powerPeakPlan"], ["ENSrealRelative_average", "ENSrealRelative_max", "ENSplanRelative_average", "ENSplanRelative_max"]):
        powerArray = []
        ENSArray = []
        for case in idxList:
            powerArray = powerArray + [outputData.outputDataDfAgg[aspect[0]].loc[case]/scaling]
            ENSArray = ENSArray + [outputData.outputDataDfAgg[aspect[1]].loc[case]/scalingRelative]

        plt.figure("scatter plot {} {}, energyIdx".format(aspect[0], aspect[1]))
        scatter = plt.scatter(powerArray, ENSArray, c=energyIdx)
        reference = plt.plot(outputData.outputDataDfAgg[aspect[0]].loc[sweepCase]/scaling, outputData.outputDataDfAgg[aspect[1]].loc[sweepCase]/scalingRelative, marker = "o", markersize = 9, markeredgecolor="red", markerfacecolor = "red")
        plt.xlabel("Aggregated power peak" + " [kW]")
        if aspect[1] in ["ENSrealRelative_average", "ENSplanRelative_average"]:
            plt.ylabel("Average energy not served" + " [%]")
        if aspect[1] in ["ENSrealRelative_max", "ENSplanRelative_max"]:
            plt.ylabel("Maximal energy not served" + " [%]")
        plt.legend(handles = [scatter.legend_elements()[0][0]] + [scatter.legend_elements()[0][-1]] + reference, labels = [labels[0]] + [labels[-1]] + [sweepName], loc = 1)
        plt.grid()
        plt.savefig(figures_dir +"scatter_{}_{}_E.pdf".format(aspect[0],aspect[1]))
        plt.close()

        plt.figure("scatter plot {} {}, timeIdx".format(aspect[0], aspect[1]))
        scatter = plt.scatter(powerArray, ENSArray, c=timeIdx)
        reference = plt.plot(outputData.outputDataDfAgg[aspect[0]].loc[sweepCase]/scaling, outputData.outputDataDfAgg[aspect[1]].loc[sweepCase]/scalingRelative, marker = "o", markersize = 9, markeredgecolor="red", markerfacecolor = "red")
        plt.xlabel("Aggregated power peak" + " [kW]")
        if aspect[1] in ["ENSrealRelative_average", "ENSplanRelative_average"]:
            plt.ylabel("Average energy not served" + " [%]")
        if aspect[1] in ["ENSrealRelative_max", "ENSplanRelative_max"]:
            plt.ylabel("Maximal energy not served" + " [%]")
        plt.legend(handles = [scatter.legend_elements()[0][0]] + [scatter.legend_elements()[0][-1]] + reference, labels = [labels[0]] + [labels[-1]] + [sweepName], loc = 1)
        plt.grid()
        plt.savefig(figures_dir +"scatter_{}_{}_T.pdf".format(aspect[0],aspect[1]))
        plt.close()

'''============================================== ENS figures ================================================================================='''
#rearrange colors for better visibility, and make two lines dashed.
color = ['#1f77b4', '#d62728', '#ff7f0e', '#2ca02c', '#9467bd']

measures = ["ENSrealRelative", "ENSplanRelative", "ENScaseInternalRelative"]
for measure in measures:
    plt.figure(measure)
    plt.plot(makeEnsLDC(cs['{}{}'.format(coreCases[0],measure)], divisor = 1/100), label = coreNames[0], linewidth=2.5, color = color[0])
    plt.plot(makeEnsLDC(cs['{}{}'.format(coreCases[1],measure)], divisor = 1/100), label = coreNames[1], linewidth=2.5, color = color[1])
    plt.plot(makeEnsLDC(cs['{}{}'.format(coreCases[2],measure)], divisor = 1/100), label = coreNames[2], linewidth=2.5, color = color[2], ls = '--')
    plt.plot(makeEnsLDC(cs['{}{}'.format(coreCases[3],measure)], divisor = 1/100), label = coreNames[3], linewidth=2.5, color = color[3], ls = '--')
    plt.plot(makeEnsLDC(cs['{}{}'.format(coreCases[4],measure)], divisor = 1/100), label = coreNames[4], linewidth=2.5, color = color[4])
    plt.xlabel("individual EVs [-]")
    plt.ylabel("{} [%]".format("Energy not served"))
    plt.ylim(-2,101)
    plt.legend(loc = 1)
    plt.grid()
    plt.savefig(figures_dir + measure + ".pdf")
    plt.close()

#measures = ["ENSrealRelative", "ENSplanRelative", "ENScaseInternalRelative"]
#for measure in measures:
#    plt.figure(measure)
#    for case in range(0,len(coreCases)):
#        plt.plot(makeEnsLDC(cs['{}{}'.format(coreCases[case],measure)], divisor = 1/100), label = coreNames[case], linewidth=2.5, color = color[case])
#    plt.xlabel("individual EVs [-]")
#    plt.ylabel("{} [%]".format("Energy not served"))
#    plt.ylim(-1,101)
#    plt.legend(loc = 1)
#    plt.grid()
#    plt.show()
##    plt.savefig(figures_dir + measure + ".pdf")
#    plt.close()

measures = ["ENSrealAbs", "ENSplanAbs", "ENScaseInternalAbs"]
for measure in measures:
    plt.figure(measure)
    plt.plot(makeEnsLDC(cs['{}{}'.format(coreCases[0],measure)], divisor = scaling), label = coreNames[0], linewidth=2.5, color = color[0])
    plt.plot(makeEnsLDC(cs['{}{}'.format(coreCases[1],measure)], divisor = scaling), label = coreNames[1], linewidth=2.5, color = color[1])
    plt.plot(makeEnsLDC(cs['{}{}'.format(coreCases[2],measure)], divisor = scaling), label = coreNames[2], linewidth=2.5, color = color[2], ls = '--')
    plt.plot(makeEnsLDC(cs['{}{}'.format(coreCases[3],measure)], divisor = scaling), label = coreNames[3], linewidth=2.5, color = color[3], ls = '--')
    plt.plot(makeEnsLDC(cs['{}{}'.format(coreCases[4],measure)], divisor = scaling), label = coreNames[4], linewidth=2.5, color = color[4])
    #for case in range(0,len(coreCases)):
    #   plt.plot(makeEnsLDC(cs['{}{}'.format(coreCases[case],measure)], divisor = scaling), label = coreNames[case], linewidth=2.5,color=color[case])
    plt.xlabel("Individual EVs [-]")
    plt.ylabel("{} [kW]".format("Energy not served"))
    plt.ylim(-2,61)
    plt.legend(loc = 1)
    plt.grid()
    plt.savefig(figures_dir + measure + ".pdf")
    plt.close()

'''============================================== power profiles figures ================================================================================='''

plt.figure("C0_ aggregated power profile")
ax = plt.gca()
ax.set_xlim([minX, maxX])
ax.set_ylim([minY, maxY])
df = pd.DataFrame.from_dict(outputData.df)
plt.plot(df.index.values.tolist(),df["{}W-power.real.c.ELECTRICITY_BufferTimeshiftable".format("C0_")]/scaling,label = baseName + " baseline", linewidth=2.5)
plt.xlabel('Time [15m since simulation start]')
plt.ylabel('Aggregated power [kW]')
plt.legend(loc = 1)
plt.grid()
plt.savefig(figures_dir +"PowerAggC0_.pdf")
plt.close()

#Blog post figure November 2022
plt.figure("Blog aggregated power profile")
ax = plt.gca()
ax.set_xlim([minX, maxX])
ax.set_ylim([minY, maxY])
df = pd.DataFrame.from_dict(outputData.df)
plt.plot(df.index.values.tolist(),df["{}W-power.real.c.ELECTRICITY_BufferTimeshiftable".format("C0_")]/scaling,label = "Uncontrolled charging", linewidth=2.5)
plt.plot(df.index.values.tolist(),df["{}W-power.real.c.ELECTRICITY_BufferTimeshiftable".format("C7_")]/scaling,label = "Historical information smart charging", linewidth=2.5)
plt.plot(df.index.values.tolist(),df["{}W-power.real.c.ELECTRICITY_BufferTimeshiftable".format("C8_")]/scaling,label = "Perfect information smart charging", linewidth=2.5)
plt.xlabel('Time [15m since simulation start]')
plt.ylabel('Aggregated power [kW]')
plt.legend(loc = 1)
plt.grid()
plt.savefig(figures_dir +"PowerAggC0C7C8.pdf")
plt.close()

for case in range(0,len(coreCases)):
    plt.figure("{} aggregated power profile".format(coreCases[case]))
    ax = plt.gca()
    ax.set_xlim([minX, maxX])
    ax.set_ylim([minY, maxY])
    df = pd.DataFrame.from_dict(outputData.df)
    plt.plot(df.index.values.tolist(),df["{}W-power.plan.real.c.ELECTRICITY_BufferTimeshiftable".format(coreCases[case])]/scaling,label = "{} plan".format(coreNames[case]), linewidth=2.5)
    plt.plot(df.index.values.tolist(),df["{}W-power.real.c.ELECTRICITY_BufferTimeshiftable".format(coreCases[case])]/scaling,label = "{} real".format(coreNames[case]), linewidth=2.5)
    plt.xlabel('Time [15m since simulation start]')
    plt.ylabel('Aggregated power [kW]')
    plt.legend(loc = 1)
    plt.grid()
    plt.savefig(figures_dir +"PowerAgg{}.pdf".format(coreCases[case]))
    plt.close()

plt.figure("aggregated power profiles all cases")
ax = plt.gca()
divisor = 1000
ax.set_xlim([minX, maxX])
ax.set_ylim([minY, maxY])
df = pd.DataFrame.from_dict(outputData.df)
plt.plot(df.index.values.tolist(),df["{}W-power.real.c.ELECTRICITY_BufferTimeshiftable".format("C0_")]/scaling,label = "C0_real", linewidth=2.5)
for case in range(0,len(coreCases)):
    plt.plot(df.index.values.tolist(),df["{}W-power.plan.real.c.ELECTRICITY_BufferTimeshiftable".format(coreCases[case])]/scaling,label = "{} plan".format(coreNames[case]), linewidth=2.5)
    plt.plot(df.index.values.tolist(),df["{}W-power.real.c.ELECTRICITY_BufferTimeshiftable".format(coreCases[case])]/scaling,label = "{} real".format(coreNames[case]), linewidth=2.5)
plt.xlabel('time [15m since simulation start]')
plt.ylabel('aggregated power [kW]')
plt.legend(loc = 1)
plt.grid()
plt.savefig(figures_dir +"powerAggALL.pdf")
plt.close()

plt.figure("Blog: aggregated power profile")
ax = plt.gca()
ax.set_xlim([minX, maxX])
ax.set_ylim([minY, maxY])
df = pd.DataFrame.from_dict(outputData.df)
plt.plot(df.index.values.tolist(),df["{}W-power.real.c.ELECTRICITY_BufferTimeshiftable".format("C0_")]/scaling,label = "uncontrolled charging")
plt.plot(df.index.values.tolist(),df["{}W-power.real.c.ELECTRICITY_BufferTimeshiftable".format("C7_")]/scaling,label = "historical information smart charging")
plt.plot(df.index.values.tolist(),df["{}W-power.real.c.ELECTRICITY_BufferTimeshiftable".format("C8_")]/scaling,label = "perfect information smart charging")
plt.xlabel('time [15min]')
plt.ylabel('aggregated power [kW]')
plt.legend()
plt.grid()
plt.savefig(figures_dir +"PowerAggBlog.pdf")
plt.close()

# End 
print("Done Saving Figures!")
print("===================================================================================================================================")
print("Whoop whoop all compiled")
