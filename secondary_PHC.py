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

import os, sys
import matplotlib
import numpy as np
import pandas as pd
from influxdb_swipe import DataSwipe
import math
import itertools
from matplotlib import pyplot as plt
from config_path import Config
from datetime import datetime
from pytz import timezone
import imageio


""""
========================== initialize dataSwipe   =========================
"""
Config.initializePath()
folder = "simOutput/"
figures_dir = os.path.join('Figures/',folder)

timeBase = 60*15 	#quarters
timeZone = timezone('Europe/Amsterdam')
startTime = int(timeZone.localize(datetime(2022, 1, 1)).timestamp())	#YYYY, MM, DD
endTime = int(timeZone.localize(datetime(2022, 1, 3)).timestamp())	#YYYY, MM, DD

outputData = DataSwipe(startTime, endTime, timeBase, url = "http://localhost:8086/query", dbname = "dem")
'''
#current cases.
#PHC_0_ = greedy. energy = real
#PHC_1_ = PS. energy = commute*efficiency
#PHC_2_ = PS. energy = commute*defaultEfficiency (199 Wh/km)
#PHC_x_ = greedy. energy = commute*efficiency
#PHC_y_ = greedy. energy = min(real,commute*efficiency) --> in PHC context chosen as baseline
#PHC_sweep_100uncontrolled = if no control. energy = real, EVs = same as 100% controlled case.
#PHC_sweep_100greedyguarantee = if greedily charge guarantee. energy = commute*defaultEfficiency, EVs = same as 100% controlled case.
#PHC_sweep_{n} = hybrid. energy = commute*defaultEfficiency or energy_real based on veto_hr criterium. n = percentage smart charging.
#PHC_sweep_{n}_1min_ = same as #PHC_sweep_{n}, but control and plannings timebase = 60 seconds. To check bunny plot behaviour - update 29.3.23 bunny fixed, bug in tsCtrl
'''
#read sampleData from before the DEMKit simulations to append with ENS per case.
sampleData = pd.read_excel("sampleData_11kW.xlsx",index_col=0)

#for faster running, if True swipe and plot data per EV, else not.
perEV = False

print('Done Initialization')
print("===================================================================================================================================")

""""
========================== swipe data from influxDB PHC guarantee simulation   =========================
"""
cases = ['PHC_sweep_100greedy_11kW', 'PHC_sweep_100uncontrolled_11kW']

for case in cases:
    field = 		"W-power.real.c.ELECTRICITY"	# Field you'd like to read
    measurement = 	"{}devices".format(case)						# Either: devices, controllers, host, flows
    condition = 	"\"devtype\" = 'BufferTimeshiftable'"		# Name of the element you want to have the data of

    outputData.dataSwipe(field,measurement,condition,'sum', True)

    #save sweeped data to dataframe
    outputData.dataAppend(case + 'W-power.real.c.ELECTRICITY_BufferTimeshiftable')

    field = 		"W-power.plan.real.c.ELECTRICITY"	# Field you'd like to read
    measurement = 	"{}controllers".format(case)						# Either: devices, controllers, host, flows
    condition = 	"\"ctrltype\" = 'BufferTimeshiftableController'"		# Name of the element you want to have the data of

    outputData.dataSwipe(field,measurement,condition,'sum', True)

    #save sweeped data to dataframe
    outputData.dataAppend(case + 'W-power.plan.real.c.ELECTRICITY_BufferTimeshiftable')

""""
==========================  start influxSweep for PHC sweep  =========================
"""

rng = range(0,101)
for i in rng:
    case = 'PHC_sweep_perfect_EC_11kW_'+str(i)

    field = 		"W-power.real.c.ELECTRICITY"	# Field you'd like to read
    measurement = 	"{}devices".format(case)						# Either: devices, controllers, host, flows
    condition = 	"\"devtype\" = 'BufferTimeshiftable'"		# Name of the element you want to have the data of

    outputData.dataSwipe(field,measurement,condition,'sum', True)

    #save sweeped data to dataframe
    outputData.dataAppend(case + 'W-power.real.c.ELECTRICITY_BufferTimeshiftable')

    field = 		"W-power.plan.real.c.ELECTRICITY"	# Field you'd like to read
    measurement = 	"{}controllers".format(case)						# Either: devices, controllers, host, flows
    condition = 	"\"ctrltype\" = 'BufferTimeshiftableController'"		# Name of the element you want to have the data of

    outputData.dataSwipe(field,measurement,condition,'sum', True)

    #save sweeped data to dataframe
    outputData.dataAppend(case + 'W-power.plan.real.c.ELECTRICITY_BufferTimeshiftable')

    if perEV:
        evs = list(range(0,400))
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

    print("Done Influx Sweep iteration {}!".format(i))
    print("===================================================================================================================================")

    """"
    ==========================  start plotting  =========================
    """

    scaling = 1000
    scalingRelative = 1/100

    #frame limits for aggregated plots:
    maxX = 96
    minX = 0
    maxY = 2300000/scaling
    minY = 0
    trafoLimit = 1300000/scaling

    """"
    ==========================  aggregated power plots  =========================
    """

    #power plot per percentage
    plt.figure("PHC sweep aggregated power plot")
    ax = plt.gca()
    ax.set_xlim([minX, maxX])
    ax.set_ylim([minY, maxY])
    df = pd.DataFrame.from_dict(outputData.df)
    plt.axhline(y=trafoLimit, color = 'r', label = "Power capacity", linewidth=2.5, linestyle = '--')
    plt.plot(df.index.values.tolist(),df["{}W-power.plan.real.c.ELECTRICITY_BufferTimeshiftable".format(case)]/scaling,label = r"Target profile $\vec p$", linewidth=2.5)
    plt.plot(df.index.values.tolist(),df["{}W-power.real.c.ELECTRICITY_BufferTimeshiftable".format(case)]/scaling,label = r"Realization $\vec x$", linewidth=2.5)
    plt.xlabel('Time [15m since simulation start]')
    plt.ylabel('Aggregated power [kW]')
    plt.legend(loc = 1, title='Percentage controlled: {}%'.format(str(i)))
    plt.grid()
    plt.savefig(figures_dir +"PowerAgg_11kW{}.pdf".format('Sweep' + str(i)))
    plt.savefig(figures_dir +"PowerAgg_11kW{}.png".format('Sweep' + str(i)))
    plt.close()

    """"
    ==========================  stack plots  =========================
    """

    if perEV:
        plt.figure("PHC sweep stacked area plot")
        ax = plt.gca()
        ax.set_xlim([minX, maxX])
        ax.set_ylim([minY, maxY])
        df = pd.DataFrame.from_dict(outputData.df)
        plt.plot(df.index.values.tolist(),df["{}W-power.real.c.ELECTRICITY_BufferTimeshiftable".format(case)]/scaling,label = r"Realization $\vec x$", linewidth=2.5)
        plt.stackplot(df.index.values.tolist(),[df["{}W-power.real.c.ELECTRICITY_ElectricVehicle-{}".format(case,str(ev))]/scaling for ev in evs],linewidth = 2.5)#labels = ['real '+ str(ev) for ev in evs], linewidth=2.5)
        plt.xlabel('Time [15m since simulation start]')
        plt.ylabel('Power [kW]')
        plt.legend(loc = 1)
        plt.grid()
        plt.savefig(figures_dir +"PowerRealStackplot_11kW{}.pdf".format('Sweep' + str(i)))
        plt.close()

        plt.figure("PHC sweep stacked area plot")
        ax = plt.gca()
        ax.set_xlim([minX, maxX])
        ax.set_ylim([minY, maxY])
        df = pd.DataFrame.from_dict(outputData.df)
        plt.plot(df.index.values.tolist(),df["{}W-power.plan.real.c.ELECTRICITY_BufferTimeshiftable".format(case)]/scaling,label = r"Target profile $\vec p$", linewidth=2.5)
        plt.stackplot(df.index.values.tolist(),[df["{}W-power.plan.real.c.ELECTRICITY_ElectricVehicle-{}".format(case,str(ev))]/scaling for ev in evs],linewidth = 2.5)#labels = ['plan '+ str(ev) for ev in evs], linewidth=2.5)
        plt.xlabel('Time [15m since simulation start]')
        plt.ylabel('Power [kW]')
        plt.legend(loc = 1)
        plt.grid()
        plt.savefig(figures_dir +"PowerPlanStackplot_11kW{}.pdf".format('Sweep' + str(i)))
        plt.close()

    """"
    ==========================  disaggregated power plots  =========================
    """

    scaling = 1000
    scalingRelative = 1/100

    #frame limits for aggregated plots:
    maxX = 96
    minX = 0
    maxY = 12000/scaling
    minY = 0

    if perEV:
        if i in [0,20,25,40,50,60,75,80,100]:
            #power plot per EV: plan and real, plus aggregated bts
            for ev in evs:
                plt.figure("PHC sweep individual power profiles")
                ax = plt.gca()
                ax.set_xlim([minX, maxX])
                ax.set_ylim([minY, maxY])
                df = pd.DataFrame.from_dict(outputData.df)
                plt.plot(df.index.values.tolist(),df["{}W-power.plan.real.c.ELECTRICITY_BufferTimeshiftable".format(case)]/scaling,label = r"Target profile $\vec p$", linewidth=2.5)
                plt.plot(df.index.values.tolist(),df["{}W-power.real.c.ELECTRICITY_BufferTimeshiftable".format(case)]/scaling,label = r"Realization $\vec x$", linewidth=2.5)
                plt.plot(df.index.values.tolist(),df["{}W-power.plan.real.c.ELECTRICITY_ElectricVehicle-{}".format(case,str(ev))]/scaling,label = 'plan '+ str(ev), linewidth=2.5)
                plt.plot(df.index.values.tolist(),df["{}W-power.real.c.ELECTRICITY_ElectricVehicle-{}".format(case,str(ev))]/scaling,label = 'real '+ str(ev), linewidth=2.5)
                plt.xlabel('Time [15m since simulation start]')
                plt.ylabel('Individual power [kW]')
                plt.legend(loc = 1)
                plt.grid()
                plt.savefig(figures_dir + "Disaggregated/" +"PowerEV{}{}_11kW.pdf".format(ev,'Sweep' + str(i)))
                plt.savefig(figures_dir + "Disaggregated/" +"PowerEV{}{}_11kW.png".format(ev,'Sweep' + str(i)))
                plt.close()

    print("Done Power Plots!")
    print("===================================================================================================================================")

#make gif with all cases
aggPowerImages = []
for fn in [figures_dir +"PowerAgg_11kW{}.png".format('Sweep' + str(i)) for i in range(0,101)]:
    aggPowerImages.append(imageio.imread(fn))
imageio.mimsave(figures_dir +"PowerAgg{}_11kW.gif".format('Sweep'), aggPowerImages)

print("Done Sweep Gif!")
print("===================================================================================================================================")

""""
==========================  start evolution plot  =========================
"""

#read in start and end times of EVs for 100% controlled case
dfIn = pd.read_csv('PHC_sweep_11kW_100ElectricVehicle_Starttimes_real.txt',header=None)
dfIn[['EV', 'start']] = dfIn[0].str.split(':', expand = True)
dfIn['end'] = pd.read_csv('PHC_sweep_11kW_100ElectricVehicle_Endtimes_vetoXsmart_real.txt',header=None)[0].str.split(':',expand = True)[1]
dfIn.sort_values(by = ['start'], inplace=True)

case = 'PHC_sweep_perfect_EC_11kW_'+str(100)

#swipe data per EV
evs = list(range(0,400))
if not perEV:
    for ev in evs:
        #realization of charging power
        field = "W-power.real.c.ELECTRICITY"
        measurement = "{}devices".format(case)
        condition = "\"name\" = 'ElectricVehicle-{}'".format(ev)
        outputData.dataSwipe(field, measurement, condition, 'sum', True)
        outputData.dataAppend('{}{}_ElectricVehicle-{}'.format(case, field, ev))

        #offline planning of charging power based on estimations
        field = "W-power.plan.real.c.ELECTRICITY"
        measurement = "{}controllers".format(case)
        condition = "\"name\" = 'ElectricVehicleController{}'".format(ev)
        outputData.dataSwipe(field, measurement, condition, 'sum', True)
        outputData.dataAppend('{}{}_ElectricVehicle-{}'.format(case, field, ev),True)

#evolution plot, minute base 
scaling = 1000
scalingRelative = 1/100
scalingResult = scaling#*15 #needed for 1min simulation to correct the power output
#frame limits for aggregated plots:
maxX = 96
minX = 0
maxY = 920000/scaling #2300000/scaling
minY = 0
trafoLimit = 1300000/scaling

df = pd.DataFrame.from_dict(outputData.df)
df['tempX'] = 0

for ev in evs:
    plt.figure("PHC sweep aggregated power plot")
    ax = plt.gca()
    ax.set_xlim([minX, maxX])
    ax.set_ylim([minY, maxY])
    plt.axhline(y=trafoLimit, color = 'r', label = "Power capacity", linewidth=2.5, linestyle = '--')
    plt.plot(df.index.values.tolist(),df["{}W-power.plan.real.c.ELECTRICITY_BufferTimeshiftable".format(case)]/scalingResult,label = r"Target profile $\vec p$", linewidth=2.5)
    plt.plot(df.index.values.tolist(),df["{}W-power.real.c.ELECTRICITY_BufferTimeshiftable".format(case)]/scalingResult,label = r"Realization $\vec x$", linewidth=2.5)
    plt.plot(df.index.values.tolist(),df['tempX']/scalingResult,label = r"Already planned load $\vec x$", linewidth=2.5)
    plt.plot(df.index.values.tolist(),df["{}W-power.real.c.ELECTRICITY_ElectricVehicle-{}".format(case,dfIn['EV'].iloc[ev])]/scalingResult,label = "Profile EV {}".format(ev), linewidth=2.5)
    plt.axvspan(int(dfIn["start"].iloc[ev])/timeBase, int(dfIn["end"].iloc[ev])/timeBase, facecolor='C0',label = "Availability EV " + str(ev), alpha = 0.2)
    plt.xlabel('Time [15m since simulation start]')
    plt.ylabel('Aggregated power [kW]')
    plt.legend(loc = 1, title='Percentage controlled: {}%'.format(str(100)))
    plt.grid()
    plt.savefig(figures_dir +"Disaggregated/PowerEV{}_11kW{}.png".format(ev,'Sweep' + str(100))) #ev is sorted by arrival time
    plt.close()
    df['tempX'] += df["{}W-power.real.c.ELECTRICITY_ElectricVehicle-{}".format(case,dfIn['EV'].iloc[ev])]

#make gif with all EVs, building up \vec x
aggPowerImages = []
for fn in [figures_dir +"Disaggregated/PowerEV{}_11kW{}.png".format(ev, 'Sweep' + str(100)) for ev in evs]:
    aggPowerImages.append(imageio.imread(fn))
imageio.mimsave(figures_dir +"PowerEVs{}_11kW_evolutionEC.gif".format('Sweep'), aggPowerImages)

print("Done Evolution Plot Controlled Case!")
print("===================================================================================================================================")

#make compliancy plot: argmax_i([max_{j<=i} max_t Power.plan.i] < trafoLimiet)
#x = trafolimiet
def f(df,x,key,args):
    n = max(args) +1
    #FIXME add sanity check is there such a value
    for j in args:
        if df[[key.format(k) for k in range(j,n)]].max().max()<=x:
            return j
    print('no valid result', x)
    return np.nan

plt.figure("PHC sweep aggregated power plot")
ax = plt.gca()
df = pd.DataFrame.from_dict(outputData.df)
plt.plot([i*50000/scaling for i in range(9,34)],[f(df,i*50000,key='PHC_sweep_perfect_EC_11kW_{}W-power.plan.real.c.ELECTRICITY_BufferTimeshiftable',args=rng) for i in range(9,34)],label = r"Target profile $\vec p$", linewidth =2.5)
plt.plot([i*50000/scaling for i in range(9,34)],[f(df,i*50000,key='PHC_sweep_perfect_EC_11kW_{}W-power.real.c.ELECTRICITY_BufferTimeshiftable',args=rng) for i in range(9,34)],label = r"Realization $\vec x$", linewidth =2.5)
plt.xlabel('Aggregated peak power [kW]')
plt.ylabel('Fraction controlled EVs [-]')
plt.legend(loc = 1)
plt.grid()
plt.savefig(figures_dir +"PowerCompliancy_11kW{}.pdf".format(''))
plt.close()

print('Plan: Fraction controlled EVs after which transformerlimit compliant: {}%'.format(f(df, trafoLimit*scaling, key='PHC_sweep_perfect_EC_11kW_{}W-power.plan.real.c.ELECTRICITY_BufferTimeshiftable', args=rng)))
print('Real: Fraction controlled EVs after which transformerlimit compliant: {}%'.format(f(df, trafoLimit*scaling, key='PHC_sweep_perfect_EC_11kW_{}W-power.real.c.ELECTRICITY_BufferTimeshiftable', args=rng)))

scaling = 1000
scalingRelative = 1/100

#frame limits for aggregated plots:
maxX = 96
minX = 0
maxY = 2500000/scaling
minY = 0
trafoLimit = 1300000/scaling

#guarantee vs no guarantee plot
plt.figure("PHC guarantee aggregated power plot")
ax = plt.gca()
ax.set_xlim([minX, maxX])
ax.set_ylim([minY, maxY])
df = pd.DataFrame.from_dict(outputData.df)
plt.axhline(y=trafoLimit, color = 'r', label = "Power capacity", linewidth=2.5, linestyle = '--')
plt.plot(df.index.values.tolist(),df["{}W-power.real.c.ELECTRICITY_BufferTimeshiftable".format('PHC_sweep_100uncontrolled_11kW')]/scaling,label = "No guarantee fast charging", linewidth=2.5)
plt.plot(df.index.values.tolist(),df["{}W-power.real.c.ELECTRICITY_BufferTimeshiftable".format('PHC_sweep_100greedy_11kW')]/scaling,label = "Guarantee fast charging", linewidth=2.5)
plt.plot(df.index.values.tolist(),df["{}W-power.plan.real.c.ELECTRICITY_BufferTimeshiftable".format('PHC_sweep_perfect_EC_11kW_'+str(100))]/scaling,label = "Guarantee coordinated charging", linewidth=2.5)
plt.xlabel('Time [15m since simulation start]')
plt.ylabel('Aggregated power [kW]')
plt.legend(loc = 1)
plt.grid()
plt.savefig(figures_dir +"PowerAggGuarantee_11kW{}.pdf".format('Sweep' + str(i)))
plt.close() 

print("Done Saving Figures!")
print("===================================================================================================================================")

""""
==========================  save to Excel  =========================
"""

outputData.dataSaveExcel('resultExport_PHC_sweep_11kW',write=True)

print("Done Save to Excel!")
print("===================================================================================================================================")

# End 
print("Whoop whoop all compiled")
