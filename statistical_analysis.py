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

import itertools
import pandas as pd
import datetime
import os
import numpy as np
from matplotlib import pyplot
from statsmodels.distributions.empirical_distribution import ECDF
import math
import scipy
import seaborn as sns; # sns.set_theme()

""""
========================== Statistical Analysis =========================
"""

class StatisticalAnalysis:
    def __init__(self, data, carStats=None):  
        self.data = data
        
        # =============================================================================================================
        #holy grail. Per column, determines count, mean, std, min, 25%, 50%, 75%, max. Saves in dataframe. 
        #only works on numerical values
        self.global_statistics_summary = self.data.describe()
        print(self.data.shape)
        print(self.data.info())
        print(self.data.describe())
        self.global_statistics_summary.to_csv('global_statistics_summary.csv',index=True) 
        #https://medium.com/analytics-vidhya/statistical-analysis-in-python-using-pandas-27c6a4209de2


    """"
    ========================== statistical analysis of the energy demand (overall and per car). =========================
    """

    def energy_demand_plots(self):
        data = self.data
        folder = "Energy_demand/"
        figures_dir = os.path.join('Figures/',folder)
        if not os.path.isdir(figures_dir):
            os.makedirs(figures_dir)

        # Plot energy consumption through time at a.s.r. df

        montly_average_energy_consumption = data.groupby(pd.PeriodIndex(data['start_datetime_utc'], freq="M"))['total_energy'].mean().to_frame().reset_index()
        montly_total_energy_consumption = data.groupby(pd.PeriodIndex(data['start_datetime_utc'], freq="M"))['total_energy'].sum().to_frame().reset_index()
        montly_total_energy_consumption['start_datetime_utc'] = montly_total_energy_consumption['start_datetime_utc'].dt.to_timestamp()
      

        pyplot.figure("energy_whole_period")
        pyplot.rcParams['font.size'] = '16'
        pyplot.figure(figsize=(10, 8))
        pyplot.plot(montly_total_energy_consumption['start_datetime_utc'], montly_total_energy_consumption['total_energy']/1000, linewidth=3)
        #pyplot.plot(montly_average_energy_consumption.values)
        pyplot.xlabel('Month')
        pyplot.ylabel('Monthly energy delivered [MWh]')
        pyplot.xticks(rotation=45)
        pyplot.savefig(figures_dir +"energy_whole_period.pdf", bbox_inches = "tight")
        pyplot.close(pyplot.figure("energy_whole_period"))


        pyplot.figure("energy_whole_period_2")
        pyplot.rcParams['font.size'] = '16'
        pyplot.figure(figsize=(10, 8))
        pyplot.bar(montly_total_energy_consumption['start_datetime_utc'], montly_total_energy_consumption['total_energy']/1000, width = 25)
        #pyplot.plot(montly_average_energy_consumption.values)
        pyplot.xlabel('Month')
        pyplot.ylabel('Monthly energy delivered [MWh]')
        pyplot.xticks(rotation=45)
        pyplot.savefig(figures_dir +"energy_whole_period_2.pdf", bbox_inches = "tight")
        pyplot.close(pyplot.figure("energy_whole_period_2"))
        
        # plot boxes diagram
        pyplot.figure("box_energy")
        pyplot.rcParams['font.size'] = '16'
        pyplot.boxplot(data['total_energy'], vert = False)
        pyplot.xlabel('Energy charged [kWh]')
        pyplot.savefig(figures_dir +"box_energy.pdf", bbox_inches = "tight")
        pyplot.close(pyplot.figure("box_energy"))


        #plot histogramm
        pyplot.figure("Histogram_energy")
        pyplot.rcParams['font.size'] = '16'
        pyplot.hist(data['total_energy'],bins = 100)
        pyplot.xlabel('Energy charged [kWh]')
        pyplot.ylabel('Number of sessions')
        pyplot.savefig(figures_dir +"Histogram_energy.pdf", bbox_inches = "tight")
        pyplot.close(pyplot.figure("Histogram_energy"))


        #plot probability density function
        mean = data['total_energy'].mean()
        std = data['total_energy'].std()

        pyplot.figure("pdf-energy")
        pyplot.rcParams['font.size'] = '16'
        a = data['total_energy'].plot.kde()

        pyplot.axvline(x=mean, color='r', ls='--', label='mean')
        pyplot.axvline(x=mean-std, color='b', ls='--', label='std(+/-)')
        pyplot.axvline(x=mean+std, color='b', ls='--')
        pyplot.legend()
        pyplot.xlabel('Energy charged [kWh]')
        pyplot.ylabel('Density')
        pyplot.savefig(figures_dir +"pdf-energy.pdf", bbox_inches = "tight")


        # plot fitiing pdf norm
        mu, sigma = scipy.stats.distributions.norm.fit(data['total_energy'])
        x = np.linspace(mu-3*sigma, mu+3*sigma, 200)
        fitted_data = scipy.stats.distributions.norm.pdf(x, mu, sigma)
        pyplot.figure("fitting-pdf-energy-norm")
        pyplot.rcParams['font.size'] = '16'
        pyplot.hist(data['total_energy'], bins=100, density=True, label='Data')
        pyplot.plot(x,fitted_data,'r-', label='Norm pdf (fit)')
        pyplot.legend()
        pyplot.xlabel('Energy charged [kWh]')
        pyplot.ylabel('Density')
        pyplot.savefig(figures_dir +"fitting-pdf-energy-norm.pdf", bbox_inches = "tight")


        # plot fitiing pdf beta
        data_norm = data['total_energy']/np.linalg.norm(data['total_energy'])

        a, b, loc, scale = scipy.stats.distributions.beta.fit(data_norm)
        x = np.linspace(scipy.stats.beta.ppf(0.01, a, b), scipy.stats.beta.ppf(0.99, a, b),100)
        fitted_data = scipy.stats.distributions.beta.pdf(x, a, b, loc=loc, scale=scale)
        pyplot.figure("fitting-pdf-energy-beta")
        pyplot.rcParams['font.size'] = '16'
        pyplot.hist(data_norm, bins=100, density=True, label='Data')
        pyplot.plot(x,fitted_data,'r-', label='Beta pdf (fit)')
        locs, labels = pyplot.xticks()
        pyplot.legend()
        #pyplot.xticks(locs, ['', '0', '20', '40', '60', '80', '100', '120', '140', ''])
        pyplot.xlabel('Energy charged [kWh]')
        pyplot.ylabel('Number of sessions')
        pyplot.savefig(figures_dir +"fitting-pdf-energy-beta.pdf", bbox_inches = "tight")


        #determine empirical cumulative density function of energy demand. I.e. ecdf(x) = P(energy demand <=x)
        pyplot.figure("cdf-energy")
        pyplot.rcParams['font.size'] = '16'
        ecdf = ECDF(data['total_energy'])
        pyplot.plot(ecdf.x, ecdf.y, label='CDF')
        pyplot.xlabel('Energy charged [kWh]')
        pyplot.ylabel('Probability')
        pyplot.legend()
        pyplot.savefig(figures_dir +"cdf-energy.pdf", bbox_inches = "tight")

    #per car, we estimate the maximum power by the maximum of the average charging power per session. 
    #exact if a car at some point ended a greedy session while still in the process of charging.
    def power_estimation_stats(self,carStats = None, powerDefault = 0, capacityDefault = 100000):
        data = self.data
        #we start analyzing data per unique car, identified by their card ID. 
        #dataframe with all unique card ids in the dataset
        cardIDs = pd.unique(self.data['card_id'])
        
        #placeholder for data we will generate per car. First column will be the card_ids as unique identifier.
        if carStats is not None:
            print('[MESSAGE:] No new carStats object was created. Augmented the input carStats object. Note that in the current configuation the column card_id is being redefined. Make sure that the data inputted in the function is the same as when the carStats object was created to prevent faulty bookkeeping.')
        else:
            carStats = pd.DataFrame()
        
        #FIXME check if input carStats non-empty whether need to append. Now assuming that all functions get the same data input
        carStats['card_id'] = cardIDs
        carStats['power_default'] = powerDefault
        carStats['capacity_default'] = capacityDefault

        statsMatrix = np.empty([0,1])        
        for car in carStats['card_id']:
            maxAverage = max(data['average_power_W'][data['card_id']==car])
            statsMatrix = np.vstack([statsMatrix, max(powerDefault, maxAverage)])
        
        #add stats columns to dataframe carStats
        carStats = pd.concat([carStats, pd.DataFrame(data = statsMatrix, index = None, columns = ['power_max_average_W'])], axis=1)
        return carStats

    def energy_demand_stats(self, carStats = None, percentiles = None):
        data = self.data
        
        #we start analyzing data per unique car, identified by their card ID. 
        #dataframe with all unique card ids in the dataset
        cardIDs = pd.unique(self.data['card_id'])
        
        #placeholder for data we will generate per car. First column will be the card_ids as unique identifier.
        if carStats is not None:
            print('[MESSAGE:] No new carStats object was created. Augmented the input carStats object. Note that in the current configuation the column card_id is being redefined. Make sure that the data inputted in the function is the same as when the carStats object was created to prevent faulty bookkeeping.')
        else:
            carStats = pd.DataFrame()
        #FIXME check if input carStats non-empty whether need to append. Now assuming that all functions get the same data input
        carStats['card_id'] = cardIDs

        #get all statistical data with respect to "variable under analysis" per individual car and put them in carStats dataframe
        #for easy conversion, we first save the output of pd.dataframe.describe() per car in the row of a numpy array
        statsMatrix = np.empty([0,8])

        #per car, generate .describe() stats
        for car in carStats['card_id']:
            statsMatrix = np.vstack([statsMatrix, data[data['card_id'] == car].describe()['total_energy_Wh'].to_numpy()])
        
        #statsMatrix might have empty entries: if only one occurence (only one charging session with that card_id), then std is not well-defined
        energyStats = pd.DataFrame(data = statsMatrix, index = None, columns = ['count', 'energy_mean', 'energy_std', 'energy_min', 'energy_25', 'energy_50', 'energy_75', 'energy_max'])
        #add stats columns to dataframe carStats
        carStats = pd.concat([carStats,energyStats], axis=1)

        #create empirical cumulative distribution function per car based on total energy.
        #this approach does not work with start times, since datetime objects cannot be compared to integers using ">".
        statsList = []
        for car in carStats['card_id']:
            statsList = statsList + [ECDF(data[data['card_id'] == car]['total_energy'])]
        carStats['energy_ecdf'] = statsList

        #for parameter sweep, we hold open the option to add percentiles of the data
        if percentiles is not None:
            percentileMatrix = np.empty([0,len(percentiles)])
            #checked percentiles against min and max values. percentile 0 = min, percentile 100 = max ok.
            for car in carStats['card_id']:
                percentileMatrix = np.vstack([percentileMatrix, np.percentile(data[data['card_id'] ==car]['total_energy_Wh'], percentiles)])
            headers = []
            for i in range(0,len(percentiles)):
                headers = headers + ['e{}'.format(percentiles[i])]
            start_percentiles = pd.DataFrame(data = percentileMatrix, index = None, columns = headers)
            carStats = pd.concat([carStats,start_percentiles],axis=1)
        
        return carStats

    """"
    ========================== statistical analysis of the starting time (overall and per car). =========================
    """

    def start_time_plots(self):
        data = self.data
        folder = "Start_time/"
        figures_dir = os.path.join('Figures/',folder)
        if not os.path.isdir(figures_dir):
            os.makedirs(figures_dir)

        # plot boxes diagram
        pyplot.figure("box_starttime")
        pyplot.rcParams['font.size'] = '16'
        pyplot.boxplot(data['start_datetime_hours'], vert = False)
        pyplot.xlabel('Start time [h]')
        pyplot.xticks(np.arange(0, 28, 4))  # Set label locations.
        pyplot.savefig(figures_dir +"box_starttime.pdf", bbox_inches = "tight")
        pyplot.close(pyplot.figure("box_starttime"))

        #plot histogramm
        pyplot.figure("Histogram_starttime")
        pyplot.rcParams['font.size'] = '16'
        pyplot.hist(data['start_datetime_hours'],bins = 100)
        pyplot.xlabel('Start time [h]')
        pyplot.ylabel('Number of sessions')
        pyplot.xticks(np.arange(0, 28, 4))  # Set label locations.
        pyplot.savefig(figures_dir +"Histogram_starttime.pdf", bbox_inches = "tight")
        pyplot.close(pyplot.figure("Histogram_starttime"))


        #plot probability density function
        mean = data['start_datetime_hours'].mean()
        std = data['start_datetime_hours'].std()

        pyplot.figure("pdf-starttime")
        pyplot.rcParams['font.size'] = '16'
        a = (data['start_datetime_hours']).plot.kde()
        pyplot.axvline(x=mean, color='r', ls='--', label='mean')
        pyplot.axvline(x=mean-std, color='b', ls='--', label='std(+/-)')
        pyplot.axvline(x=mean+std, color='b', ls='--')
        pyplot.legend()
        pyplot.xlabel('Arrival time [h]')
        pyplot.xticks(np.arange(0, 28, 4))  # Set label locations.
        pyplot.ylabel('Density')
        pyplot.savefig(figures_dir +"pdf-starttime.pdf")



        # plot fitiing pdf norm
        mu, sigma = scipy.stats.distributions.norm.fit(data['start_datetime_hours'])
        x = np.linspace(mu-3*sigma, mu+3*sigma, 200)
        fitted_data = scipy.stats.distributions.norm.pdf(x, mu, sigma)
        pyplot.figure("fitting-pdf-starttime-norm")
        pyplot.rcParams['font.size'] = '16'
        pyplot.hist(data['start_datetime_hours'], bins=100, density=True, label='Data')
        pyplot.plot(x,fitted_data,'r-', label='Norm pdf (fit)')
        pyplot.legend()
        pyplot.xlabel('Arrival time [h]')
        pyplot.ylabel('Density')
        pyplot.savefig(figures_dir +"fitting-pdf-starttime-norm.pdf",bbox_inches = "tight")


        # plot fitiing pdf beta
        data_norm = data['start_datetime_hours']/np.linalg.norm(data['start_datetime_hours'])

        a, b, loc, scale = scipy.stats.distributions.beta.fit(data_norm)
        
        x = np.linspace(scipy.stats.beta.ppf(0.01, a, b), scipy.stats.beta.ppf(0.99, a, b),100)
        
        fitted_data = scipy.stats.distributions.beta.pdf(x, a, b, loc=loc, scale=scale)
        pyplot.figure("fitting-pdf-starttime-beta")
        pyplot.rcParams['font.size'] = '16'
        pyplot.hist(data_norm, bins=100, density=True, label='Data')
        pyplot.plot(x,fitted_data,'r-', label='Beta pdf (fit)')
        locs, labels = pyplot.xticks()
        pyplot.legend()
        # pyplot.xticks(locs, ['', '0', '20', '40', '60', '80', '100', '120', '140', ''])
        pyplot.xlabel('Arrival time [h]')
        pyplot.ylabel('Number of sessions')
        pyplot.savefig(figures_dir +"fitting-pdf-starttime-beta.pdf", bbox_inches = "tight")


        #FIXME only consider HH:MM and disregards dates. Want the distribution as a function of time during the day
        pyplot.figure("cdf-starttime-hour")
        pyplot.rcParams['font.size'] = '16'
        ecdf = ECDF(data['start_datetime_hours'])
        pyplot.plot(ecdf.x, ecdf.y, label='CDF')
        pyplot.xlabel('Arrival time [h]')
        pyplot.ylabel('Probability')
        pyplot.xticks(np.arange(0, 28, 4))  # Set label locations.
        pyplot.legend()
        pyplot.savefig(figures_dir +"cdf-starttime-hour.pdf",bbox_inches = "tight")


    def start_time_stats(self, carStats = None, percentiles = None):
        data = self.data
        
        #we start analyzing data per unique car, identified by their card ID. 
        #dataframe with all unique card ids in the dataset
        cardIDs = pd.unique(self.data['card_id'])
        
        #placeholder for data we will generate per car. First column will be the card_ids as unique identifier.
        if carStats is not None:
            print('[MESSAGE:] No new carStats object was created. Augmented the input carStats object. Note that in the current configuation the column card_id is being redefined. Make sure that the data inputted in the function is the same as when the carStats object was created to prevent faulty bookkeeping.')
        else:
            carStats = pd.DataFrame()
        
        #FIXME check if input carStats non-empty whether need to append. Now assuming that all functions get the same data input
        carStats['card_id'] = cardIDs

        #get all statistical data with respect to "variable under analysis" per individual car and put them in carStats dataframe
        #for easy conversion, we first save the output of pd.dataframe.describe() per car in the row of a numpy array
        statsMatrix = np.empty([0,8])

        #per car, generate .describe() stats on data in seconds
        for car in carStats['card_id']:
            statsMatrix = np.vstack([statsMatrix, data[data['card_id'] == car].describe()['start_datetime_seconds'].to_numpy()])
        #statsMatrix might have empty entries: if only one occurence (only one charging session with that card_id), then std is not well-defined
        start_time_Stats = pd.DataFrame(data = statsMatrix, index = None, columns = ['count', 'start_time_mean', 'start_time_std', 'start_time_min', 'start_time_25', 'start_time_50', 'start_time_75', 'start_time_max'])
        #add stats columns to dataframe carStats
        carStats = pd.concat([carStats,start_time_Stats], axis=1)

        # Have to remove duplicated count column 
        carStats = carStats.loc[:,~carStats.columns.duplicated()]

        #create empirical cumulative distribution function per car based on start times.
        statsList = []
        for car in carStats['card_id']:
            statsList = statsList + [ECDF(data[data['card_id'] == car]['start_datetime_hours'])]
        carStats['start_time_ecdf'] = statsList

        #for parameter sweep, we hold open the option to add percentiles of the data
        if percentiles is not None:
            percentileMatrix = np.empty([0,len(percentiles)])
            #checked percentiles against min and max values. percentile 0 = min, percentile 100 = max ok.
            for car in carStats['card_id']:
                percentileMatrix = np.vstack([percentileMatrix, np.percentile(data[data['card_id'] ==car]['start_datetime_seconds'], percentiles)])
            headers = []
            for i in range(0,len(percentiles)):
                headers = headers + ['s{}'.format(percentiles[i])]
            start_percentiles = pd.DataFrame(data = percentileMatrix, index = None, columns = headers)
            carStats = pd.concat([carStats,start_percentiles],axis=1)

        return carStats


    """"
    ========================== statistical analysis of the departure time (overall and per car). =========================
    """

    def end_time_plots(self):
        data = self.data
        folder = "End_time/"
        figures_dir = os.path.join('Figures/',folder)
        if not os.path.isdir(figures_dir):
            os.makedirs(figures_dir)

        # plot boxes diagram
        pyplot.figure("box_endtime")
        pyplot.rcParams['font.size'] = '16'
        pyplot.boxplot(data['end_datetime_hours'], vert = False)
        pyplot.xlabel('Departure time [h]')
        pyplot.xticks(np.arange(0, 28, 4))  # Set label locations.
        pyplot.savefig(figures_dir +"box_endtime.pdf", bbox_inches = "tight")
        pyplot.close(pyplot.figure("box_endtime"))

        #plot histogramm
        pyplot.figure("Histogram_endtime")
        pyplot.rcParams['font.size'] = '16'
        pyplot.hist(data['end_datetime_hours'],bins = 100)
        pyplot.xlabel('Departure time [h]')
        pyplot.ylabel('Number of sessions')
        pyplot.xticks(np.arange(0, 28, 4))  # Set label locations. 
        pyplot.savefig(figures_dir +"Histogram_endtime.pdf", bbox_inches = "tight")
        pyplot.close(pyplot.figure("Histogram_endtime"))

        
        #plot probability density function
        mean = data['end_datetime_hours'].mean()
        std = data['end_datetime_hours'].std()

        pyplot.figure("pdf-endtime")
        pyplot.rcParams['font.size'] = '16'
        a = (data['end_datetime_hours']).plot.kde()
        pyplot.axvline(x=mean, color='r', ls='--', label='mean')
        pyplot.axvline(x=mean-std, color='b', ls='--', label='std(+/-)')
        pyplot.axvline(x=mean+std, color='b', ls='--')
        pyplot.legend()
        pyplot.xlabel('Departure time [h]')
        pyplot.ylabel('Density')
        pyplot.xticks(np.arange(0, 28, 4))  # Set label locations.
        pyplot.savefig(figures_dir +"pdf-endtime.pdf", bbox_inches = "tight")
        pyplot.close(pyplot.figure("pdf-endtime"))
        

        #FIXME only consider HH:MM and disregards dates. Want the distribution as a function of time during the day
        pyplot.figure("cdf-endtime-hour")
        pyplot.rcParams['font.size'] = '16'
        ecdf = ECDF(data['end_datetime_hours'])
        pyplot.plot(ecdf.x, ecdf.y, label='CDF')
        pyplot.xlabel('Departure time [h]')
        pyplot.ylabel('Probability')
        pyplot.xticks(np.arange(0, 28, 4))  # Set label locations.
        pyplot.legend()
        pyplot.savefig(figures_dir +"cdf-endtime-hour.pdf", bbox_inches = "tight")
        pyplot.close(pyplot.figure("cdf-endtime-hour"))

    def end_time_stats(self, carStats = None):
        data = self.data

        #we start analyzing data per unique car, identified by their card ID. 
        #dataframe with all unique card ids in the dataset
        cardIDs = pd.unique(self.data['card_id'])

        #placeholder for data we will generate per car. First column will be the card_ids as unique identifier.
        if carStats is not None:
            print('[MESSAGE:] No new carStats object was created. Augmented the input carStats object. Note that in the current configuation the column card_id is being redefined. Make sure that the data inputted in the function is the same as when the carStats object was created to prevent faulty bookkeeping.')
        else:
            carStats = pd.DataFrame()
        #FIXME check if input carStats non-empty whether need to append. Now assuming that all functions get the same data input
        carStats['card_id'] = cardIDs

        #get all statistical data with respect to starting time per individual car and put them in carStats dataframe
        #for easy conversion, we first save the output of pd.dataframe.describe() per car in the row of a numpy array
        statsMatrix = np.empty([0,8])
        #per car, generate .describe() stats on data in seconds
        for car in carStats['card_id']:
            statsMatrix = np.vstack([statsMatrix, data[data['card_id'] == car].describe()['end_datetime_seconds'].to_numpy()])
        #statsMatrix might have empty entries: if only one occurence (only one charging session with that card_id), then std is not well-defined
        end_time_Stats = pd.DataFrame(data = statsMatrix, index = None, columns = ['count', 'end_time_mean', 'end_time_std', 'end_time_min', 'end_time_25', 'end_time_50', 'end_time_75', 'end_time_max'])
        #add stats columns to dataframe carStats
        carStats = pd.concat([carStats,end_time_Stats], axis=1)

        # Have to remove duplicated count column 
        carStats = carStats.loc[:,~carStats.columns.duplicated()]

        #create empirical cumulative distribution function per car based on total energy.
        #this approach does not work with end times, since datetime objects cannot be compared to integers using ">".
        statsList = []
        for car in carStats['card_id']:
            statsList = statsList + [ECDF(data[data['card_id'] == car]['end_datetime_hours'])]
        carStats['end_time_ecdf'] = statsList

        return carStats



    """"
    ========================== statistical analysis of the dwell time (overall and per car). =========================
    """

    def dwell_time_plots(self):
        data = self.data

        folder = "Dwell_time/"
        figures_dir = os.path.join('Figures/',folder)
        if not os.path.isdir(figures_dir):
            os.makedirs(figures_dir)
        

        # plot boxes diagram
        pyplot.figure("box_dwelltime")
        #pyplot.tight_layout()
        pyplot.rcParams['font.size'] = '16'
        pyplot.boxplot(data['dwell_time_hours'], vert = False)
        pyplot.xlabel('Dwell time [h]')
        pyplot.xticks(np.arange(0, 28, 4),size=16)  # Set label locations.
        pyplot.savefig(figures_dir +"box_dwelltime.pdf", bbox_inches = "tight")
        pyplot.close(pyplot.figure("box_dwelltime"))

        #plot histogramm
        pyplot.figure("Histogram_dwelltime")
        #pyplot.tight_layout()
        pyplot.rcParams['font.size'] = '16'
        pyplot.hist(data['dwell_time_hours'],bins = 100)
        pyplot.xlabel('Dwell time [h]')
        pyplot.ylabel('Number of sessions')
        pyplot.xticks(np.arange(0, 28, 4))  # Set label locations.
        pyplot.savefig(figures_dir +"Histogram_dwelltime.pdf", bbox_inches = "tight")
        pyplot.close(pyplot.figure("Histogram_dwelltime"))

        #plot probability density function
        mean = data['dwell_time_hours'].mean()
        std = data['dwell_time_hours'].std()


        pyplot.figure("pdf-dwelltime")
        pyplot.rcParams['font.size'] = '16'
        a = (data['dwell_time_hours']).plot.kde()
        pyplot.axvline(x=mean, color='r', ls='--', label='mean')
        pyplot.axvline(x=mean-std, color='b', ls='--', label='std(+/-)')
        pyplot.axvline(x=mean+std, color='b', ls='--')
        pyplot.legend()
        pyplot.xlabel('Dwell time [h]')
        pyplot.ylabel('Density')
        pyplot.xticks(np.arange(0, 28, 4))  # Set label locations.
        pyplot.savefig(figures_dir +"pdf-dwelltime.pdf", bbox_inches = "tight")
        pyplot.close(pyplot.figure("pdf-dwelltime"))
        

        #FIXME only consider HH:MM and disregards dates. Want the distribution as a function of time during the day
        pyplot.figure("cdf-dwelltime-hour")
        pyplot.rcParams['font.size'] = '16'
        ecdf = ECDF(data['dwell_time_hours'])
        pyplot.plot(ecdf.x, ecdf.y, label='CDF')
        pyplot.legend()
        pyplot.xlabel('Dwell time [h]')
        pyplot.ylabel('Probability')
        pyplot.xticks(np.arange(0, 28, 4))  # Set label locations.
        pyplot.savefig(figures_dir +"cdf-dwelltime-hour.pdf", bbox_inches = "tight")
        pyplot.close(pyplot.figure("cdf-dwelltime-hour"))
        
    def dwell_time_stats(self, carStats = None, dwell = 1800, percentiles = None):
        data = self.data

        #we start analyzing data per unique car, identified by their card ID. 
        #dataframe with all unique card ids in the dataset
        cardIDs = pd.unique(self.data['card_id'])

        #placeholder for data we will generate per car. First column will be the card_ids as unique identifier.
        if carStats is not None:
            print('[MESSAGE:] No new carStats object was created. Augmented the input carStats object. Note that in the current configuation the column card_id is being redefined. Make sure that the data inputted in the function is the same as when the carStats object was created to prevent faulty bookkeeping.')
        else:
            carStats = pd.DataFrame()
        #FIXME check if input carStats non-empty whether need to append. Now assuming that all functions get the same data input
        carStats['card_id'] = cardIDs

        #get all statistical data with respect to starting time per individual car and put them in carStats dataframe
        #for easy conversion, we first save the output of pd.dataframe.describe() per car in the row of a numpy array
        statsMatrix = np.empty([0,8])
        #per car, generate .describe() stats
        for car in carStats['card_id']:
            statsMatrix = np.vstack([statsMatrix, data[data['card_id'] == car].describe()['dwell_time_seconds'].to_numpy()])
        #statsMatrix might have empty entries: if only one occurence (only one charging session with that card_id), then std is not well-defined
        dwell_time_Stats = pd.DataFrame(data = statsMatrix, index = None, columns = ['count', 'dwell_time_mean', 'dwell_time_std', 'dwell_time_min', 'dwell_time_25', 'dwell_time_50', 'dwell_time_75', 'dwell_time_max'])
        #add stats columns to dataframe carStats
        carStats = pd.concat([carStats,dwell_time_Stats], axis=1)

        # Have to remove duplicated count column 
        carStats = carStats.loc[:,~carStats.columns.duplicated()]

        #create empirical cumulative distribution function per car based on dwell times.
        statsList = []
        for car in carStats['card_id']:
            statsList = statsList + [ECDF(data[data['card_id'] == car]['dwell_time_hours'])]
        carStats['dwell_time_ecdf'] = statsList


        #based on dwell and start time stats, we create 'logical' end times. This only works if prior to dwell time stats, start time stats were generated.
        for aspect in itertools.product(['mean', 'max', 'min', '75', '50', '25'],['mean', 'max', 'min', '75', '50', '25']):
            #To prevent errors due to start and end times of a session being less than 15 minutes apart, or within the same 15 min interval, we enforce a minimum duration of 30 minutes
            carStats['dwell_time_{}Enforced'.format(aspect[1])] = carStats['dwell_time_{}'.format(aspect[1])]
            carStats.loc[carStats['dwell_time_{}'.format(aspect[1])] < dwell, 'dwell_time_{}Enforced'.format(aspect[1])] = dwell

            #calculate new end time 
            carStats['end_s{}_d{}'.format(aspect[0],aspect[1])] = carStats['start_time_{}'.format(aspect[0])] + carStats['dwell_time_{}Enforced'.format(aspect[1])]
        
        #for parameter sweep, we hold open the option to add percentiles of the data
        if percentiles is not None:
            percentileMatrix = np.empty([0,len(percentiles)])
            #checked percentiles against min and max values. percentile 0 = min, percentile 100 = max ok.
            for car in carStats['card_id']:
                percentileMatrix = np.vstack([percentileMatrix, np.percentile(data[data['card_id'] ==car]['dwell_time_seconds'], percentiles)])
            headers = []
            for i in range(0,len(percentiles)):
                headers = headers + ['d{}'.format(percentiles[i])]
            start_percentiles = pd.DataFrame(data = percentileMatrix, index = None, columns = headers)
            carStats = pd.concat([carStats,start_percentiles],axis=1)
        
            for aspect in itertools.product(percentiles, percentiles):
                #To prevent errors due to start and end times of a session being less than 15 minutes apart, or within the same 15 min interval, we enforce a minimum duration of 30 minutes
                carStats['d{}Enforced'.format(aspect[1])] = carStats['d{}'.format(aspect[1])]
                carStats.loc[carStats['d{}'.format(aspect[1])] < dwell, 'd{}Enforced'.format(aspect[1])] = dwell

                #calculate new end time 
                carStats['end_s{}d{}'.format(aspect[0],aspect[1])] = carStats['s{}'.format(aspect[0])] + carStats['d{}Enforced'.format(aspect[1])]
                
        return carStats


    """"
    ========================== statistical analysis - CORRELATION. =========================
    """

    def correlation(self):
        data = self.data
        column_names = ['total_energy', 'start_datetime_hours', 'end_datetime_hours', 'dwell_time_hours']
        folder = "Correlation/"
        figures_dir = os.path.join('Figures/',folder)
        if not os.path.isdir(figures_dir):
            os.makedirs(figures_dir)

        # calculate correlation among parameters
        corr = data[column_names].corr()

        print(corr)


        #plot correlation
        pyplot.figure("correlation", figsize=(12,10))
        pyplot.rcParams['font.size'] = '16'
        sns.heatmap(corr, cmap='viridis_r')
        #pyplot.matshow(corr, cmap='RdBu')
        pyplot.xticks(np.arange(0, len(column_names), 1)+0.5, ['Total energy', 'Arrival time', 'Departure time', 'Dwell time'],rotation=30)
        pyplot.yticks(np.arange(0, len(column_names), 1)+0.5, ['Total energy', 'Arrival time', 'Departure time', 'Dwell time'],rotation=45)

        #pyplot.ylabel('Number of sessions')
        #pyplot.xticks(rotation=0)
        #pyplot.yticks(rotation=90)
        pyplot.savefig(figures_dir +"correlation.pdf", bbox_inches = "tight")
        pyplot.close(pyplot.figure("correlation"))






    """"
    ========================== Individual statistical analysis (EV with most charging sessions)=========================
    """
    def stats_per_car(self, carStats):
        # Sorting cars with most charging sessions (high to low)
        carStats2 = carStats.sort_values(by="count", ascending=False)
               
        carStats2 = carStats2.head(10)
        print(max(carStats["count"]))
        print(carStats2)

        folder = "Per_Car/"
        figures_dir = os.path.join('Figures/',folder)
        if not os.path.isdir(figures_dir):
            os.makedirs(figures_dir)

        # ================================================================================================================================================
        #plot energy+percentiles (10 cars wieth more charging sessions)
        pyplot.figure("energy-percentiles-10-cars")
        pyplot.rcParams['font.size'] = '16'
        y=carStats2["energy_mean"]/1000
        x=np.arange(0,len(carStats2["card_id"]))
        pyplot.plot(x, y, color='r', label='Mean')
        locs2, labels2 = pyplot.xticks()

        y=carStats2["energy_max"]/1000
        y2=carStats2["energy_75"]/1000
        y3=carStats2["energy_50"]/1000
        y4=carStats2["energy_25"]/1000
        y5=carStats2["energy_min"]/1000

        pyplot.fill_between(x, y, color='gray', alpha=0.3, label='Max')
        pyplot.fill_between(x, y2, color='gray', alpha=0.4, label='75%')
        pyplot.fill_between(x, y3, color='gray', alpha=0.6, label='50%')
        pyplot.fill_between(x, y4, color='gray', alpha=0.8, label='25%')
        pyplot.fill_between(x, y5, color='black', alpha=0.2, label='Min')
        pyplot.legend(loc='upper right',fancybox=True, framealpha=0.5, fontsize=10)

        pyplot.ylabel('Energy demand [kWh]')

        pyplot.xticks(x, ['EV1', 'EV2', 'EV3', 'EV4', 'EV5', 'EV6', 'EV7', 'EV8', 'EV9', 'EV10'])
        pyplot.xticks(rotation='vertical')
        pyplot.savefig(figures_dir +"energy-percentiles-10-cars.pdf", bbox_inches = "tight")


        # ================================================================================================================================================
        #plot starttime+percentiles (10 cars wieth more charging sessions)
        pyplot.figure("starttime-percentiles-10-cars")
        pyplot.rcParams['font.size'] = '16'
        x = carStats2["card_id"]
        y = carStats2["start_time_mean"]/3600
        
        pyplot.plot(x, y, color='r',label='Mean')
        locs2, labels2 = pyplot.xticks()

        x=np.arange(0,len(carStats2["card_id"]))

        y=carStats2["start_time_max"]/3600
        y2=carStats2["start_time_75"]/3600
        y3=carStats2["start_time_50"]/3600
        y4=carStats2["start_time_25"]/3600
        y5=carStats2["start_time_min"]/3600

        pyplot.fill_between(x, y, color='gray', alpha=0.3, label='Max')
        pyplot.fill_between(x, y2, color='gray', alpha=0.4, label='75%')
        pyplot.fill_between(x, y3, color='gray', alpha=0.6, label='50%')
        pyplot.fill_between(x, y4, color='gray', alpha=0.8, label='25%')
        pyplot.fill_between(x, y5, color='black', alpha=0.2, label='Min')
        pyplot.legend(loc='upper right', fancybox=True, framealpha=0.5, fontsize=10)

        pyplot.ylabel('Arrival time [h]')

        pyplot.xticks(x, ['EV1', 'EV2', 'EV3', 'EV4', 'EV5', 'EV6', 'EV7', 'EV8', 'EV9', 'EV10'])
        pyplot.xticks(rotation='vertical')
        pyplot.yticks(np.arange(0, 26, 4))  # Set label locations.
        pyplot.savefig(figures_dir +"starttime-percentiles-10-cars.pdf", bbox_inches = "tight")


        
        # ================================================================================================================================================
        #plot starttime+percentiles (10 cars wieth more charging sessions)
        pyplot.figure("endtime-percentiles-10-cars")
        pyplot.rcParams['font.size'] = '16'
        x = carStats2["card_id"]
        y = carStats2["end_time_mean"]/3600
        
        pyplot.plot(x, y, color='r', label='Mean')
        locs2, labels2 = pyplot.xticks()

        x=np.arange(0,len(carStats2["card_id"]))

        y=carStats2["end_time_max"]/3600
        y2=carStats2["end_time_75"]/3600
        y3=carStats2["end_time_50"]/3600
        y4=carStats2["end_time_25"]/3600
        y5=carStats2["end_time_min"]/3600

        pyplot.fill_between(x, y, color='gray', alpha=0.3, label='Max')
        pyplot.fill_between(x, y2, color='gray', alpha=0.4, label='75%')
        pyplot.fill_between(x, y3, color='gray', alpha=0.6, label='50%')
        pyplot.fill_between(x, y4, color='gray', alpha=0.8, label='25%')
        pyplot.fill_between(x, y5, color='black', alpha=0.2, label='Min')
        pyplot.legend(loc='upper right', fancybox=True, framealpha=0.5, fontsize=10)

        pyplot.ylabel('Departure time [h]')

        pyplot.xticks(x, ['EV1', 'EV2', 'EV3', 'EV4', 'EV5', 'EV6', 'EV7', 'EV8', 'EV9', 'EV10'])
        pyplot.xticks(rotation='vertical')
        pyplot.yticks(np.arange(0, 26, 4))  # Set label locations.
        pyplot.savefig(figures_dir +"endtime-percentiles-10-cars.pdf", bbox_inches = "tight")


        # ================================================================================================================================================
        #plot starttime+percentiles (10 cars wieth more charging sessions)
        pyplot.figure("dwelltime-percentiles-10-cars")
        pyplot.rcParams['font.size'] = '16'
        x = carStats2["card_id"]
        y = carStats2["dwell_time_mean"]/3600
        
        pyplot.plot(x, y, color='r', label='Mean')
        locs2, labels2 = pyplot.xticks()

        x=np.arange(0,len(carStats2["card_id"]))

        y=carStats2["dwell_time_max"]/3600
        y2=carStats2["dwell_time_75"]/3600
        y3=carStats2["dwell_time_50"]/3600
        y4=carStats2["dwell_time_25"]/3600
        y5=carStats2["dwell_time_min"]/3600

        pyplot.fill_between(x, y, color='gray', alpha=0.3, label='Max')
        pyplot.fill_between(x, y2, color='gray', alpha=0.4, label='75%')
        pyplot.fill_between(x, y3, color='gray', alpha=0.6, label='50%')
        pyplot.fill_between(x, y4, color='gray', alpha=0.8, label='25%')
        pyplot.fill_between(x, y5, color='black', alpha=0.2, label='Min')
        pyplot.legend(loc='upper right', fancybox=True, framealpha=0.5, fontsize=10)

        pyplot.ylabel('Dwell time [h]')

        pyplot.xticks(x, ['EV1', 'EV2', 'EV3', 'EV4', 'EV5', 'EV6', 'EV7', 'EV8', 'EV9', 'EV10'])
        pyplot.xticks(rotation='vertical')
        pyplot.yticks(np.arange(0, 20, 4))  # Set label locations.
        pyplot.savefig(figures_dir +"dwelltime-percentiles-10-cars.pdf", bbox_inches = "tight")

        return carStats2