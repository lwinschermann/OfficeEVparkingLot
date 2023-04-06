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
import os
import math


""""
========================== convert statistical and real data into DEMKit input =========================
 """

# FIXME what about cars we don't have data for (ie no historical information). What does DEMKit do when planning for 2 cars and 1 doesnt come (ie empty real data?)

# class to generate txt file to use as input for DEMKit simulation. Similar layout as ALPG output. Supports multiple sessions per car. 
class DemkitSessions:
    def __init__(self, dataStats, dataReal, ScenarioPrefix):
        self.dataStats = dataStats
        self.dataReal = dataReal
        self.ScenarioPrefix = ScenarioPrefix

        # location to store txt files
        self.data_dir = os.path.join('output/')
        if not os.path.isdir(self.data_dir):
            os.makedirs(self.data_dir)

# function to create session input files for DEMkit. Suitable for start and end times of sessions, and energy requirements per session.
# rounding can have values {None, 'up', 'down', 'closest'}
# filePrefix is prefix of saved txt file in ALPG output format, one for real and one for estimated data with the file names 'fileprefix'_real.txt and 'fileprefix'_estimate.txt respectively
# staticDefault is what we take as the estimated input in case there is no historical data on an individual car
    def generateDemkitSessionInput(self, keyStats, keyReal, filePrefix, rounding = None, staticDefault = None, perCar=True):
        dataStats = self.dataStats
        dataReal = self.dataReal
        data_dir = self.data_dir

        filePrefix = filePrefix.format(self.ScenarioPrefix)

        cardIDs = pd.unique(dataReal['card_id'])
        carCount = 0
        #loop over cars in the real data set
        for car in cardIDs:
            #filter out the sessions per car and count number of sessions.
            tempReal = dataReal[dataReal['card_id'] == car]
            nSession = len(tempReal['card_id'])

            #start line in txt docs
            with open(data_dir+'{}_real.txt'.format(filePrefix), 'a') as f:
                f.writelines('{}:'.format(carCount))
            with open(data_dir+'{}_estimate.txt'.format(filePrefix), 'a') as g:
                g.writelines('{}:'.format(carCount))

            #introduce a mask for rounding values the way we indicated in the function input.
            if rounding is not None:
                if rounding == 'up':
                    correct = math.ceil
                if rounding == 'down':
                    correct = math.floor
                if rounding == 'closest':
                    correct = round
            else:
                correct = lambda x: x
                
            #loop over sessions of a certain car
            for sessionIndex in range(0,nSession):
                #put session value and a comma for both real and estimated files
                with open(data_dir+'{}_real.txt'.format(filePrefix), 'a') as f:
                    f.writelines('{},'.format(str(correct(tempReal[keyReal].iloc[[sessionIndex]].values[0])))) #Solved problem that indexing was returning errors due to mask. See https://stackoverflow.com/questions/46307490/how-can-i-extract-the-nth-row-of-a-pandas-data-frame-as-a-pandas-data-frame
                
                #if only interested in aggregated profile, and don't need to track individual EVs afterwards, model each session as seperate EV in DEMKit
                if not perCar:
                    try:
                        carEstimate = str(correct(dataStats[dataStats['card_id']==car][keyStats].values[sessionIndex]))
                        with open(data_dir+'{}_estimate.txt'.format(filePrefix), 'a') as g:
                            g.writelines('{},'.format(carEstimate))
                    except:
                        print('[EXCEPTION]: An exception occurred. Real data probably includes a car that is not in the training set, or else keyStats was not recognized. Defaulted carEstimate for this instance to staticDefault = {}.'.format(staticDefault))
                        with open(data_dir+'{}_estimate.txt'.format(filePrefix), 'a') as g:
                            g.writelines('{},'.format(staticDefault))
                    #remove last comma in line and start new line
                    with open(data_dir+'{}_real.txt'.format(filePrefix), 'a') as f:
                        f.seek(0,2)
                        f.truncate(f.tell()-1)
                        f.writelines('\n')
                    with open(data_dir+'{}_estimate.txt'.format(filePrefix), 'a') as g:
                        g.seek(0,2)
                        g.truncate(g.tell()-1)
                        g.writelines('\n')
                    carCount = carCount +1
                    if sessionIndex<nSession-1:
                        #start new line with new index. Only if multiple sessions
                        with open(data_dir+'{}_real.txt'.format(filePrefix), 'a') as f:
                            f.writelines('{}:'.format(carCount))
                        with open(data_dir+'{}_estimate.txt'.format(filePrefix), 'a') as g:
                            g.writelines('{}:'.format(carCount))
                #FIXME Having overlapping sessions does not work for DEMKit. Could try to put the estimation to each successive day, but to simulate multiple days, can just run the code multiple times and adapt time filters.
            #FIXME sanity check, need if perCar to be within session loop? Then tab right...
            if perCar:
                try:
                    carEstimate = str(correct(dataStats[dataStats['card_id']==car][keyStats].values[0]))
                    with open(data_dir+'{}_estimate.txt'.format(filePrefix), 'a') as g:
                        g.writelines('{},'.format(carEstimate))
                except:
                    print('[EXCEPTION]: An exception occurred. Real data probably includes a car that is not in the training set, or else keyStats was not recognized. Defaulted carEstimate for this instance to staticDefault = {}.'.format(staticDefault))
                    with open(data_dir+'{}_estimate.txt'.format(filePrefix), 'a') as g:
                        g.writelines('{},'.format(staticDefault))

                #remove last comma in line and start new line
                with open(data_dir+'{}_real.txt'.format(filePrefix), 'a') as f:
                    f.seek(0,2)
                    f.truncate(f.tell()-1)
                    f.writelines('\n')
                with open(data_dir+'{}_estimate.txt'.format(filePrefix), 'a') as g:
                    g.seek(0,2)
                    g.truncate(g.tell()-1)
                    g.writelines('\n')
                carCount = carCount +1
        #remove last redundant enter
        try:
            with open(data_dir+'{}_real.txt'.format(filePrefix), 'a') as f:
                f.seek(0,2)
                f.truncate(f.tell()-1)    
            with open(data_dir+'{}_estimate.txt'.format(filePrefix), 'a') as g:
                g.seek(0,2)
                g.truncate(g.tell()-1) 
            print(filePrefix, ' done')
        except:
            print(filePrefix, 'empty')    
        return

# slightly different format for generating EV specs (ie file with capacity and max charging power)
# rounding can have values {None, 'up', 'down', 'closest'}
# filePrefix is prefix of saved txt file in ALPG output format, one for real and one for estimated data with the file names 'fileprefix'_real.txt and 'fileprefix'_estimate.txt respectively
# staticDefault is what we take as the estimated input in case there is no historical data on an individual car
    def generateDemkitEVSpecsInput(self, keyStatsCap, keyStatsPower, keyRealCap, keyRealPower, filePrefix, rounding = None, staticDefaultPower = 7400, staticDefaultCapacity = 100000, perCar = True):
        dataStats = self.dataStats 
        dataReal = self.dataReal
        data_dir = self.data_dir

        filePrefix = filePrefix.format(self.ScenarioPrefix)
        cardIDs = pd.unique(dataReal['card_id'])
        
        #if we generate data per session, this suffices for the stats since capacity and max power are invariable over time (reasonable assumption)
        if not perCar:
            cardIDs = dataReal['card_id']
        
        carCount = 0
        #loop over cars in the real data set
        for car in cardIDs:
            #filter out the sessions per car and count number of sessions.
            tempReal = dataReal[dataReal['card_id'] == car]

            if rounding is not None:
                if rounding == 'up':
                    correct = math.ceil
                if rounding == 'down':
                    correct = math.floor
                if rounding == 'closest':
                    correct = round
            #for the estimated input, all sessions have the same values. We determine it once and then just copy it deterministically.
                if dataStats[dataStats['card_id']==car][keyStatsCap] is not None:
                    carEstimateCap = str(correct(dataStats[dataStats['card_id']==car][keyStatsCap].values[0]))
                else:
                    carEstimateCap = staticDefaultCapacity

                if dataStats[dataStats['card_id']==car][keyStatsPower] is not None:
                    carEstimatePower = str(correct(dataStats[dataStats['card_id']==car][keyStatsPower].values[0]))
                else:
                    carEstimatePower = staticDefaultPower

                with open(data_dir+'{}_real.txt'.format(filePrefix), 'a') as f:
                    f.writelines('{}:{},{}\n'.format(carCount,str(correct(tempReal[keyRealCap].iloc[[0]].values[0])),str(correct(tempReal[keyRealPower].iloc[[0]].values[0])))) #Solved problem that indexing was returning errors due to mask. See https://stackoverflow.com/questions/46307490/how-can-i-extract-the-nth-row-of-a-pandas-data-frame-as-a-pandas-data-frame
                with open(data_dir+'{}_estimate.txt'.format(filePrefix), 'a') as g:
                    g.writelines('{}:{},{}\n'.format(carCount,carEstimateCap,carEstimatePower))

            else:
                if dataStats[dataStats['card_id']==car][keyStatsCap] is not None:
                    carEstimateCap = str(dataStats[dataStats['card_id']==car][keyStatsCap].values[0])
                else:
                    carEstimateCap = staticDefaultCapacity

                if dataStats[dataStats['card_id']==car][keyStatsPower] is not None:
                    carEstimatePower = str(dataStats[dataStats['card_id']==car][keyStatsPower].values[0])
                else:
                    carEstimatePower = staticDefaultPower

                #put session value and a comma for both real and estimated files
                with open(data_dir+'{}_real.txt'.format(filePrefix), 'a') as f:
                    f.writelines('{}:{},{}\n'.format(carCount,str(tempReal[keyRealCap].values[0]),str(tempReal[keyRealPower].values[0]))) #Solved problem that indexing was returning errors due to mask. See https://stackoverflow.com/questions/46307490/how-can-i-extract-the-nth-row-of-a-pandas-data-frame-as-a-pandas-data-frame
                with open(data_dir+'{}_estimate.txt'.format(filePrefix), 'a') as g:
                    g.writelines('{}:{},{}\n'.format(carCount,carEstimateCap,carEstimatePower))
            carCount = carCount +1

        #remove last redundant enter
        try:
            with open(data_dir+'{}_real.txt'.format(filePrefix), 'a') as f:
                f.seek(0,2)
                f.truncate(f.tell()-1)
            with open(data_dir+'{}_estimate.txt'.format(filePrefix), 'a') as g:
                g.seek(0,2)
                g.truncate(g.tell()-1)  
            print(filePrefix, 'done')      
        except:
            print(filePrefix, 'empty')
        return