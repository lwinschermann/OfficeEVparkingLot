#    Data analysis in OfficeEVparkingLot
#    Filter, process and analyze EV data collected at Dutch office building parking lot
#    Survey analysis of EV data at ASR facilities - GridShield project - developed by 
#    Leoni Winschermann, University of Twente, l.winschermann@utwente.nl
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

# %% Upload Libraries
import pandas as pd
import datetime as dt
import os
import numpy as np
import re
import math
from matplotlib import pyplot as plt

""""
========================== Filtering Data =========================
"""

class SurveyData:
    def __init__(self,skipClean=False,skipProcess=False):
        #import data from Excel sheet
        self.dataFiles = ['20221129GS_PHC_survey_data_processed.csv']
        self.figures_dir = os.path.join('/Analysis/Figures/')
        if not os.path.isdir(self.figures_dir):
            os.makedirs(self.figures_dir)
        if skipClean:
            self.rawData = pd.read_excel('surveyCleaned.xlsx')
        if skipProcess:
            self.rawData = pd.read_excel('surveyProcessed.xlsx')

    def clean_data(self):#, startTimeFilter = True, afterStartDate = dt.datetime(2021, 1, 1, 0, 0), beforeEndDate = dt.datetime(2021, 12, 31, 23, 59), energyFilter = True, energyCutOff = 0, defaultCapacity = None, defaultPower = None, maxDwellTime = None, minDwellTime = None, managersFilter = None,  listmanagersFilter = None, idFilter = None):
        dataFiles = self.dataFiles
        
        # Data has columns: [,Status,IPAddress,Progress,Duration (in seconds),Finished,RecordedDate,ResponseId,RecipientLastName,RecipientFirstName,RecipientEmail,ExternalReference,LocationLatitude,LocationLongitude,DistributionChannel,UserLanguage,Q6 Text entry,Q16,Q17,Q18,Q19,Q19_4_TEXT,Q20,Q8,Q27,Q37_1,Q37_5,Q9_1,Q29_1,Q18_1,Q18_2,Q15_2,Q15_3,Q16]	
        self.rawData = pd.read_csv(dataFiles[0])
        #Drop rows with meta data that is irrelevant to our analysis
        self.rawData = self.rawData.drop(labels=[0,1], axis=0)

        #Filter out answers that are most likely automatic sweeps (6% completed)
        self.rawData = self.rawData.drop(self.rawData[self.rawData["Progress"].astype(int)<50].index)
        self.rawData.reset_index(drop=True,inplace=True)

        #Drop rows and columns with meta data that is irrelevant to our analysis
        self.rawData = self.rawData.drop(labels=["StartDate", "EndDate", "Status", "IPAddress", "Progress", "Duration (in seconds)", "Finished", "RecordedDate", "ResponseId", "RecipientLastName", "RecipientFirstName","RecipientEmail","ExternalReference","LocationLatitude","LocationLongitude","DistributionChannel","UserLanguage"], axis=1)

        #remove '-' from EV tags
        self.rawData['Q6 Text entry'] = self.rawData['Q6 Text entry'].apply(lambda x: str(x).replace("-", ""))
        self.rawData['Q6 Text entry'] = self.rawData['Q6 Text entry'].apply(lambda x: str(x).replace("=", ""))

        #after clean, manually copy this file and remove nonsenical answers.
        with open(r'surveyCardIdRaw.txt', 'w') as f:
            for item in self.rawData['Q6 Text entry'].unique():
                f.write("%s\n"  % item)
        
        #after clean, manually copy file and make answers purely numerical. 
        #If not nonsensible, kW was interpreted as kWh, assumed to be input errors.
        #The following exceptions to adapting the direct answers were made, based on EV brand and model:
        #TeslaModel 3 Standard Range,340km --> 50kWh https://en.wikipedia.org/wiki/Tesla_Model_3#Specifications accessed 1.12.2022 10:22 CET
        #bmwi4,kunt u op google opzoeken, weet ik niet uit mijn hoofd --> 80.7 or 67.0 kWh https://ev-database.org/cheatsheet/useable-battery-capacity-electric-car accessed 1.12.2022 10:23 CET 80.7 https://ev-database.org/car/1252/BMW-i4-eDrive40 accessed 1.12.2022 12:30 CET 
        #MitsubishiOutlander PHEV,12Kwh ( waarvan 8 kwh beschikbaar voor het rijden) --> 8kWh
        #Tesla Model 3, geen idee --> 57.5 or 75 https://ev-database.org/cheatsheet/useable-battery-capacity-electric-car accessed 1.12.2022 10:27 CET
        #TeslaModel 3,366 kwh --> 75 https://ev-database.org/car/1591/Tesla-Model-3-Long-Range-Dual-Motor accessed 1.12.2022 11:30 CET has 366 kW power
        #TeslaModel 3 LR,82 kWh bruto; 75 kWh netto --> 75kWh
        #Tesla Model 3,470  (theorie 600) --> 76 https://ev-database.org/car/1322/Tesla-Model-3-Performance accessed 1.12.2022 11:32 CET real range 470
        #VolvoCX40,40 km https://www.volvocars.com/nl/build/xc40-hybrid accessed 1.12.2022 11:39 CET 46km range, 15.4kWh/100km ~ 6.16. Respondent said 40km range, so round down to 6kWh
        #MGZS EV,'+/- 250 km --> 49 https://ev-database.org/car/1540/MG-ZS-EV-Standard-Range accessed 1.12.2022 11:48 CET
        #Volkswagen ID3,340 km --> 58 https://ev-database.org/car/1531/Volkswagen-ID3-Pro or https://ev-database.org/car/1532/Volkswagen-ID3-Pro-Performance accessed 1.12.2022 11:51 CET
        #TeslaTesla 3,Dual Motor capaciteit --> 75
        #NissanLeaf,weet niet --> 39 https://ev-database.org/cheatsheet/useable-battery-capacity-electric-car accessed 1.12.2022 11:52 CET
        #VolvoV60 hybride,Geen idee --> 11.2 https://www.volvocars.com/en-om/support/manuals/v60-plug-in-hybrid/2015w17/specifications/specifications/hybrid-battery---specification or https://www.volvocars.com/mt/support/manuals/v60-twin-engine/2016w17/specifications/specifications/hybrid-battery---specification accessed 1.12.2022 11:55 CET
        #MGMarvel,400 km --> 65 https://ev-database.org/cheatsheet/useable-battery-capacity-electric-car accessed 1.12.2022 11:57 CET
        #KiaNiro EV,440 --> 64.8 https://ev-database.org/cheatsheet/useable-battery-capacity-electric-car accessed 1.12.2022 11:58 CET
        #Kia Niro EV,150KW  --> 64.8 https://ev-database.org/car/1666/Kia-Niro-EV accessed 1.12.2022 12:28 CET (based on specified battery power of 150 kW) 1.32 or 11.1 or 64.8kWh https://www.kia.com/nl/modellen/niro/ontdekken/ accessed 1.12.2022 11:20 CET
        #FordMustang Mach-E,325 km range --> 70 https://ev-database.org/cheatsheet/useable-battery-capacity-electric-car accessed 1.12.2022 12:21 CET
        #PeugeotE2008,Opladen tot 280km --> 45 https://ev-database.org/car/1584/Peugeot-e-2008-SUV acccessed 1.12.2022 12:22 CET
        #HyundaiKona,300 --> 39.2kWh : 39.2 (range 250km) or 64 (range 395km)  https://ev-database.org/cheatsheet/useable-battery-capacity-electric-car accessed 1.12.2022 12:24 CET
        #VolkswagenE-Golf,? --> 32 https://ev-database.org/car/1087/Volkswagen-e-Golf accessed 1.12.2022 12:26 CET
        #audiQ4 etron,dat weet ik niet --> ? 52 or 76.6 https://ev-database.org/cheatsheet/useable-battery-capacity-electric-car accessed 1.12.2022 13:16 CET
        #Audi A3 etronHybride, 30 km --> 8.8kWh https://www.car.info/en-se/audi/a3/a3-5-door-dct6-2020-19330699/specs accessed 5.1.20233 15:02 CET
        #additionally for efficiency values: default source https://ev-database.org/cheatsheet/energy-consumption-electric-car accessed 5.1.2023
        #VolvoS60, 85kWh --> 140 Wh/km highway, 168 Wh/km city. Volvo S60 doesn't exist as fully electric. Assumed that 85 as inputted by respondent is a km estimate. 11.6kWh https://www.meinauto.de/testberichte/volvo-s60-plug-in-hybrid-im-test-2020-21-der-schweden-mittelklassler-stuermt-voran accessed 5.1.2023 11:25 CET https://www.volvocars.com/nl/cars/s60-hybrid/ accessed 5.1.2023 11:39 CET https://www.volvocars.com/en-th/support/manuals/s60-recharge-plug-in-hybrid/2021w22/maintenance-and-service/battery/hybrid-battery accessed 5.1.2023 11:43 CET
        #Tesla Model 3 without indication of SR/LR assumed SR efficiency in optimistic, Performance in LR
        #FordKuga PHEV 2021 --> 270Wh/km https://www.adac.de/rund-ums-fahrzeug/autokatalog/marken-modelle/ford/ford-kuga/#:~:text=Der%20reine%20Stromverbrauch%20liegt%20hier,%2C4%20l%2F100%20km. accessed 5.1.2023 12:03 CET
        #Bmw 530e, 10 --> 131 Wh/km https://www.autoweek.nl/auto/90860/bmw-530e-iperformance/ accessed 5.1.2023 13:30 CET
        #MitsubishiOutlander PHEV 8 --> 134Wh/km https://www.autoweek.nl/carbase/mitsubishi/outlander/ accessed 5.1.2023 13:41 CET
        #Lunc01, 17.6 --> niet bekend https://www.autoweek.nl/auto/102909/lynk-co-01/ accessed 5.1.2023 13:44 CET
        #MercedesGLC, 14 --> 139 Wh/km https://www.autoweek.nl/auto/87532/mercedes-benz-glc-350-e-4matic/ accessed 5.1.2023 14:15 CET
        #Tesla S-75D, 75 --> 148 https://www.autoweek.nl/auto/87847/tesla-model-s-75d/ accessed 5.1.2023 14:19 CET
        #VW Passat GTE Hybride, 7 --> 63 Wh/km https://www.anwb.nl/auto/tests-en-specificaties/detail/volkswagen/passat/specificaties/b4ba39f2-f6e1-4475-81fb-9fb2755a6ec9 accessed 5.1.2023
        #BMWX1xDrive25e (hybride), 10kW --> 135 https://www.autoweek.nl/auto/99919/bmw-x1-xdrive25e/ accessed 5.1.2023 15:08 CET
        self.rawData['EV type'] = self.rawData['Q16'] + self.rawData['Q17']
        with open(r'surveyEVtypeRaw.txt', 'w') as g:
            #with open(r'surveyEVstatsRaw.txt', 'w') as s:
                for idx,row in enumerate(self.rawData['EV type']):
                    #s.write("{}\n".format(self.rawData.iloc[idx]['Q18']))
                    g.write("{},{}\n".format(self.rawData.iloc[idx]['EV type'],self.rawData.iloc[idx]['Q18']))

        #save to Excel. Can simply read after first initialization.
        self.rawData.to_excel("surveyCleaned.xlsx")
        
        return self.rawData

    def process_data(self):
        
        cardIDs_replace = pd.read_csv('surveyCardIdRaw_replace.txt',header=None)
        cardIDs_replace.columns = ['card_id']
        #make IDs all capital letters
        cardIDs_replace['card_id'] = cardIDs_replace['card_id'].apply(lambda x: str(x).upper())#.split(' EN '))
        #cardIDs_replace['card_id'][cardIDs_replace['card_id'] == ['X']] = [np.nan]

        #replace IDs in data by processed IDs
        cardIDs_dict = pd.Series(cardIDs_replace['card_id'].values, index = self.rawData['Q6 Text entry'].unique()).to_dict()
        self.rawData.replace({"Q6 Text entry": cardIDs_dict}, inplace = True)

        #one respons specified two IDs. Splitting those now.
        self.rawData['card_id'] = self.rawData['Q6 Text entry'].apply(lambda x: str(x).split(' EN '))

        #processed capacity
        self.rawData['capacity_kwh'] = pd.read_csv('surveyEVtypeRaw_replace.txt',header=None)
        self.rawData['capacity_kwh'] = self.rawData['capacity_kwh'].apply(lambda x: str(x).split(' en '))
        self.rawData['capacity_kwh'] = self.rawData['capacity_kwh'].apply(lambda x: [np.nan if i=='x' else float(i) for i in x])

        #processed efficiency
        self.rawData['efficiency_wh_per_km'] = pd.read_csv('surveyEfficiencyRaw_replace_conservative.txt',header=None) #made a conservative and optimistic file with efficiency estimates. Assume the conservative one.
        self.rawData['efficiency_wh_per_km'] = self.rawData['efficiency_wh_per_km'].apply(lambda x: str(x).split(' en '))
        self.rawData['efficiency_wh_per_km'] = self.rawData['efficiency_wh_per_km'].apply(lambda x: [np.nan if i=='x' else float(i) for i in x])

        #remove text from commute answers
        self.rawData['commute_km'] = self.rawData['Q20'].apply(lambda x: re.sub(r'[^0-9]', '', x))
        #two responses without answer. Use same format as with capacity_kWh and card_ids.
        self.rawData.loc[self.rawData['commute_km'] == '','commute_km'] = np.nan #'x'

        #save to Excel. Can simply read after first initialization.
        self.rawData.to_excel("surveyProcessed.xlsx")

        self.rawData['Q6 Text entry'] = self.rawData['Q6 Text entry'].apply(lambda x: str(x).replace("-", ""))

        self.cardIDs_unique = pd.DataFrame(pd.Series(sum(self.rawData['card_id'].tolist(),[]),index=None).unique(),columns=['ids'])

        return

    def doubleDeal(self):
        #this function deals with responses that have multiple cars. Sometimes, we want to make that two sets of responses, while others, we need answers per user. 
        self.rawData['user'] = self.rawData.index
        dealing = []
        capacity_index = self.rawData.columns.get_loc('capacity_kwh')
        card_index = self.rawData.columns.get_loc('card_id')
        efficiency_index = self.rawData.columns.get_loc('efficiency_wh_per_km')

        for user in self.rawData.index:
            for instance in range(0,len(self.rawData.loc[user,'capacity_kwh'])):
                temp = self.rawData.loc[user].values.tolist()
                temp[capacity_index] = temp[capacity_index][instance]
                temp[card_index] = temp[card_index][instance]
                temp[efficiency_index] = temp[efficiency_index][instance]
                dealing.append(temp)
        self.dealing = pd.DataFrame(dealing, columns = self.rawData.columns)

    def commuteEnergy(self,efficiencyDefault):
        self.dealing['efficiencyDefault_wh_per_km'] = efficiencyDefault
        self.dealing['commuteEffDefault_wh'] = self.dealing['efficiencyDefault_wh_per_km']*self.dealing['commute_km'].astype(float)
        self.dealing['commute_wh'] = self.dealing['efficiency_wh_per_km']*self.dealing['commute_km'].astype(float)

    def basicFigures(self):
        figures_dir = self.figures_dir

        #scatter plot
        plt.figure("scatter plot commute vs capacity")
        plt.scatter(self.dealing['commute_km'].astype(float), self.dealing['capacity_kwh'].astype(float))
        plt.xlabel("One-way commute" + " [km]")
        plt.ylabel("Battery capacity" + " [kWh]")
        plt.legend()
        plt.grid()
        plt.show()
        plt.savefig(figures_dir +"scatter_commute_capacity.pdf")
        plt.close()

        #pie chart
        plt.figure("pie chart charging locations")
        location_freq = [np.sum(self.rawData['Q37_1'].value_counts()) ,np.sum(self.rawData['Q9_1'].value_counts()), np.sum(self.rawData['Q29_1'].value_counts())]
        plotLabels = ['home and public', 'home only', 'public only']
        plt.pie(location_freq,labels=plotLabels)
        plt.title("Responses about charging pole availability off-PL")
        plt.legend(title = "Total of {} responses".format(np.sum(location_freq)))
        plt.show()
        plt.savefig(figures_dir +"pie_offPLlocation.pdf")
        plt.close()

        #histogram
        plt.figure("histogram charging frequency office")
        plt.hist([self.rawData['Q18_1'].astype(float),self.rawData['Q18_2'].astype(float)], label = ['days coming to office', 'days charging at office'])
        plt.xlabel("Frequency [d]")
        plt.ylabel("Responses [-]")
        plt.title("Responses about charging at office")
        plt.legend()
        plt.show()
        plt.savefig(figures_dir +"hist_officeCharging.pdf")
        plt.close()

        #scatter plot
        plt.figure("scatter plot min vs max acceptable soc")
        plt.scatter(self.rawData['Q15_2'].astype(float), self.rawData['Q15_3'].astype(float))
        plt.xlabel("Minimum acceptable SoC" + " [%]")
        plt.ylabel("Maximum acceptable SoC" + " [%]")
        plt.legend()
        plt.grid()
        plt.show()
        plt.savefig(figures_dir +"scatter_acceptableSOC.pdf")
        plt.close()

        # plot boxes diagram
        plt.figure("box min SOC")
        plt.boxplot(self.rawData['Q15_3'].astype(float))#, vert = False)
        plt.xlabel('Energy charged [kWh]')
        plt.show()
        #plt.savefig(figures_dir +"box_energy.pdf")
        plt.close()

        ##histogram
        #plt.figure("histogram charging frequency office")
        #plt.hist([self.rawData['Q18_1'].astype(float),self.rawData['Q18_2'].astype(float)], label = ['days coming to office', 'days charging at office'])
        #plt.xlabel("Frequency [d]")
        #plt.ylabel("Responses [-]")
        #plt.title("Responses about charging at office")
        #plt.legend()
        #plt.show()
        #plt.savefig(figures_dir +"hist_officeCharging.pdf")
        #plt.close()
        
        print('average amount of days coming to office: ',np.mean(self.rawData['Q18_1'].astype(float)))
        print('average amount of days charging at office: ',np.mean(self.rawData['Q18_2'].astype(float)))

        ##pie chart
        #plt.figure("pie chart charging frequency home")
        ##plt.pie((self.rawData['Q37_1']+self.rawData['Q9_1']).value_counts(normalize=False))
        #plotLabels = self.rawData['Q37_1'].unique()[1:]
        #plt.pie((self.rawData['Q37_1']).value_counts(normalize=False),labels=plotLabels)
        #plt.title("Frequency charging at home")
        #plt.legend(title = "Total of {} only charging at home".format(89-self.rawData['Q37_1'].isna().sum()))
        #plt.show()
        ##plt.savefig(figures_dir +"pie_frequency_{}.pdf")
        #plt.close()
    #LDC
    #plt.figure(measure)
    #for case in range(0,len(coreCases)):
    #    plt.plot(makeEnsLDC(cs['{}{}'.format(coreCases[case],measure)], divisor = 1/100), label = coreNames[case], linewidth=2.5)
    #plt.xlabel("individual EVs [-]")
    #plt.ylabel("{} [%]".format("Energy not served"))
    #plt.ylim(0,101)
    #plt.legend(loc = 1)
    #plt.grid()
    #plt.show()
    ##plt.savefig(figures_dir + measure + ".pdf")
    #plt.close()


    #Give EVs unique identifiers. 
    def anonymize(self,otherData):
        IDkey = pd.DataFrame(pd.unique(self.cardIDs_unique['ids'].append(otherData['card_id'])), columns=["card_id"])
        IDkey['EV_id'] = np.nan
        IDkey['EV_id_digit'] = np.nan
        i = 0
        for id in IDkey['card_id']:
            IDkey.loc[(IDkey['card_id'] == id)&(id!='X'),'EV_id'] = 'EV' + str(i)
            IDkey.loc[(IDkey['card_id'] == id)&(id!='X'),'EV_id_digit'] = int(i)
            i += 1
        self.IDkey_dict = pd.Series(IDkey['EV_id'].values, index = IDkey['card_id']).to_dict()
        
        IDkey.to_excel("IDkey.xlsx")
        #add entries to filteredData
        anonymized = otherData.merge(IDkey, how='left', on='card_id')
        self.dealing = self.dealing.merge(IDkey, how='left', on='card_id')
        mergedData = anonymized.merge(self.dealing[['card_id','capacity_kwh','efficiency_wh_per_km','commute_km','user','commute_wh','commuteEffDefault_wh']],how='left',on='card_id')

        # FIXME count frequency of EV in mergedData and save in dealing

        return anonymized, mergedData
        #FIXME how to deal with double EVs = one user multiple cars?!?! This only works after applying doubleDeal(). So currently not per user but per card_id.
         
    # veto_hc: if we know home commute smart, if not veto
    # veto_energy: if realized energy demand > energyThreshold % of mean, veto
    # veto_dwell: if stay < y hours, veto
    def veto(self,mergedData,energyThreshold=1.1,dwellThreshold = 6*3600):
        mergedData['veto_hc'] = mergedData['commute_km'].apply(lambda x: 1 if float(x)>0 else 0)
        temp = mergedData['total_energy']*1000 - mergedData['energy_mean']*energyThreshold
        mergedData['veto_energy'] = temp.apply(lambda x: 1 if x<0 else 0)
        mergedData['veto_dwell'] = mergedData['dwell_time_seconds'].apply(lambda x: 1 if float(x)>dwellThreshold else 0)
        return mergedData

    #function allows to combine two data series based on a binary.
    #use it for the veto-button analysis. E.g. the target energy to be provided is either the home commute energy or greedy energy requirement, depending on whether smart or fast charging is chosen.
    def hybridData(self,data,key,binaryKey,oneKey,zeroKey):
        data[key] = data[oneKey].apply(lambda x: x if float(x)>0 else 0) *data[binaryKey] + data[zeroKey]*(1-data[binaryKey])
        return data

    def sample(self, mergedData, n=400, smart = 'commuteEffDefault_wh'):
        #data points with home commute non-empty
        sampleDataSmart = mergedData[mergedData[smart] > 0 ]
        #data points with home commute empty (by taking complement)
        sampleDataFast = mergedData[~mergedData.index.isin(sampleDataSmart.index)]

        #account for situation where data set might be too small to sample n data points
        nSmart = min(len(sampleDataSmart),n)
        nFast = min(len(sampleDataFast),n)
        if min(nSmart,nFast) < n:
            print('[MESSAGE:] data too small to sample n = {} for both smart and fast charging instances.\nSmart sample size = {}\nFast sample size = {}'.format(n,nSmart,nFast))
        
        #sample n last data points
        sampleDataSmart = sampleDataSmart[-nSmart:]
        sampleDataFast = sampleDataFast[-nFast:]

        return sampleDataSmart, sampleDataFast 

    #make a sample EV population with percentSmart EVs that have home commute information and 100-percentSmart that veto smart charging
    def sampleCombined(self,sampleDataSmart,sampleDataFast,percentSmart=10,n=400):
        nSmart = math.ceil(n*percentSmart/100)
        nFast = math.floor(n*(100-percentSmart)/100)
        sampleCombined = sampleDataSmart[-nSmart:].append(sampleDataFast[-nFast:],ignore_index = True)

        #if percentSmart == 0 or 100, df[-n:] selects the entire data set. Get 800 samples then. We check for that and redefine sampleCombined if applicable
        if nSmart == 0:
            sampleCombined = sampleDataFast
        if nFast == 0:
            sampleCombined = sampleDataSmart 
        #FIXME doesn't account for instances where one of dataframes has less than n entries. 
        return sampleCombined


# %%
