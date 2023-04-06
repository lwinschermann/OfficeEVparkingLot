# OfficeEVparkingLot
Data analysis tool for electric vehicle (EV) charging session data. 

Supports filtering, processing, statistical analysis, generating input for DEMKit software, swiping DEMKit output from an Influx database, and generating results/figures from that. 

Developed for EV charging session data collected at an office building parking lot in Utrecht, the Netherlands. 
Can be applied in a broader context, given that data is supplied in an Excel file with (at least) the proper headers. 

Code developed within SmoothEMS met GridShield project subsidized by the Dutch ministries of EZK and BZK (MOOI32005) - developed by Leoni Winschermann, University of Twente, l.winschermann@utwente.nl and Nataly Bañol Arias, University of Twente, m.n.banolarias@utwente.nl

Code is developed such that main.py runs filtering, processing, statistical analysis, generating input for DEMKit software. 
After that, simulations in DEMKit can be run independently. 
Then, secondary.py runs swiping DEMKit output from an Influx database, and generating results/figures from that. 

Structure is further included in the source files. 

Clustering approach not yet committed. Will be added soon.

## Commits corresponding to papers:
This code is used in on-going research and for transparency referred to in various publications. 
Here, we give an overview of which version of the code was used to produce the results of which publication.

Note that to run the code with the configurations corresponding to a publication, we keep various main.py and secondary.py files. 
Below we also specify the executable files that go with each publication.

Note further that in the initalization of FilteringData, the data sets are being read from csv files. 
Since data collection within the research project is on-going, the data set updates regularly, and the data corresponding to different papers is not necessarily the same. To reproduce exact results, carefully check the data input in the initialization of FilteringData.

### 80b246ce15e79a64bb1bce0439e4458cdc1df62f
[Under Review] Assessing the Value of Information for Electric Vehicle Charging Strategies at Office Buildings. (under review since Dec 2022).
Winschermann, Bañol Arias, Hoogsteen, Hurink

Files: main.py and secondary.py

### bc4dd80e7729da0f8a965711379dde1ca7cd445f
[Under Review] Integrating Guarantees and Veto-Buttons into the Charging of Electric Vehicles at Office Buildings. (under review since Apr 2023).
Winschermann, Hoogsteen, Hurink

Files: main_PHC.py and secondary_PHC.py
