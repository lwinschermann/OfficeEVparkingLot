# OfficeEVparkingLot
Data analysis tool for electric vehicle (EV) charging session data. 

Supports filtering, processing, statistical analysis, generating input for DEMKit software, swiping DEMKit output from an Influx database, and generating results/figures from that. 

Developed for EV charging session data collected at an office building parking lot in Utrecht, the Netherlands. 
Can be applied in a broader context, given that data is supplied in an Excel file with (at least) the proper headers. 

Code developed within GridShield project - developed by Leoni Winschermann, University of Twente, l.winschermann@utwente.nl and Nataly Bañol Arias, University of Twente, m.n.banolarias@utwente.nl

Code is developed such that main.py runs filtering, processing, statistical analysis, generating input for DEMKit software. 
After that, simulations in DEMKit can be run independently. 
Then, secondary.py runs swiping DEMKit output from an Influx database, and generating results/figures from that. 

Structure is further included in the source files. 

Clustering approach not yet committed. Will be added soon.
