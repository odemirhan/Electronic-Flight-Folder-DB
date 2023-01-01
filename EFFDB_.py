# -*- coding: utf-8 -*-


import pyodbc
import pandas as pd
from datetime import datetime, timedelta
import numpy as np
import sys


currentDT=datetime.now()
#currentDTm15=currentDT-timedelta(days=0)
currentDTm5=currentDT-timedelta(days=5)

Filename=datetime.strftime(currentDT, '%Y%m%d_%H%M')+"_CoreEFF.csv"


username=xxx
pwd=xxx
conn155=pyodbc.connect('Driver={SQL Server};'
                          'Server=xxx;'
                          'Database=xxx;'
                          'UID='xxx';PWD='xxx';'
                          )




conn36=pyodbc.connect('Driver={SQL Server};'
                          'Server=localhost;'
                          'Database=FuelMasterDB;'
                          'Trusted_Connection=yes;'
                          )


username=xxx
pwd=xxx
conn90=pyodbc.connect('Driver={SQL Server};'
                          'Server=xxx;'
                          'Database=xxx;'
                          'UID='xxx';PWD='xxx';'
                          )

FLTdb=pd.read_csv(r"C:\Users\Engineering\TURISTIK HAVA TASIMACILIK A.S\coreSafety - VERSION_03/FLT_dynamic.csv", usecols=["FlightID","LEGNO", "TOdateTime", "TDdateTime"])
LastValuesDB=pd.read_sql_query("Select * From dwh_ipadInputs_LastValues WHERE SummaryReceiveDT>?" , conn90, dtype=str, params=[currentDTm5])
SummaryDB=pd.read_sql_query("Select * From dwh_ipadInputs_Summary WHERE LastInputReceiveDT>?" , conn90, dtype=str, params=[currentDTm5])
NetlineDB=pd.read_sql_query("Select * From dwh_NetlineFlights WHERE STD>?" , conn90, dtype=str,  params=[currentDTm5])
conn90.close()

LastValuesDB=LastValuesDB.drop_duplicates(["IDSummary", "FieldName"])


Pivotted=LastValuesDB.pivot(index="IDSummary", columns="FieldName",values="FValue")
ftc=Pivotted.filter(regex="ftc")
rvsm=Pivotted.filter(regex="rvsm")
PivottedDF=Pivotted[Pivotted.columns.drop(ftc)]
PivottedDF=PivottedDF[PivottedDF.columns.drop(rvsm)]



MergedDF=PivottedDF.merge(SummaryDB, left_on="IDSummary", right_on="ID")
MergedDF2=MergedDF.merge(NetlineDB, left_on="NETLINEID", right_on="LEG_NO")

MergedDF2["NETLINEID"]=pd.to_numeric(MergedDF2["NETLINEID"], errors='coerce')

MergedDF2["offBlockTime"]=pd.to_numeric(MergedDF2["offBlockTime"], errors='coerce')

MergedDF2["onBlockTime"]=pd.to_numeric(MergedDF2["onBlockTime"], errors='coerce')
MergedDF2["takeOffTime"]=pd.to_numeric(MergedDF2["takeOffTime"], errors='coerce')
MergedDF2["landingTime"]=pd.to_numeric(MergedDF2["landingTime"], errors='coerce')


MergedDF2=MergedDF2.dropna(subset=["NETLINEID","offBlockTime", "onBlockTime","takeOffTime", "landingTime" ])


MergedDF2["offBlockTime"]=MergedDF2["offBlockTime"].astype(int)
MergedDF2["onBlockTime"]=MergedDF2["onBlockTime"].astype(int)
MergedDF2["takeOffTime"]=MergedDF2["takeOffTime"].astype(int)
MergedDF2["landingTime"]=MergedDF2["landingTime"].astype(int)


MergedDF2["offBlockTime"]=MergedDF2["offBlockTime"].apply(lambda x: x-2400 if x>=2400 else x)
MergedDF2["onBlockTime"]=MergedDF2["onBlockTime"].apply(lambda x: x-2400 if x>=2400 else x)
MergedDF2["takeOffTime"]=MergedDF2["takeOffTime"].apply(lambda x: x-2400 if x>=2400 else x)
MergedDF2["landingTime"]=MergedDF2["landingTime"].apply(lambda x: x-2400 if x>=2400 else x)



MergedDF2["offBlockTime"]=MergedDF2["offBlockTime"].astype(str)
MergedDF2["onBlockTime"]=MergedDF2["onBlockTime"].astype(str)
MergedDF2["takeOffTime"]=MergedDF2["takeOffTime"].astype(str)
MergedDF2["landingTime"]=MergedDF2["landingTime"].astype(str)



MergedDF2["offBlockTime"]=MergedDF2["offBlockTime"].str.zfill(4)
MergedDF2["onBlockTime"]=MergedDF2["onBlockTime"].str.zfill(4)
MergedDF2["takeOffTime"]=MergedDF2["takeOffTime"].str.zfill(4)
MergedDF2["landingTime"]=MergedDF2["landingTime"].str.zfill(4)

#MergedDF2["offBlockTime"]=MergedDF2["offBlockTime"].str[-2:].apply(lambda x: str(int(x)-60) if int(x)>=60 else x)
#MergedDF2["onBlockTime"]=MergedDF2["onBlockTime"].str[-2:].apply(lambda x: str(int(x)-60) if int(x)>=60 else x)
#MergedDF2["takeOffTime"]=MergedDF2["takeOffTime"].str[-2:].apply(lambda x: str(int(x)-60) if int(x)>=60 else x)
#MergedDF2["landingTime"]=MergedDF2["landingTime"].str[-2:].apply(lambda x: str(int(x)-60) if int(x)>=60 else x)


#MergedDF2=MergedDF2[MergedDF2["NETLINEID"]!=""]
#MergedDF2=MergedDF2[MergedDF2["offBlockTime"]!=""]

#MergedDF2=MergedDF2[MergedDF2["onBlockTime"]!=""]
#MergedDF2=MergedDF2[MergedDF2["takeOffTime"]!=""]
#MergedDF2=MergedDF2[MergedDF2["landingTime"]!=""]
 #MergedDF2=MergedDF2[MergedDF2['STD']>'2021-12-01']
                                    

MergedDF2["EFB_BlockOFFdum"]=pd.to_datetime(pd.to_datetime(MergedDF2['STD'], format='%Y-%m-%d').dt.date.astype(str)+" "+pd.to_datetime(MergedDF2['offBlockTime'], format='%H%M').dt.time.astype(str))
MergedDF2["STD"]=pd.to_datetime(MergedDF2["STD"], format='%Y-%m-%d %H:%M:%S')
#MergedDF=MergedDF2[MergedDF2["STD"]>currentDTm5]
MergedDF2["offBlockDiff"]=(MergedDF2["EFB_BlockOFFdum"]-MergedDF2["STD"]).dt.total_seconds()/3600
MergedDF2.loc[(MergedDF2["offBlockDiff"]>0) & (MergedDF2["offBlockDiff"]>=12), "EFB_BlockOFF"]=MergedDF2["EFB_BlockOFFdum"]+timedelta(days=-1)
MergedDF2.loc[(MergedDF2["offBlockDiff"]>=0) & (MergedDF2["offBlockDiff"]<12), "EFB_BlockOFF"]=MergedDF2["EFB_BlockOFFdum"]
MergedDF2.loc[(MergedDF2["offBlockDiff"]<0) & (MergedDF2["offBlockDiff"]<=-12), "EFB_BlockOFF"]=MergedDF2["EFB_BlockOFFdum"]+timedelta(days=1)
MergedDF2.loc[(MergedDF2["offBlockDiff"]<0) & (MergedDF2["offBlockDiff"]>-12), "EFB_BlockOFF"]=MergedDF2["EFB_BlockOFFdum"]



MergedDF2["EFB_TakeOffdum"]=pd.to_datetime(MergedDF2['EFB_BlockOFF'].dt.date.astype(str)+" "+pd.to_datetime(MergedDF2['takeOffTime'], format='%H%M').dt.time.astype(str))
MergedDF2.loc[MergedDF2["EFB_TakeOffdum"]<MergedDF2['EFB_BlockOFF'], "EFB_TakeOff"]=MergedDF2["EFB_TakeOffdum"]+timedelta(days=1)
MergedDF2.loc[MergedDF2["EFB_TakeOffdum"]>=MergedDF2['EFB_BlockOFF'], "EFB_TakeOff"]=MergedDF2["EFB_TakeOffdum"]



MergedDF2["EFB_Landingdum"]=pd.to_datetime(MergedDF2['EFB_TakeOff'].dt.date.astype(str)+" "+pd.to_datetime(MergedDF2['landingTime'], format='%H%M').dt.time.astype(str))
MergedDF2.loc[MergedDF2["EFB_Landingdum"]<MergedDF2['EFB_TakeOff'], "EFB_Landing"]=MergedDF2["EFB_Landingdum"]+timedelta(days=1)
MergedDF2.loc[MergedDF2["EFB_Landingdum"]>=MergedDF2['EFB_TakeOff'], "EFB_Landing"]=MergedDF2["EFB_Landingdum"]

#MergedDF2['onBlockTime']=MergedDF2['onBlockTime'].apply(lambda x: datetime.strptime(x, '%H%M') if type(x)==str else np.NaN)
MergedDF2["EFB_BlockONdum"]=pd.to_datetime(MergedDF2['EFB_Landing'].dt.date.astype(str)+" "+pd.to_datetime(MergedDF2['onBlockTime'], format='%H%M').dt.time.astype(str))
MergedDF2.loc[MergedDF2["EFB_BlockONdum"]<MergedDF2['EFB_Landing'], "EFB_BlockON"]=MergedDF2["EFB_BlockONdum"]+timedelta(days=1)
MergedDF2.loc[MergedDF2["EFB_BlockONdum"]>=MergedDF2['EFB_Landing'], "EFB_BlockON"]=MergedDF2["EFB_BlockONdum"]


MergedDF2["EFB_ActualblockfuelincltaxiKg"]=pd.to_numeric(MergedDF2["actualBlockFuelIncludingTaxi"], errors='coerce')
MergedDF2=MergedDF2.dropna(subset=["EFB_ActualblockfuelincltaxiKg"])
MergedDF2["EFB_ActualblockfuelincltaxiKg"]=MergedDF2["EFB_ActualblockfuelincltaxiKg"].astype(int)

MergedDF2["EFB_LandingFuelFromPrevFlightKg"]=pd.to_numeric(MergedDF2["landingFuelFromPreviousFlight"], errors='coerce')
MergedDF2=MergedDF2.dropna(subset=["EFB_LandingFuelFromPrevFlightKg"])
MergedDF2["EFB_LandingFuelFromPrevFlightKg"]=MergedDF2["EFB_LandingFuelFromPrevFlightKg"].astype(int)


MergedDF2["EFB_Density"]=pd.to_numeric(MergedDF2["density"], errors='coerce')
MergedDF2=MergedDF2.dropna(subset=["EFB_Density"])

MergedDF2["EFB_ActualFuelUpliftL"]=pd.to_numeric(MergedDF2["actualFuelUplift"], errors='coerce')
MergedDF2=MergedDF2.dropna(subset=["EFB_ActualFuelUpliftL"])
MergedDF2["EFB_ActualFuelUpliftL"]=MergedDF2["EFB_ActualFuelUpliftL"].astype(int)

MergedDF2["EFB_FuelOnBlocksKg"]=pd.to_numeric(MergedDF2["fuelOnBlocks"], errors='coerce')
MergedDF2=MergedDF2.dropna(subset=["EFB_FuelOnBlocksKg"])
MergedDF2["EFB_FuelOnBlocksKg"]=MergedDF2["EFB_FuelOnBlocksKg"].astype(int)


MergedDF2["EFB_Child"]=pd.to_numeric(MergedDF2["paxOnboardChild"], errors='coerce').replace({np.nan:0}).astype(int)
MergedDF2["EFB_PAD"]=pd.to_numeric(MergedDF2["paxOnboardPAD"], errors='coerce').replace({np.nan:0}).astype(int)
MergedDF2["EFB_Infant"]=pd.to_numeric(MergedDF2["paxOnboardInfant"], errors='coerce').replace({np.nan:0}).astype(int)
MergedDF2["EFB_paxOnboardFemale"]=pd.to_numeric(MergedDF2["paxOnboardFemale"], errors='coerce').replace({np.nan:0}).astype(int)
MergedDF2["EFB_paxOnboardMale"]=pd.to_numeric(MergedDF2["paxOnboardMale"], errors='coerce').replace({np.nan:0}).astype(int)
MergedDF2["EFB_Adult"]=(MergedDF2["EFB_paxOnboardMale"]+MergedDF2["EFB_paxOnboardFemale"]).astype(int)



MergedDF2["EFB_Adult"]=MergedDF2["EFB_Adult"].astype(int)

MergedDF2["EFB_TotalPax"]=MergedDF2["EFB_Adult"]+MergedDF2["EFB_Child"]
MergedDF2["legno"]=MergedDF2["LEG_NO"]


MergedDF2["Netline_Act_Off_Block_DAY"]=pd.to_datetime(MergedDF2['ATD'], format='%Y-%m-%d').dt.date.astype(str)
MergedDF2["Netline_Act_Off_Block_TIME"]=pd.to_datetime(MergedDF2['ATD'], format='%Y-%m-%d %H:%M:%S.%f').dt.hour.astype(str).str.zfill(2)+pd.to_datetime(MergedDF2['ATD'], format='%Y-%m-%d %H:%M:%S.%f').dt.minute.astype(str).str.zfill(2)


MergedDF2["Netline_Act_On_Block_DAY"]=pd.to_datetime(MergedDF2['ATA'], format='%Y-%m-%d').dt.date.astype(str)
MergedDF2["Netline_Act_On_Block_TIME"]=pd.to_datetime(MergedDF2['ATA'], format='%Y-%m-%d %H:%M:%S.%f').dt.hour.astype(str).str.zfill(2)+pd.to_datetime(MergedDF2['ATD'], format='%Y-%m-%d %H:%M:%S.%f').dt.minute.astype(str).str.zfill(2)


MergedDF2["legno"]=MergedDF2["legno"].astype(int)
MergedDF2.reset_index(inplace=True)
MergedDF2=MergedDF2.merge(FLTdb, left_on="legno", right_on="LEGNO", how="left")

MergedDF2["FDM_Takeoff"]=MergedDF2["TOdateTime"]
MergedDF2["FDM_Touchdown"]=MergedDF2["TDdateTime"]

try:
    MergedDF2["asrSent"].fillna(value='false', inplace=True )
except:
    MergedDF2["asrSent"]="false"
    
try:
    MergedDF2["BTO_PP"]=MergedDF2["parkPosition"].fillna(value='NIL')
except: 
    MergedDF2["BTO_PP"]="NIL"
    
try:
    MergedDF2["AL_PP"]=MergedDF2["parkingPosition"].fillna(value='NIL')
except: 
    MergedDF2["AL_PP"]="NIL"

try:
    MergedDF2["gpu"].fillna(value='false', inplace=True )
except: 
    MergedDF2["gpu"]="false"
try:
    MergedDF2["acu"].fillna(value='false', inplace=True )
except: 
    MergedDF2["acu"]="false"
    
try:
    MergedDF2["asu"].fillna(value='false', inplace=True )
except: 
    MergedDF2["asu"]="false"

try:
    MergedDF2["disruption"].fillna(value='N', inplace=True )
except:
    MergedDF2["disruption"]="N"
    
try:
    MergedDF2["flightType"].fillna(value='Line', inplace=True )
except:
    MergedDF2["flightType"]="Line"


MergedDF3=MergedDF2[(MergedDF2["EFB_BlockON"]-MergedDF2["EFB_BlockOFF"])>timedelta(hours=0)]
MergedDF3=MergedDF2[(MergedDF2["EFB_BlockON"]-MergedDF2["EFB_BlockOFF"])<timedelta(hours=13)]

MergedDF3=MergedDF2[(MergedDF2["EFB_Landing"]-MergedDF2["EFB_TakeOff"])>timedelta(hours=0)]
MergedDF3=MergedDF2[(MergedDF2["EFB_Landing"]-MergedDF2["EFB_TakeOff"])<timedelta(hours=10)]

try:
    SQLDF=MergedDF3[["legno","FlightID", "CP1","CP2","CP3", "executedSID", "executedSTAR", "feltTiredness", "lvoType", "pilotLanding", "pilotTakeOff", "reasonOfDifference", 
                 "asrSent","BTO_PP","AL_PP", "gpu", "acu", "asu","disruption", "flightType",
               "weatherOnApproach", "EFB_BlockOFF","EFB_TakeOff","EFB_Landing" , "EFB_BlockON", "EFB_ActualblockfuelincltaxiKg", "EFB_LandingFuelFromPrevFlightKg", "EFB_Density","EFB_ActualFuelUpliftL", 
                                  "EFB_FuelOnBlocksKg", "EFB_Adult", "EFB_Child","EFB_Infant", "EFB_PAD", "EFB_TotalPax",  "STD", "SDEP", "SARR", "SDEP_ICAO", "SARR_ICAO", "ADEP", "AARR", "ADEP_ICAO", "AARR_ICAO", "ACREG"]]
except KeyError as e:
    handler=e.args[0]
    handler=(((handler.replace("[","")).replace("]","")).replace("'","")).split(" ")
    handler=handler[0]
    MergedDF3[handler]=""
    SQLDF=MergedDF3[["legno","FlightID", "CP1","CP2","CP3", "executedSID", "executedSTAR", "feltTiredness", "lvoType", "pilotLanding", "pilotTakeOff", "reasonOfDifference", 
                 "asrSent","BTO_PP","AL_PP", "gpu", "acu", "asu","disruption", "flightType",
               "weatherOnApproach", "EFB_BlockOFF","EFB_TakeOff","EFB_Landing" , "EFB_BlockON", "EFB_ActualblockfuelincltaxiKg", "EFB_LandingFuelFromPrevFlightKg", "EFB_Density","EFB_ActualFuelUpliftL", 
                                  "EFB_FuelOnBlocksKg", "EFB_Adult", "EFB_Child","EFB_Infant", "EFB_PAD", "EFB_TotalPax",  "STD", "SDEP", "SARR", "SDEP_ICAO", "SARR_ICAO", "ADEP", "AARR", "ADEP_ICAO", "AARR_ICAO", "ACREG"]]

    
    
    
    
SQLDF = SQLDF.where(pd.notnull(SQLDF), None)
#SQLDF=SQLDF.dropna(subset=["FlightID"])
SQLDF["FlightID"]=SQLDF["FlightID"].astype('Int64')
SQLDF["FlightID"]=SQLDF["FlightID"].astype(str).str.replace("<NA>", '')


FinalDF=MergedDF3[["EFB_BlockOFF","EFB_TakeOff","EFB_Landing" , "EFB_BlockON", "EFB_ActualblockfuelincltaxiKg", "EFB_LandingFuelFromPrevFlightKg", "EFB_Density","EFB_ActualFuelUpliftL", 
                   "EFB_FuelOnBlocksKg", "EFB_Adult", "EFB_Child","EFB_Infant", "EFB_PAD", "EFB_TotalPax", "legno","Netline_Act_Off_Block_DAY", "Netline_Act_Off_Block_TIME", "Netline_Act_On_Block_DAY", "Netline_Act_On_Block_TIME",
                   "FDM_Takeoff","FDM_Touchdown"]]




logdf=pd.concat([MergedDF3,MergedDF2]).drop_duplicates(keep=False)


cur=conn36.cursor()

for cntSQL in range(len(SQLDF)):
    SQLlist=SQLDF.iloc[cntSQL, :].tolist()
    
    cur.execute("SELECT * from dbo.CoreEFF_DWH WHERE legno=? ", int(SQLlist[0]))
    ftcur=cur.fetchone()
    if not ftcur:
        try:
            
            list2=[int(SQLlist[0])]+SQLlist[1:25]+[int(SQLlist[25])]+[int(SQLlist[26])]+[SQLlist[27]]
            for cntlist in range(28,35):
                list2=list2+[int(SQLlist[cntlist])]
            list2=list2+SQLlist[35:45]
            cur.execute("""INSERT INTO dbo.CoreEFF_DWH VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                      list2)
            conn36.commit()
        except Exception as E:
             print(E)
     
             pass  
        
    else:
        if not (SQLlist[1] is None):
            try:
                list3=[SQLlist[1]]+[int(SQLlist[0])]
                cur.execute("""UPDATE dbo.CoreEFF_DWH SET FID=? WHERE legno=?""",
                      list3)
                conn36.commit()
            except Exception as E:
                print(E)
        
                pass  


FinalDF.to_csv(r"\\192.168.3.12\ifs/" +Filename, sep=";", index=False)
logdf.to_csv(r"C:\Users\Engineering\TURISTIK HAVA TASIMACILIK A.S\Gökmen Düzgören - FOE_2019\phyton\db_python\CoreEFFDB\logs_notincluded/" +Filename, sep=";", index=False)

conn36.close()
conn155.close()
