#!/usr/bin/python3

import pandas as pd
from datetime import datetime

"""
A processor using the Pandas framework to manipulate
Well processing information and allow it to be easily
viewed in PowerBI

"""
pd.options.mode.chained_assignment = None  # default='warn'

dropNoRig = True

#"dataset" would be the var used to store data in PBI
#remove this line in production
dataset = pd.read_csv('WTExport200000.csv')

if dropNoRig:
    dataset.dropna(subset = ['RigName'], inplace=True, axis=0, how='any')
else:
    dataset['RigName'].fillna(value='none',inplace=True)

#Drop rows with start, end, top or bottom as NULL or NAN
dataset.dropna(subset = ['StartDate', 'EndDate','TopDepth','BottomDepth'], inplace=True, axis=0, how='any')


def testDS():

    """
      Several functions to test the successful loading of the sample 
      data - can de disgarded on production deployement.

    """

    #Test load
    print(dataset.info())
    #print(dataset['ProjectName'].unique().tolist())
    #print(dataset['ProjectName'].value_counts())
    #print(dataset['Project_Guid'].value_counts())
    #activ = dataset['Activity'].value_counts()
    #for a,b in activ.iteritems():
    #    print(a,b)

def defineProjects():

    """
       Drop duplicates, returning only the first records with unique
       WellTrak Project GUID, WellName and RigName 

       The purpose of this is to return a set of records that is a record
       of every Well monitored by the Welltrak System

       Returns: Pandas DataFrame

    """

    IDS = dataset.drop_duplicates(subset = ['Welltrak_Project_Guid','WellName','RigName'], keep ='first', inplace = False)

    #Drop unused cols
    IDS.drop(['JobName', 'FinalReportFlag','Borehole','Phase','StartDate','EndDate','Duration(Days)',
    'Start_Day_Number','Activity','SubActivity','TopDepth','BottomDepth','TimeClassification','Planned_Flag'], axis=1,inplace=True)
    IDS.drop(['NPTVendor', 'Global Name','NPTCategory','NPTSubCategory','Scope','Bit_Serial_Number','WE','WSS','AWSS','WSC','Skipped_Flag'], axis=1,inplace=True)

    IDS.sort_values(by=['ProjectName','RigName','WellName'],inplace=True)

    #Calcuate the initial well stats
    for index, row in IDS.iterrows():
        #print('Getting Stats for: ',row['ProjectName'],' | ',row['RigName'],' | ',row['WellName'])

        nptDays = calculateNPTStats(project=row['ProjectName'],rig=row['RigName'],well=row['WellName'])

        stat = calculateWellStats(project=row['ProjectName'],rig=row['RigName'],well=row['WellName'])

        print('Setting Stats for: ',row['ProjectName'],' | ',row['RigName'],' | ',row['WellName'])

        if stat is None:
            print('***NULL ROW***')
            IDS = IDS.drop(index, axis=0)
        else:
            IDS.at[index,'PYmaxEndDT'] = stat['maxEndDT']
            IDS.at[index,'PYminStartDT'] = stat['minStartDT']
            IDS.at[index,'PYminTopDepthFT'] = stat['minTopDepth']*3.28084
            IDS.at[index,'PYmaxBottomDepthFT'] = stat['maxBottomDepth']*3.28084
            IDS.at[index,'PYTDorCDreachedDT'] = stat['TDorCDreachedDT']
            IDS.at[index,'PYNPTDays'] = nptDays

            #Calculate flatTime
            flatTimeDays = calculateFlatTime(project=row['ProjectName'],rig=row['RigName'],well=row['WellName'],tddate=stat['TDorCDreachedDT'])
            IDS.at[index,'PYFlatDays'] = flatTimeDays

            #Days to Target Depth or Current Depth
            drillDays = pd.Timedelta(pd.to_datetime(stat['TDorCDreachedDT']) - pd.to_datetime(row['SpudDate'])).total_seconds() / 86400.0
            IDS.at[index,'PYdaysToTDorCD'] = drillDays

            #Days from SPUD to RR
            opsDays = pd.Timedelta(pd.to_datetime(stat['maxEndDT']) - pd.to_datetime(row['SpudDate'])).total_seconds() / 86400.0
            IDS.at[index,'PYOpsDays'] = opsDays
            IDS.at[index,'PYNPTPercent'] = (nptDays / opsDays) * 100

            #Feet Per day
            IDS.at[index,'PYFeetPerDay'] = (stat['maxBottomDepth']*3.28084) / drillDays
               
    return IDS

def calculateWellStats(project='ADMA SARB Island UAE',rig='ND-78',well='SR54'):

    """
      Calculate the wellstats for a given well. This is to allow for the generation of
      ft / day calcs

    """

    well = dataset.query('ProjectName=="'+project+'" & WellName=="'+well+'" & RigName=="'+rig+'" ')

    std = well['StartDate'].dropna(axis=0,how='any')
    endd = well['EndDate'].dropna(axis=0,how='any')
    td = well['TopDepth'].dropna(axis=0,how='any')
    bd = well['BottomDepth'].dropna(axis=0,how='any')

    #if any entries come back NAN throw it in the f**king bin - we cannot calcualte on them anyway
    if std.count() == 0 or endd.count() == 0 or td.count() == 0 or bd.count() == 0 :
        return None

    wellStat = {
        'maxEndDT':endd.max(),
        'minStartDT':std.min(),
        'minTopDepth':td.min(),
        'maxBottomDepth':bd.max()
    }
    

    TDRecords = well.query('BottomDepth == '+str(wellStat['maxBottomDepth']))
    TDRecords['EndDate'].dropna(axis=0,how='any',inplace=True)
    wellStat['TDorCDreachedDT'] = TDRecords['EndDate'].min()
  
    print(wellStat)
    return(wellStat)

def calculateNPTStats(project='ADMA SARB Island UAE',rig='ND-78',well='SR54'):
    """
        Return the sum of Non productive time days

    """

    #Get NPT rows
    npt = dataset.query('ProjectName=="'+project+'" & WellName=="'+well+'" & RigName=="'+rig+'" & TimeClassification=="Non Productive" ')
    nptDays = npt['Duration(Days)'].sum(skipna = True)
    return nptDays 

def calculateFlatTime(project='ADMA SARB Island UAE',rig='ND-78',well='SR54',tddate=pd.Timestamp('2018-12-17 17:30:00.000')):
    """
        Sum the time for each entry where there has not been a change in
        bottomdepth

        && PowerBI_wdm_Operational_TimeBreakdown[EndDate]<=PowerBI_wdm_Operational_TimeBreakdown[15 WelTD or ft/day Day TBD])

    """

    flatTimeEntries = dataset.query('ProjectName=="'+project+'" & WellName=="'+well+'" & RigName=="'+rig+'" & TopDepth==BottomDepth & EndDate < "'+tddate+'" ')
    flatTimeDays = flatTimeEntries['Duration(Days)'].sum(skipna = True)
    return flatTimeDays


testDS()

IDS = defineProjects()
IDS.to_csv (r'export_IDS.csv', index = None, header=True)



"""
11 DailyFootage = IF(PowerBI_wdm_Operational_TimeBreakdown[TimeClassification]="Productive" && PowerBI_wdm_Operational_TimeBreakdown[Scope]<>"No",
(PowerBI_wdm_Operational_TimeBreakdown[BottomDepth]-PowerBI_wdm_Operational_TimeBreakdown[TopDepth]),0)

"""