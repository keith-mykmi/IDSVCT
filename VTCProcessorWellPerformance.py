#!/usr/bin/python3

import pandas as pd
from datetime import datetime

"""
A processor using the Pandas framework to manipulate
Well processing information and allow it to be easily
viewed in PowerBI

"""
pd.options.mode.chained_assignment = None  # default='warn'

#Create a new Dataframe with session data
dtn = datetime.utcnow()
sessionDetails = {'Category': ['Session_Date_Time','Session_TimeZone'], 'DateTime': [dtn.strftime("%d %B %Y %H:%M:%S"),'UTC']}
df = pd.DataFrame(data=sessionDetails)

#"dataset" would be the var used to store data in PBI
#remove this line in production
dataset = pd.read_csv(r'Input\WTExport20182019.csv')

#Load the mobilisation spreadsheet
mob = pd.read_csv(r'Input\WTMobilisation20182019.csv')

#Drop Entries with no RigName
dataset.dropna(subset = ['RigName'], inplace=True, axis=0, how='any')

#Drop rows with start, end, top or bottom as NULL or NAN
dataset.dropna(subset = ['StartDate', 'EndDate','TopDepth','BottomDepth'], inplace=True, axis=0, how='any')

#Remove any rows with whitespace that is causing issues
dataset = dataset.rename(columns={"Global Name":"GlobalName"},errors="raise")


def defineProjects():

    """
       Drop duplicates, returning only the first records with unique
       WellTrak Project GUID, WellName and RigName 

       The purpose of this is to return a set of records that is a record
       of every Well monitored by the Welltrak System

       Returns: Pandas DataFrame

    """

    #Drop the duplicate rows for each well, leaving a one line entry for each.
    IDS = dataset.drop_duplicates(subset = ['Welltrak_Project_Guid','WellName','RigName'], keep ='first', inplace = False)

    #Drop unused cols
    IDS.drop(['JobName', 'FinalReportFlag','Borehole','Phase','StartDate',
    'Start_Day_Number','Activity','SubActivity','TopDepth','BottomDepth','TimeClassification',
    'Planned_Flag','NPTVendor', 'GlobalName','NPTCategory','NPTSubCategory','Scope','Bit_Serial_Number',
    'WE','WSS','AWSS','WSC','Skipped_Flag'], axis=1,inplace=True)

    #Sort by project name
    IDS = IDS.sort_values(by=['ProjectName','RigName','WellName'])

    #For each single well row, calcuate the initial well stats
    for index, row in IDS.iterrows():

        print('Setting Stats for: ',row['ProjectName'],' | ',row['RigName'],' | ',row['WellName'])

        nptDays = calculateNPTStats(project=row['ProjectName'],rig=row['RigName'],well=row['WellName'])
        stat = calculateWellStats(project=row['ProjectName'],rig=row['RigName'],well=row['WellName'])
        mobilisationDays = calculateMobilisation(project=row['ProjectName'],rig=row['RigName'],well=row['WellName'])
        completionDays = calculateCompletions(project=row['ProjectName'],rig=row['RigName'],well=row['WellName'])
        drillingTime = calculateDrillingTime(project=row['ProjectName'],rig=row['RigName'],well=row['WellName'])
        InScopeAndProductiveFootage = calculateInScopeProductiveFootage(project=row['ProjectName'],rig=row['RigName'],well=row['WellName'])


        if stat is None:
            print('***NULL ROW***')
            IDS = IDS.drop(index, axis=0)
        else:

            IDS.at[index,'PYdrillingTime'] = drillingTime
            IDS.at[index,'PYInScopeProductiveFootage'] = InScopeAndProductiveFootage

            #Calculate FPDDrilling measure
            if stat['minTopDepth'] < 150 :
                IDS.at[index,'PYFPDDrilling'] = ((InScopeAndProductiveFootage*3.2808) + stat['minTopDepth']*3.28084) / drillingTime
            else:
                IDS.at[index,'PYFPDDrilling'] = (InScopeAndProductiveFootage*3.2808) / drillingTime


            IDS.at[index,'PYmaxEndDT'] = stat['maxEndDT']
            IDS.at[index,'PYminStartDT'] = stat['minStartDT']
            IDS.at[index,'PYminTopDepthFT'] = stat['minTopDepth']*3.28084
            IDS.at[index,'PYmaxBottomDepthFT'] = stat['maxBottomDepth']*3.28084
            IDS.at[index,'PYTDorCDreachedDT'] = stat['TDorCDreachedDT']
            IDS.at[index,'PYMaxStartNumber'] = stat['maxStartDayNumber']

            IDS.at[index,'PYNPTDays'] = nptDays

            #Calculate flatTime
            flatTimeDays = calculateFlatTime(project=row['ProjectName'],rig=row['RigName'],well=row['WellName'],tddate=stat['TDorCDreachedDT'])
            IDS.at[index,'PYFlatDays'] = flatTimeDays

            #Days to Target Depth or Current Depth
            drillDays = pd.Timedelta(pd.to_datetime(stat['TDorCDreachedDT']) - pd.to_datetime(row['SpudDate'])).total_seconds() / 86400.0
            IDS.at[index,'PYdaysToTDorCD'] = drillDays

            #Days from SPUD to Last DT Entry
            opsDays = pd.Timedelta(pd.to_datetime(stat['maxEndDT']) - pd.to_datetime(row['SpudDate'])).total_seconds() / 86400.0
            IDS.at[index,'PYOpsDays'] = opsDays
 
            #OPT - SPUD to RR
            OPT = pd.Timedelta(pd.to_datetime(row['Rig_Release']) - pd.to_datetime(row['SpudDate'])).total_seconds() / 86400.0
            IDS.at[index,'PYOPTSpudToRR'] = (stat['maxBottomDepth']*3.28084) / OPT

            #NPT
            IDS.at[index,'PYNPTPercent'] = (nptDays / OPT) * 100

          
            #Feet Per day 
            IDS.at[index,'PYFeetPerDayToTD'] = (stat['maxBottomDepth']*3.28084) / drillDays
            IDS.at[index,'PYFeetPerDayToRR'] = (stat['maxBottomDepth']*3.28084) / OPT

            #Mobilisation time
            IDS.at[index,'PYMoblisationDays'] = mobilisationDays

            #Completions time
            IDS.at[index,'PYCompletionDays'] = completionDays
               
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
    sdn = well['Start_Day_Number'].dropna(axis=0,how='any')


    #if any entries come back NAN throw it in the f**king bin - we cannot calculate on them anyway
    if std.count() == 0 or endd.count() == 0 or td.count() == 0 or bd.count() == 0 :
        return None


    wellStat = {
        'maxEndDT':endd.max(),
        'minStartDT':std.min(),
        'minTopDepth':td.min(),
        'maxBottomDepth':bd.max(),
        'maxStartDayNumber':sdn.max()
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

def calculateCompletions(project='ADMA SARB Island UAE',rig='ND-78',well='SR54'):
    """
        Sum the time for each entry in completions phase

    """
    completionTimeEntries = dataset.query('ProjectName=="'+project+'" & WellName=="'+well+'" & RigName=="'+rig+'" & Activity=="Completion" ')
    completionTimeEntriesDays = completionTimeEntries['Duration(Days)'].sum(skipna = True)
    return completionTimeEntriesDays

def calculateMobilisation(project='ADMA SARB Island UAE',rig='ND-78',well='SR54'):

   mobSingleWell = mob.query('Project_Name=="'+project+'" & Well=="'+well+'" & Rig=="'+rig+'" ')
   mobDays = mobSingleWell['Duration(Days)'].sum(skipna = True)
   return mobDays

def NPTBreakDown():

    #Generate a list of NPT categories, and drop_duplicated to create a header..
    NPTCats = dataset[['Welltrak_Project_Guid','ProjectName','WellName','RigName','GlobalName','Duration(Days)','Rig_Release']]
    NPTCats.dropna(subset = ['GlobalName'], inplace=True, axis=0, how='any')
    NPTCatsHeader = NPTCats.drop_duplicates(subset = ['Welltrak_Project_Guid','WellName','RigName','GlobalName'], keep ='first', inplace = False)

    print('Processing NPT for: ',NPTCatsHeader)

    for index, row in NPTCatsHeader.iterrows():
        print('Processing NPT for: ',row.tolist())

        singleNPTEnt = NPTCats.query('ProjectName=="'+row['ProjectName']+'" & WellName=="'+row['WellName']+'" & RigName=="'+row['RigName']+'" & GlobalName=="'+row['GlobalName']+'"  ')
        currentNPTDys = singleNPTEnt['Duration(Days)'].sum()
        NPTCatsHeader.at[index,'PYDurationDays'] = currentNPTDys

    #Remove the Duration field as it will cause confusion and return
    return NPTCatsHeader.drop(['Duration(Days)'], axis=1)

def calculateDrillingTime(project='ADMA SARB Island UAE',rig='ND-78',well='SR54'):

   singleWell = dataset.query('ProjectName=="'+project+'" & WellName=="'+well+'" & RigName=="'+rig+'" & TimeClassification == "Productive" & Scope == "Yes" & BottomDepth > TopDepth  ')
   drillingTime = singleWell['Duration(Days)'].sum(skipna = True)
   return drillingTime

def calculateInScopeProductiveFootage(project='ADMA SARB Island UAE',rig='ND-78',well='SR54'):

   singleWell = dataset.query('ProjectName=="'+project+'" & WellName=="'+well+'" & RigName=="'+rig+'" & TimeClassification == "Productive" & Scope == "Yes"')
   InScopeAndProductiveFootage = singleWell['BottomDepth'].sum(skipna = True) - singleWell['TopDepth'].sum(skipna = True)
   return InScopeAndProductiveFootage


IDS = defineProjects()
IDSNPT = NPTBreakDown()

with pd.ExcelWriter('IDS.xlsx') as writer:  # doctest: +SKIP
    IDS.to_excel(writer, sheet_name='WellPerformance')
    IDSNPT.to_excel(writer, sheet_name='NPTBreakdown')
    df.to_excel(writer, sheet_name='SessionNotes')