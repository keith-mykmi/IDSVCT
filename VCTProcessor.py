#!/usr/bin/python3

import pandas as pd

"""
A processor using the Pandas framework to manipulate
Well processing information and allow it to be easily
viewed in PowerBI

"""

#"dataset" would be the var used to store data in PBI
#remove this line in production
dataset = pd.read_csv('WTExport200000.csv')
dataset['RigName'].fillna(value='none',inplace=True)


def testDS():

    """
      Several functions to test the successful loading of the sample 
      data - can de disgarded on production deployement.

    """

    #Test load
    print(dataset.info())
    print(dataset['ProjectName'].unique().tolist())
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
    print('IDS Project Record Generated: ',  IDS.info())
    IDS.sort_values(by=['ProjectName','RigName','WellName'],inplace=True)
    #wellValues = IDS[['ProjectName','RigName','WellName']].values

    #Calcuate the initial well stats
    for index, row in IDS.iterrows():
        print('Getting Stats for: ',row['ProjectName'],' | ',row['RigName'],' | ',row['WellName'])

        stat = calculateWellStats(project=row['ProjectName'],rig=row['RigName'],well=row['WellName'])

        print('Setting Stats for: ',row['ProjectName'],' | ',row['RigName'],' | ',row['WellName'])
        
        for key, val in stat.items():
            row[key] = stat[key]
        
def calculateWellStats(project='ADMA SARB Island UAE',rig='ND-78',well='SR54'):

    """
      Calculate the wellstats for a given well. This is to allow for the generation of
      ft / day calcs

    """

    well = dataset.query('ProjectName=="'+project+'" & WellName=="'+well+'" & RigName=="'+rig+'" ')
    std = well['StartDate'].dropna()
    endd = well['EndDate'].dropna()
    td = well['TopDepth'].dropna()
    bd = well['BottomDepth'].dropna()

    wellStat = {
        'maxEndDT': endd.max(),
        'minStartDT':std.min(),
        'minTopDepth':td.min(),
        'maxBottomDepth':bd.max()
    }

    TDRecords = well.query('BottomDepth == '+str(wellStat['maxBottomDepth']))

    wellStat['TDorCDreachedDT'] = TDRecords['EndDate'].min()

    print(wellStat)

    return(wellStat)


testDS()
IDS = defineProjects()





"""
11 DailyFootage = IF(PowerBI_wdm_Operational_TimeBreakdown[TimeClassification]="Productive" && PowerBI_wdm_Operational_TimeBreakdown[Scope]<>"No",
(PowerBI_wdm_Operational_TimeBreakdown[BottomDepth]-PowerBI_wdm_Operational_TimeBreakdown[TopDepth]),0)

"""