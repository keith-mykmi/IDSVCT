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


def testDS():

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
    

testDS()
IDS = defineProjects()

"""
11 DailyFootage = IF(PowerBI_wdm_Operational_TimeBreakdown[TimeClassification]="Productive" && PowerBI_wdm_Operational_TimeBreakdown[Scope]<>"No",
(PowerBI_wdm_Operational_TimeBreakdown[BottomDepth]-PowerBI_wdm_Operational_TimeBreakdown[TopDepth]),0)

"""