#!/usr/bin/python3

import pandas as pd
from datetime import datetime



pd.options.mode.chained_assignment = None  # default='warn'

#"dataset" would be the var used to store data in PBI
#remove this line in production
dataset = pd.read_csv('WTMobilisation20182019.csv')

print(dataset.info())