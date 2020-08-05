import pandas as pd
import json
from pandas import json_normalize
import re

with open('data.json', 'r') as f:
    data = json.load(f)
df = json_normalize(data)
finalDf = pd.DataFrame(columns=['State', 'District', 'Delta Confirmed', 'Delta Deceased', 'Delta Recovered', 'Total Confirmed', 'Total Deceased', 'Total Recovered'])


df = df.drop(df.filter(regex='meta').columns, axis=1)
df = df.drop(df.filter(regex='tested').columns, axis=1)
df = df.loc[:, ~df.columns.str.startswith('AN')]
df = df.loc[:, ~df.columns.str.startswith('DN')]

templist = ['','','','','','','','']
counter = 1
finalData = []

for col in df.columns:
    flag = 0
    if col.find("districts") != -1:
        
        templist[0] = col[:2]
        try:

            if col.find("delta") != -1:
                district = re.search(r'districts.(.*?).delta', col).group(1)
                templist[1] = district
            
            elif col.find("total") != -1:
                district = re.search(r'districts.(.*?).total.*', col).group(1)

        except AttributeError:
            continue

            
        
        
        if templist[1] != '':      

            if col.find("delta") != -1:
                if col.find("delta.confirmed") != -1 and col.find("districts") != -1:
                    
                    dConfirmed = df.at[0, col]
                    templist[2] = dConfirmed
                
                elif col.find("delta.deceased") != -1 and col.find("districts") != -1:
                    
                    dDeceased = df.at[0, col]
                    templist[3] = dDeceased
                
                elif col.find("delta.recovered") != -1 and col.find("districts") != -1:
                    dRecovered = df.at[0, col]
                    templist[4] = dRecovered
            
            
            elif col.find("total.confirmed") != -1 and col.find("districts") != -1:
                tConfirmed = df.at[0, col]
                templist[5] = tConfirmed
                
            elif col.find("total.deceased") != -1 and col.find("districts") != -1:
                    
                tDeceased = df.at[0, col]
                templist[6] = tDeceased
                
            elif col.find("total.recovered") != -1 and col.find("districts") != -1:
                tRecovered = df.at[0, col]
                templist[7] = tRecovered
                print(">>  Entry #" + str(counter) + " " + templist[1] + " District in " + templist[0] + " State" " - Successfully Added")
                finalData.append(templist)
                counter = counter + 1
                templist = ['','','','','','','','']


finalDf = pd.DataFrame(finalData, columns=['State', 'District', 'Delta Confirmed', 'Delta Deceased', 'Delta Recovered', 'Total Confirmed', 'Total Deceased', 'Total Recovered'])

finalDf.to_csv('total.csv', index=False)

print("\n>> Data.CSV Generated \n")

print(">> Task Completed Successfully\n\n")
