import locale
import pandas as pd

locale.setlocale(locale.LC_ALL,'')

#Change this line to the data source filepath
df = pd.read_csv(r"C:\Users\ehkim\Desktop\Putty\TEST BATCH_Scheduled_Report.csv")
df.drop([0,1,2],axis = 0, inplace = True)
ef = pd.read_csv(r"C:\Users\ehkim\Desktop\Putty\TEST BATCH_Scheduled_Report.csv")

meters = list(df.iloc[0].tolist())
metrics = list(df.iloc[1].tolist())
meter_list = []
metric_list = []

for i in reversed(range(1, len(meters))):
        if meters[i] == meters[i - 1]:
            if metrics[i] in metric_list:
                 pass
            else:
                metric_list.append(metrics[i])
            i += 1
        else:
            meter_list.append(meters[i])
            if metrics[i] in metric_list:
                 pass
            else:
                metric_list.append(metrics[i])
            i += 1

metric_list.append('Meter Number')
metric_list.append('Datetime')
meter_list = list( reversed( meter_list ) )
metric_list = list(reversed(metric_list))
metric_count = len(metric_list)

print(metric_count)
output = []
csv = []
csv.append(metric_list)
df.drop( [3,4], axis = 0, inplace = True )

for index, row in ef.iterrows():
    if index <= 4:
        pass
    else:
        m = 0
        j = 1
        while j <=  len( row ): 
                if len( output ) == 0:
                    output.insert( 0 , row[0] )
                    output.insert( 1 , meter_list[m] )
                elif len ( output ) < metric_count and len( output ) != 0:
                    if ef.isnull().iloc[index,j]:
                        j += 1
                        output.append(int(0))
                        print(j)
                    else:  
                        output.append( locale.atof( row [j] ) )
                        j += 1
                        print(j)
                elif j == len( row ):
                    print(output)
                    csv.append(output) 
                    m = 0
                    output=[]   
                    break        
                else:   
                    print(output)
                    csv.append(output) 
                    m += 1
                    output=[]           
                
        
                

    
out = pd.DataFrame(csv)
#Change this line to output filepath
out.to_csv(r"C:\Users\ehkim\Desktop\Putty\Output.csv",index=False)
print(metric_count)
print(metric_list)
print(meter_list)

