import locale
import pandas as pd

locale.setlocale( locale.LC_ALL,'' )

# this reads the CSV and stores a virtual copy of it in the variable df.
# df is Separate from the physical CSV and editing df does not edit the CSV.
# df stands for "dataframe"
#Change this line to the data source filepath
df = pd.read_csv( r"C:\Users\ehkim\Desktop\Putty\TEST BATCH_Scheduled_Report.csv" )

# This drops/removes the first three rows of the dataframe. The first three rows are 
# not relevant to the actual data. 
df.drop( [0,1,2],axis = 0, inplace = True )

# rf stands for "reference frame" and is used to check for NULL values in the original
# data
rf = pd.read_csv( r"C:\Users\ehkim\Desktop\Putty\TEST BATCH_Scheduled_Report.csv" )

# We previously removed the first three rows of df, that means row 3
# is now row 0. 
# iloc[n] will retrieve a LOCation (row) based on its Index (row number)
meters      = list( df.iloc[0].tolist() )    # list of ALL meter numbers (including repeats)
metrics     = list( df.iloc[1].tolist() )    # list of ALL metrics       (including repeats)
meter_list  = []                             # an empty list that will hold unique meter numbers
metric_list = []                             # an empty list that will hold unique metrics

# This loop extracts the unique meter numbers and unique metrics
for index in reversed( range( 1, len( meters ) ) ):          # operates in reverse
        if meters[index] == meters[index - 1]:               # checks if the current meter number is identical to the previous meter number
            if metrics[index] in metric_list:                # checks if the current metric is already in the list if unique metrics
                pass                                             # if the current metric is already in the unique list, it will ignore the current metric
            else:
                metric_list.append( metrics[index] )             # if the current metric IS NOT in the unique list, it will add the current metric to the list
            index += 1
        else:
            meter_list.append(meters[index])                 # if the current meter number IS NOT identical to the previous meter number, then add the meter number to the unique list of meters
            if metrics[index] in metric_list:
                 pass
            else:
                metric_list.append(metrics[index])
            index += 1

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

# the transformed data must have one row per meter number per timestamp
# the original data is processed one row at a time. the transformed data is temporarily stored in an array called "output" 
# and once the meter number changes, the program injects the output array into another virtual table and then resets the output array

# iterates over the reference dataframe
for index, row in ef.iterrows():
    if index <= 4:              # accounts for the three rows we threw out in df and the fourth row which contains the meter numbers
        pass
    else:
        m = 0                   # meter list index
        j = 1                   # column number 
        while j <=  len( row ): 
                if len( output ) == 0:                  # checks if the output array is empty ( AKA the start of a new row )
                    output.insert( 0 , row[0] )         # adds the rf's 0th column ( AKA datetime ) at the front of output array
                    output.insert( 1 , meter_list[m] )
                elif len ( output ) < metric_count and len( output ) != 0:    # the length of the transformed row is equal to the number of unique metrics, anything else and the tranformed row/output array is considered "incomplete"
                    if rf.isnull().iloc[index,j]:       # checks if the current ( row, column ) entry is null 
                        j += 1
                        output.append(int(0))           # if a null is detected, replace it with a 0
                        print(j)
                    else:  
                        output.append( locale.atof( row [j] ) )     # if the ( row, column ) is not a null, the programs converts the string to a float using atof ( ASCII to float )
                        j += 1
                        print(j)
                elif j == len( row ):                   # if the column index = the length of the row ( in the original data ) then move on to the next row 
                    print(output)
                    csv.append(output) 
                    m = 0
                    output=[]   
                    break        
                else:                                  # anything else means the tranformed row ( output array ) is complete
                    print(output)
                    csv.append(output)                 # add the output array to the virtual table 
                    m += 1                             # move on to the next meter
                    output=[]                          # clear the output array and prepare for the next transformation
                
        
                

    
out = pd.DataFrame(csv)
#Change this line to output filepath
out.to_csv(r"C:\Users\ehkim\Desktop\Putty\Output.csv",index=False)
print(metric_count)
print(metric_list)
print(meter_list)

