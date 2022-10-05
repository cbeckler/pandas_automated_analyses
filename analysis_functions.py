# SUITE OF FUNCTIONS TO AUTOMATE ANALYSIS AND REPORTING WITH PANDAS

######################## LIST AND DICTIONARY GENERATION ##################################

def create_label_order_dict(label_list):

    # this function will create a dictionary to apply new numbered lables to your data so that it orders correctly in tables

    # ARGUMENTS

    ## MANDATORY
    ### label_list is the list of labels for your category, which MUST be in desired order

    # create an empty dict
    order_dict = {}

    # iterating over the label_list
    for position, label in enumerate(label_list):
        # create a pair of the label and its numeric position
        order_dict[label] = str(position)

    return order_dict


def create_label_mapping(data_list, label_list):
    
    # this function will create a dictionary to apply map your data values to you desired labels
    ## the two lists MUST be in the same order!!

    # ARGUMENTS

    ## MANDATORY
    ### data_list is the list of labels in your raw data, which MUST be in the same order as your label_list
    ### label_list is the list of labels for your category, which MUST be in desired order

    # a dictionary is created that matches the value from each list together
    return dict(zip(data_list, label_list))


######################## GROUPBY RESULTS ##################################


def simple_groupby(df, col_name, aggregations, index_mapping=None, index_ordered_list=None, index_name=None, stats_names=None):

    # this function will perform a simple groupby by the specified column (col_name) and has optional args for formatting

    ## MANDATORY:
    ### df is your dataframe to be analyzed
    ### col_name is the column to perform the groupby with    
    ### aggregations is the dictionary containing your analyses for the groupby

    ## OPTIONAL:
    ### index_mapping is the dictionary to map your values in your col_name column to their desired labels
    ### index_ordered_list is the list of your col_name index values in their desired order
    ####        if you are using index_mapping to change the index values, this list MUST match the new names!
    ### index_name is the name of your index  
    ### stats_names is the list of names of your columns containing your results
    ####        this list MUST be in the same around as your aggregations!

    # import pandas in case any aggregations require it (ex: pd.Series.nunique for unique counts)
    import pandas as pd

    # the groupby
    df = df.groupby(col_name).agg(aggregations)

    # renaming index values
    if index_mapping == None:
        pass 
    else:
        # if your index name is also the name of one of your columns (ex: you took a count of your groupby var)
        if df.index.name in list(df.columns):
            # temporarily rename results col
            df.rename(columns={col_name:'temp'}, inplace=True)
            # reset the index for manipulation
            df.reset_index(inplace=True)
            # map the desired values onto your index
            df[col_name] = df[col_name].map(index_mapping)
            # set the index back
            df.set_index(col_name, inplace=True)
            # change the name of the column back
            df.rename(columns={'temp':col_name}, inplace=True)
        else:
           # reset the index for manipulation
            df.reset_index(inplace=True)
            # map the desired values onto your index
            df[col_name] = df[col_name].map(index_mapping)
            # set the index back
            df.set_index(col_name, inplace=True)

    # reordering the index
    if index_ordered_list == None:
        pass 
    else:
        # if your index name is also the name of one of your columns (ex: you took a count of your groupby var)
        if df.index.name in list(df.columns):
            # temporarily rename results col
            df.rename(columns={col_name:'temp'}, inplace=True)
            # reset the index for manipulation
            df.reset_index(inplace=True)
            # assigning int value to each index value in ascending order
            mapping = {index_order: i for i, index_order in enumerate(index_ordered_list)}
            # mapping those int values onto the index variable to create a key
            key = df[col_name].map(mapping)
            # reordering the dataframe by the key
            df = df.iloc[key.argsort()]
            # set the index back
            df.set_index(col_name, inplace=True)
            # change the name of the column back
            df.rename(columns={'temp':col_name}, inplace=True)
        else:
            # reset the index for manipulation
            df.reset_index(inplace=True)
            # assigning int value to each index value in ascending order
            mapping = {index_order: i for i, index_order in enumerate(index_ordered_list)}
            # mapping those int values onto the index variable to create a key
            key = df[col_name].map(mapping)
            # reordering the dataframe by the key
            df = df.iloc[key.argsort()]
            # set the index back
            df.set_index(col_name, inplace=True)

    # renaming index
    if index_name == None:
        pass 
    else:
        df.index.name = index_name

    # renaming results columns
    if stats_names == None:
        pass
    else:
        df.columns = stats_names         

    return df


######################## PIVOTED RESULTS ##################################

#####       SINGLE ROW INDEX SINGLE HEADER ROW              #####

def col_pivot_row_index_results(df, col_name, index_ordered_list, aggregations, col_mapping=None, col_order=None, index_mapping=None, \
    index_name=None):

    # this function will perform an analysis of the data by the col_name and index columns with groupby by col_name
    # it will then reshape and clean the results table to a report-ready format

    ## this function should be used when you have data in a column you wish to analyze by and then pivot so each
    ## data label in that column is the heading of a column 
    ###       ex: FY19-20, FY20-21, etc. that were in a single 'Fiscal Year' column
    ## the data should also have values in multiple columns you wish to analyze and combine into one index
    ###       ex: counts for 'inpatient', 'outpatient', 'emergency_care' columns to be combined into one 'Care Type' index
     
    # ARGUMENTS
    
    ## MANDATORY:
    ### df is your dataframe to be analyzed
    ### col_name is the column to perform the groupby with and whose values will become your column headers    
    ### index_ordered_list is the list of columns you wish to make your index after groupby, in their desired order
    ####        if you are using index_mapping to change the column names, this list MUST match the new names!
    ### aggregations is the dictionary containing your analyses for the groupby
    
    ## OPTIONAL:
    ### col_mapping is the dictionary to map your data values in col_name to you desired labels
    ### col_order is the dictionary to map your desired labels to their desired order
    ####    if you are using col_mapping, this dcitionary MUST match the new names!
    ### index_mapping is the dictionary to map your index col names in your data to their desired labels
    ### index_name is the name of your index           

    import pandas as pd

    # set up ordering for the pivot column

    ## map the labels to the data values for the column
    if col_mapping == None:
        pass 
    else:
        df = df.assign(name_col=df[col_name].apply(lambda x: col_mapping[x]))

    ## map the order to the label values for the column
    if col_order == None:
        pass
    if col_mapping == None:
        df = df.assign(order_col=df[col_name].apply(lambda x: col_order[x]))
    else:
        df = df.assign(order_col=df.name_col.apply(lambda x: col_order[x]))

    ## drop the orginal data column and the one with names--will be remapped to the order column later
    if col_mapping == None:
        pass 
    else:
        if col_order == None:
            df.drop(columns=['name_col'], inplace=True)
        else:
            df.drop(columns=[col_name, 'name_col'], inplace=True)
    
    ## rename order column to original col name
    if col_order == None:
        pass 
    else:
        df.rename(columns={'order_col':col_name}, inplace=True)

    ## casting var as categorical type instead of string (takes less memory)
    df[col_name] = df[col_name].astype('category')

    # rename index columns
    if index_mapping == None:
        pass 
    else:
        df.rename(columns=index_mapping, inplace=True)

    # groupby analysis
    df = df.groupby(col_name).agg(aggregations)

    # reshaping data

    # code to convert from wide to long format, and then pivot so the timepoints are columns and locs are rows

    # the index must be reset to a normal column in order to manipulate the data
    ## the inplace argument makes the change persist
    df.reset_index(inplace=True)

    # wide to long format--must be done before a pivot is possible
    ## will output a dataframe that gives a row with the index column and stats per index category per col_name category
    df = pd.melt(df, 
                 id_vars=[col_name], 
                 value_vars=index_ordered_list)

    # pivot--will assign the col_name values to the columns and the index values to the index row
    df = df.pivot(index='variable',columns=col_name,values='value')
    
    # cleaning up dataframe

    # code to order the index values in the order they are meant to be in for visualization and reporting

    # must reset index to manipulate it
    df.reset_index(inplace=True)
    
    # assigning int value to each index value in ascending order
    mapping = {index_order: i for i, index_order in enumerate(index_ordered_list)}
    # mapping those int values onto the index variable to create a key
    key = df['variable'].map(mapping)
    # reordering the dataframe by the key
    df = df.iloc[key.argsort()]
    # setting index and renaming if necessary
    if index_name == None:
        # setting the index back to index var
        df.set_index('variable', inplace=True)
    else:
        # rename index
        df.rename(columns={'variable':index_name}, inplace=True)
        # setting index back to index var
        df.set_index(index_name, inplace=True)

    if col_mapping == None:    
        pass
    if col_order == None:
        # putting labels back on columns
        df.rename(columns=col_mapping, inplace=True)
    else:        
        # reverse key and value pairs of col_order dict
        col_rename = {v: k for k, v in col_order.items()}    

        # putting labels back on columns
        df.rename(columns=col_rename, inplace=True) 

    return df