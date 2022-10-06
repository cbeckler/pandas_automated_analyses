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


def simple_groupby(df, col_name, aggregations, index_mapping=None, index_ordered_list=None, index_name=None, stats_names=None,\
    null_to_0=None):

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
    ### null_to_0 is your list of columns (matching stats_names) to convert nulls to 0s. defaults to None

    # import pandas in case any aggregations require it (ex: pd.Series.nunique for unique counts)
    import pandas as pd


    # groupby var needs to be cateogorical so all rows return data even if there is none, which needs to be categorical dtype
    ## but categorical dtype cols cannot be sorted
    ## create a copy var and set that as categorical type
    df['copy'] = df[col_name]
    df['copy'] = df['copy'].astype('category')    

    # groupby the col_name and its copy
    df = df.groupby([col_name, 'copy']).agg(aggregations)

    # reset the index
    df.reset_index(inplace=True)
    # get rid of duplicates by only keeping rows where the copy matches the col_name
    df = df[df[col_name]==df['copy']]
    # drop the copy column
    df.drop(columns=['copy'],inplace=True)
    # set the index back to just col_name
    df.set_index(col_name, inplace=True)

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

    # replace nulls with 0
    if null_to_0 == None:
        pass
    else:
        for null_col_name in null_to_0:
            for col_num, df_col_name in enumerate(df.columns):
                if null_col_name == df_col_name:
                    df[df_col_name] = df[df_col_name].fillna(0)         

    return df


######################## PIVOTED RESULTS ##################################

#####       SINGLE ROW INDEX SINGLE HEADER ROW              #####

def col_pivot_row_combined_index_results(df, col_name, index_ordered_list, aggregations, col_mapping=None, col_order=None, index_mapping=None, \
    index_name=None, null_to_0=False):

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
    ####        if you are using index_mapping to change the column names, this dictionary MUST match the new names!
    
    ## OPTIONAL:
    ### col_mapping is the dictionary to map your data values in col_name to you desired labels
    ### col_order is the dictionary to map your desired labels to their desired order
    ####    if you are using col_mapping, this dcitionary MUST match the new names!
    ### index_mapping is the dictionary to map your index col names in your data to their desired labels
    ### index_name is the name of your index 
    ### null_to_0 will convert all nulls to 0 if True. defaults to False          

    import pandas as pd

    # set up ordering for the pivot column

    ## map the labels to the data values for the column
    if col_mapping == None:
        pass 
    else:
        # create a name_col with label values
        df = df.assign(name_col=df[col_name].apply(lambda x: col_mapping[x]))

    ## map the order to the label values for the column
    if col_order == None and col_mapping == None:
        pass
    elif col_mapping == None:
        # if there is not col mapping but is col order, create a order_col based off col from raw data
        df = df.assign(order_col=df[col_name].apply(lambda x: col_order[x]))
    elif col_order != None:
        # if there is a col mapping and col ordering, create order_col based off mapping
        df = df.assign(order_col=df.name_col.apply(lambda x: col_order[x]))
    else:
        pass

    ## drop the orginal data column and the one with names--will be remapped to the order column later
    if col_mapping == None:
        pass 
    else:
        # if there is no col order
        if col_order == None:
            # rename original column (code will break if you don't)
            df.rename(columns={col_name:'temp'}, inplace=True)
            # rename name_col to orignal column name
            df.rename(columns={'name_col':col_name}, inplace=True)
            # drop old column
            df.drop(columns=['temp'], inplace=True)
        else:
            # if there IS an order column, then drop name_col
            df.drop(columns=['name_col'], inplace=True)
    
    ## rename order column to original col name--order column should take precedence if it exists
    if col_order == None:
        pass 
    else:
        # rename original column (code will break if you don't)
        df.rename(columns={col_name:'temp'}, inplace=True)
        # rename order_col to orignal column name
        df.rename(columns={'order_col':col_name}, inplace=True)
        # drop old column
        df.drop(columns=['temp'], inplace=True)

    # rename index columns
    if index_mapping == None:
        pass 
    else:
        # rename index columns with index_mapping
        df.rename(columns=index_mapping, inplace=True)

    # groupby var needs to be cateogorical so all rows return data even if there is none, which needs to be categorical dtype
    ## but categorical dtype cols cannot be sorted
    ## create a copy var and set that as categorical type
    df['copy'] = df[col_name]
    df['copy'] = df['copy'].astype('category') 

    # groupby analysis
    df = df.groupby([col_name, 'copy']).agg(aggregations)

    # reset the index
    df.reset_index(inplace=True)
    # get rid of duplicates by only keeping rows where the copy matches the col_name
    df = df[df[col_name]==df['copy']]
    # drop the copy column
    df.drop(columns=['copy'],inplace=True)
    # set the index back to just col_name
    df.set_index(col_name, inplace=True)

    # if you do not have col_mapping but you do have col_order
    if col_mapping == None and col_order != None:
        # reset index to manipulate it
        df.reset_index(inplace=True)   
        # sort by col_order values ascending 
        df.sort_values(by=[col_name], inplace=True)
        # set index back in place
        df.set_index([col_name], inplace=True)
        # create dictionary to return order name to original data labels
        index_rename = {v: k for k, v in col_order.items()}
        # rename index values with dictionary
        df.rename(index=index_rename, level=0, inplace=True)
    else:
        pass

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
    elif col_order == None:
        # putting labels back on columns when there is no col_order
        df.rename(columns=col_mapping, inplace=True)
    else:        
        # reverse key and value pairs of col_order dict
        col_rename = {v: k for k, v in col_order.items()}    

        # putting labels back on columns
        df.rename(columns=col_rename, inplace=True)

    # replace nulls with 0
    if null_to_0 == False:
        pass
    else:
        for col_num, df_col_name in enumerate(df.columns):
            df[df_col_name] = df[df_col_name].fillna(0) 

    return df


#####       TWO VAR ROW MULTIINDEX SINGLE HEADER ROW              #####


def col_pivot_row_combined_multiindex_results(df, col_name, index_ordered_list, index_col, aggregations, col_mapping=None, col_order=None, index_mapping=None, \
    index2_ordered_list=None, index1_name=None, index2_name=None, reorder_row_indices=True, pct_index1cat=False, null_to_0=False):

    # this function will perform an analysis of the data by the col_name and index columns with groupby by col_name and index_col
    # it will then reshape and clean the results table to a report-ready format

    ## this function should be used when you have data in a column you wish to analyze by and then pivot so each
    ## data label in that column is the heading of a column 
    ###       ex: FY19-20, FY20-21, etc. that were in a single 'Fiscal Year' column
    ## the data should also have values in multiple columns you wish to analyze and combine into one index
    ###       ex: counts for 'inpatient', 'outpatient', 'emergency_care' columns to be combined into one 'Care Type' index
    ## it should also have one categorical column to be used as index_col in the groupby (ex: a column for race or gender)
     
    # ARGUMENTS
    
    ## MANDATORY:
    ### df is your dataframe to be analyzed
    ### col_name is the column to perform the groupby with and whose values will become your column headers    
    ### index_ordered_list is the list of columns you wish to make your index after groupby, in their desired order
    ####        if you are using index_mapping to change the column names, this list MUST match the new names!
    ### aggregations is the dictionary containing your analyses for the groupby
    ####        if you are using index_mapping to change the column names, this dictionary MUST match the new names!
    
    ## OPTIONAL:
    ### col_mapping is the dictionary to map your data values in col_name to you desired labels
    ### col_order is the dictionary to map your desired labels to their desired order
    ####    if you are using col_mapping, this dcitionary MUST match the new names!
    ### index_mapping is the dictionary to map your index col names in your data to their desired labels
    ### index2_ordered_list is the list of your index 2 values in order. while not mandatatory, highly recommended
    ### index1_name is the name of your first index (created from the index_ordered_list columns)
    ### index2_name is the name of your second index (created from the index_col)
    ### reorder_row_indices will reorder your results df in ascending order for both indices. defaults to True
    ####        this will reorder accourding to index_ordered_list (always) and index2_ordered_list (when present)
    ### pct_index1cat will convert your data into percentage form per category in index1 for each column. defaults to False
    ### null_to_0 will convert all nulls to 0 if True. defaults to False

    import pandas as pd

    if reorder_row_indices == True and index2_ordered_list == None:
        index2_ordered_list = [value for value in pd.unique(df[index_col])]

    # set up ordering for the pivot column

    ## map the labels to the data values for the column
    if col_mapping == None:
        pass 
    else:
        # create a name_col with label values
        df = df.assign(name_col=df[col_name].apply(lambda x: col_mapping[x]))

    ## map the order to the label values for the column
    if col_order == None and col_mapping == None:
        pass
    elif col_mapping == None:
        # if there is not col mapping but is col order, create a order_col based off col from raw data
        df = df.assign(order_col=df[col_name].apply(lambda x: col_order[x]))
    elif col_order != None:
        # if there is a col mapping and col ordering, create order_col based off mapping
        df = df.assign(order_col=df.name_col.apply(lambda x: col_order[x]))
    else:
        pass

    ## drop the orginal data column and the one with names--will be remapped to the order column later
    if col_mapping == None:
        pass 
    else:
        # if there is no col order
        if col_order == None:
            # rename original column (code will break if you don't)
            df.rename(columns={col_name:'temp'}, inplace=True)
            # rename name_col to orignal column name
            df.rename(columns={'name_col':col_name}, inplace=True)
            # drop old column
            df.drop(columns=['temp'], inplace=True)
        else:
            # if there IS an order column, then drop name_col
            df.drop(columns=['name_col'], inplace=True)
    
    ## rename order column to original col name--order column should take precedence if it exists
    if col_order == None:
        pass 
    else:
        # rename original column (code will break if you don't)
        df.rename(columns={col_name:'temp'}, inplace=True)
        # rename order_col to orignal column name
        df.rename(columns={'order_col':col_name}, inplace=True)
        # drop old column
        df.drop(columns=['temp'], inplace=True)

    # rename index columns
    if index_mapping == None:
        pass 
    else:
        # rename index columns with index_mapping
        df.rename(columns=index_mapping, inplace=True)

    # groupby var needs to be cateogorical so all rows return data even if there is none, which needs to be categorical dtype
    ## but categorical dtype cols cannot be sorted
    ## create a copy var and set that as categorical type
    df['copy'] = df[col_name]
    df['copy'] = df['copy'].astype('category') 

    df = df.groupby([col_name, 'copy', index_col]).agg(aggregations)

    # reset the index
    df.reset_index(inplace=True)
    # get rid of duplicates by only keeping rows where the copy matches the col_name
    df = df[df[col_name]==df['copy']]
    # drop the copy column
    df.drop(columns=['copy'],inplace=True)
    # set the index back to just col_name
    df.set_index([col_name, index_col], inplace=True)    

    # if you do not have col_mapping but you do have col_order
    if col_mapping == None and col_order != None:
        # reset index to manipulate it
        df.reset_index(inplace=True)   
        # sort by col_order values ascending 
        df.sort_values(by=[col_name], inplace=True)
        # set index back in place
        df.set_index([col_name], inplace=True)
        # create dictionary to return order name to original data labels
        index_rename = {v: k for k, v in col_order.items()}
        # rename index values with dictionary
        df.rename(index=index_rename, level=0, inplace=True)
    else:
        pass

    if pct_index1cat == False:
        pass 
    else:
        df = df.groupby(level=0).apply(lambda x: x / x.sum())

    # reshaping data

    # code to convert from wide to long format, and then pivot so col_name values are columns and our two indices are rows

    # same as before, index must be reset to regular columns to manipulate
    df.reset_index(inplace=True)

    # wide to long
    df = pd.melt(df, 
            # in this, both col_name and index_cols are the categorical variables, but the values still come solely from index_ordered_list vars
            id_vars=[col_name, index_col], 
            value_vars=index_ordered_list)
 
    # pivot. this time, both our new categorical var from index_ordered_list and index_col are set as index, creating a multiindex
    df = df.pivot(index=['variable',index_col],columns=col_name,values='value')

    # cleaning up dataframe

    # reordering row indices
    if reorder_row_indices == False:
        pass 
    else:
        # must reset index to manipulate it
        df.reset_index(inplace=True)
        
        # assigning int value to each index value in ascending order
        mapping1 = {index_order: i for i, index_order in enumerate(index_ordered_list)}
        mapping2 = {index_order: i for i, index_order in enumerate(index2_ordered_list)}
        
        # mapping those int values onto the index variable to create a key
        key1 = df['variable'].map(mapping1)
        key2 = df[index_col].map(mapping2)
        
        # reordering the dataframe by the key
        ## creating int columns based off key values (iloc method will not work for multiindex reordering more than one of the indices)
        df['key1'] = key1
        df['key2'] = key2

        # sorting by the new columns and then dropping them
        df.sort_values(by=['key1','key2'], inplace=True)
        df.drop(['key1','key2'], axis=1, inplace=True)
        # setting the index back
        df.set_index(['variable',index_col], inplace=True)

    # renaming indices
    if index1_name == None:
        pass 
    else:
        df.index.set_names(index1_name, level=0, inplace=True)

    if index2_name == None:
        pass 
    else:
        df.index.set_names(index2_name, level=1, inplace=True)

    # returning column lables
    if col_mapping == None:    
        pass
    elif col_order == None:
        # putting labels back on columns when there is no col_order
        df.rename(columns=col_mapping, inplace=True)
    else:        
        # reverse key and value pairs of col_order dict
        col_rename = {v: k for k, v in col_order.items()}    

        # putting labels back on columns
        df.rename(columns=col_rename, inplace=True) 

    # replace nulls with 0
    if null_to_0 == False:
        pass
    else:
        for col_num, df_col_name in enumerate(df.columns):
            df[df_col_name] = df[df_col_name].fillna(0) 

      
    return df


#####       SINGLE ROW INDEX DOUBLE HEADER ROW              #####

def col_pivot_row_index_dbl_header_results(df, col_name, index_col, stats_names, aggregations, col_mapping=None, col_order=None, \
    index_mapping=None, index_order=None, index_name=None, null_to_0=None):

    # this function will perform an analysis of the data by the col_name and index_col with specificed aggregations 
    # and groupby by col_name and index_col
    ## it will then reshape and clean the results table to a report-ready format

    ## this function should be used when you have data in a column you wish to analyze by and then pivot so each
    ## data label in that column is the heading of a column 
    ###       ex: FY19-20, FY20-21, etc. that were in a single 'Fiscal Year' column
    ## the data should also have values in an index column
    ###       ex: months in a month column for 'Jan', 'Feb', 'Mar' etc
     
    # ARGUMENTS
    
    ## MANDATORY:
    ### df is your dataframe to be analyzed
    ### col_name is the column to perform the groupby with and whose values will become your column headers    
    ### index_col is the column that will be your row index
    ### stats_names is your list of what each analysis should be called in your able, in desired order
    ####        MUST be in the same order as your aggregations!
    ### aggregations is the dictionary containing your analyses for the groupby, in desired order
    ####        MUST be in the same order as stats_names!
    
    ## OPTIONAL:
    ### col_mapping is the dictionary to map your data values in col_name to you desired labels
    ### col_order is the dictionary to map your desired labels to their desired order
    ####    if you are using col_mapping, this dcitionary MUST match the new names!
    ### index_mapping is the dictionary to map your index col names in your data to their desired labels
    ### index_order is the list of index values in the order you want them to be in
    ####    if you are changing your index values with index_mapping, they MUST match the new values!
    ### index_name is the name of your index
    # ### null_to_0 is your list of columns (matching stats_names) to convert nulls to 0s. defaults to None           

    import pandas as pd

    # set up ordering for the pivot column

    ## map the labels to the data values for the column
    if col_mapping == None:
        pass 
    else:
        # create a name_col with label values
        df = df.assign(name_col=df[col_name].apply(lambda x: col_mapping[x]))

    ## map the order to the label values for the column
    if col_order == None and col_mapping == None:
        pass
    elif col_mapping == None:
        # if there is not col mapping but is col order, create a order_col based off col from raw data
        df = df.assign(order_col=df[col_name].apply(lambda x: col_order[x]))
    elif col_order != None:
        # if there is a col mapping and col ordering, create order_col based off mapping
        df = df.assign(order_col=df.name_col.apply(lambda x: col_order[x]))
    else:
        pass

    ## drop the orginal data column and the one with names--will be remapped to the order column later
    if col_mapping == None:
        pass 
    else:
        # if there is no col order
        if col_order == None:
            # rename original column (code will break if you don't)
            df.rename(columns={col_name:'temp'}, inplace=True)
            # rename name_col to orignal column name
            df.rename(columns={'name_col':col_name}, inplace=True)
            # drop old column
            df.drop(columns=['temp'], inplace=True)
        else:
            # if there IS an order column, then drop name_col
            df.drop(columns=['name_col'], inplace=True)
    
    ## rename order column to original col name--order column should take precedence if it exists
    if col_order == None:
        pass 
    else:
        # rename original column (code will break if you don't)
        df.rename(columns={col_name:'temp'}, inplace=True)
        # rename order_col to orignal column name
        df.rename(columns={'order_col':col_name}, inplace=True)
        # drop old column
        df.drop(columns=['temp'], inplace=True)

    # groupby var needs to be cateogorical so all rows return data even if there is none, which needs to be categorical dtype
    ## but categorical dtype cols cannot be sorted
    ## create a copy var and set that as categorical type
    df['copy'] = df[col_name]
    df['copy'] = df['copy'].astype('category') 

    # groupby analysis
    df = df.groupby([col_name, 'copy', index_col]).agg(aggregations)

    # reset the index
    df.reset_index(inplace=True)
    # get rid of duplicates by only keeping rows where the copy matches the col_name
    df = df[df[col_name]==df['copy']]
    # drop the copy column
    df.drop(columns=['copy'],inplace=True)
    # set the index back to just col_name
    df.set_index([col_name, index_col], inplace=True)

    # if you do not have col_mapping but you do have col_order
    if col_mapping == None and col_order != None:
        # reset index to manipulate it
        df.reset_index(inplace=True)   
        # sort by col_order values ascending 
        df.sort_values(by=[col_name], inplace=True)
        # set index back in place
        df.set_index([col_name], inplace=True)
        # create dictionary to return order name to original data labels
        index_rename = {v: k for k, v in col_order.items()}
        # rename index values with dictionary
        df.rename(index=index_rename, level=0, inplace=True)
    else:
        pass

    # setting names of stats columns
    df.columns = stats_names

    # reshaping data

    # code to convert from wide to long format, and then pivot so the timepoints are columns and locs are rows

    # the index must be reset to a normal column in order to manipulate the data
    ## the inplace argument makes the change persist
    df.reset_index(inplace=True)

    # wide to long format--must be done before a pivot is possible
    ## will output a dataframe that gives a row with the index column and stats per index category per col_name category
    df = pd.melt(df, 
                 id_vars=[col_name, index_col], 
                 value_vars=stats_names)

    # pivot--will assign the col_name values to the columns and the index values to the index row
    df = df.pivot(index=index_col,columns=[col_name, 'variable'],values='value')

    # replace nulls with 0
    if null_to_0 == None:
        pass
    else:
        for null_col_name in null_to_0:
            for col_num, df_col_name in enumerate(df.columns):
                if null_col_name == df_col_name:
                    df[df_col_name] = df[df_col_name].fillna(0)   
    
    # cleaning up dataframe

    # this will resort the columns to keep header level 0 values together
    ## list of columns in desired order created with sorted()
    col_names = sorted(df)
    ## column ordering list applied to dataframe with reindex()
    df = df.reindex(columns=col_names)
    ## making our stats columns in the right order after using sorted() to fix level 0
    df = df.reindex(stats_names, axis=1, level=1)
   
    # relabeling columns without leading number
   
    if col_mapping == None:    
        pass
    elif col_order == None:
        pass
    else:        
        # reverse key and value pairs of col_order dict
        col_rename = {v: k for k, v in col_order.items()}  
        # putting labels back on columns
        df.rename(columns=col_rename, inplace=True) 

    # renaming index values
    if index_mapping == None:
        pass 
    else:
        # if your index name is also the name of one of your columns (ex: you took a count of your groupby var)
        if df.index.name in list(df.columns):
            # temporarily rename results col
            df.rename(columns={index_col:'temp'}, inplace=True)
            # reset the index for manipulation
            df.reset_index(inplace=True)
            # map the desired values onto your index
            df[index_col] = df[index_col].map(index_mapping)
            # set the index back
            df.set_index(index_col, inplace=True)
            # change the name of the column back
            df.rename(columns={'temp':index_col}, inplace=True)
        else:
           # reset the index for manipulation
            df.reset_index(inplace=True)
            # map the desired values onto your index
            df[index_col] = df[index_col].map(index_mapping)
            # set the index back
            df.set_index(index_col, inplace=True)

    # # code to order the index values in the order they are meant to be in for visualization and reporting

    if index_order == None:
        pass
    else:
        # must reset index to manipulate it
        df.reset_index(inplace=True)
        # assigning int value to each index value in ascending order
        mapping = {index_order: i for i, index_order in enumerate(index_order)}
        # mapping those int values onto the index variable to create a key
        key = df[index_col].map(mapping)
        # reordering the dataframe by the key
        df = df.iloc[key.argsort()]
        # setting index
        df.set_index(index_col, inplace=True)

    # set index name
    if index_name == None:
        pass
    else:
        df.index.name = index_name

    # remove unused extra levels
    ## create column object with unused levels removed
    header_cols = df.columns.remove_unused_levels()
    # assign that to the df columns
    df.columns = header_cols

    return df


#####      TWO COL ROW MULTIINDEX AND DOUBLE HEADER ROW              #####


def col_pivot_row_multiindex_dbl_header_results(df, col_name, index1_col, index2_col, stats_names, aggregations, col_mapping=None, col_order=None, \
    index1_mapping=None, index1_ordered_list=None, index1_name=None, index2_mapping=None, index2_ordered_list=None, index2_name=None, \
    null_to_0=None, reorder_row_indices=True):

    # this function will perform an analysis of the data by the col_name and index_col with specificed aggregations 
    # and groupby by col_name and index_col
    ## it will then reshape and clean the results table to a report-ready format

    ## this function should be used when you have data in a column you wish to analyze by and then pivot so each
    ## data label in that column is the heading of a column 
    ###       ex: FY19-20, FY20-21, etc. that were in a single 'Fiscal Year' column
    ## the data should also have values in two index columns
    ###       ex: months in a month column for 'Jan', 'Feb', 'Mar' etc and departments in department column for 'oncology', 'cardiac' etc
     
    # ARGUMENTS
    
    ## MANDATORY:
    ### df is your dataframe to be analyzed
    ### col_name is the column to perform the groupby with and whose values will become your column headers    
    ### index1_col is the column that will be your first row index
    ### index2_col is the column that will be your second row index
    ### stats_names is your list of what each analysis should be called in your able, in desired order
    ####        MUST be in the same order as your aggregations!
    ### aggregations is the dictionary containing your analyses for the groupby, in desired order
    ####        MUST be in the same order as stats_names!
    
    ## OPTIONAL:
    ### col_mapping is the dictionary to map your data values in col_name to you desired labels
    ### col_order is the dictionary to map your desired labels to their desired order
    ####    if you are using col_mapping, this dcitionary MUST match the new names!
    ### index1_mapping is the dictionary to map your first index col names in your data to their desired labels
    ### index1_ordered_list is the list of your first index values in the order you want them to be in
    ####    if you are changing your first index values with index1_mapping, they MUST match the new values!
    ### index1_name is the name of your first index
    ### index2_mapping is the dictionary to map your second index col names in your data to their desired labels
    ### index2_ordered_list is the list of your second index values in the order you want them to be in
    ####    if you are changing your second index values with index2_mapping, they MUST match the new values!
    ### index2_name is the name of your second index  
    ### null_to_0 is your list of columns (matching stats_names) to convert nulls to 0s. defaults to None    
    ### reorder_row_indices will reorder your results df in ascending order for both indices. defaults to True
    ####        this will reorder accourding to index_ordered_list (when present) and index2_ordered_list (when present)      

    import pandas as pd

    if reorder_row_indices == True and index1_ordered_list == None:
        index1_ordered_list = [value for value in pd.unique(df[index1_col])]
    else:
        pass 

    if reorder_row_indices == True and index2_ordered_list == None:
        index2_ordered_list = [value for value in pd.unique(df[index2_col])]
    else:
        pass 

    # set up ordering for the pivot column

    ## map the labels to the data values for the column
    if col_mapping == None:
        pass 
    else:
        # create a name_col with label values
        df = df.assign(name_col=df[col_name].apply(lambda x: col_mapping[x]))

    ## map the order to the label values for the column
    if col_order == None and col_mapping == None:
        pass
    elif col_mapping == None:
        # if there is not col mapping but is col order, create a order_col based off col from raw data
        df = df.assign(order_col=df[col_name].apply(lambda x: col_order[x]))
    elif col_order != None:
        # if there is a col mapping and col ordering, create order_col based off mapping
        df = df.assign(order_col=df.name_col.apply(lambda x: col_order[x]))
    else:
        pass

    ## drop the orginal data column and the one with names--will be remapped to the order column later
    if col_mapping == None:
        pass 
    else:
        # if there is no col order
        if col_order == None:
            # rename original column (code will break if you don't)
            df.rename(columns={col_name:'temp'}, inplace=True)
            # rename name_col to orignal column name
            df.rename(columns={'name_col':col_name}, inplace=True)
            # drop old column
            df.drop(columns=['temp'], inplace=True)
        else:
            # if there IS an order column, then drop name_col
            df.drop(columns=['name_col'], inplace=True)
    
    ## rename order column to original col name--order column should take precedence if it exists
    if col_order == None:
        pass 
    else:
        # rename original column (code will break if you don't)
        df.rename(columns={col_name:'temp'}, inplace=True)
        # rename order_col to orignal column name
        df.rename(columns={'order_col':col_name}, inplace=True)
        # drop old column
        df.drop(columns=['temp'], inplace=True)

    # groupby var needs to be cateogorical so all rows return data even if there is none, which needs to be categorical dtype
    ## but categorical dtype cols cannot be sorted
    ## create a copy var and set that as categorical type
    df['copy'] = df[col_name]
    df['copy'] = df['copy'].astype('category') 

    # groupby analysis
    df = df.groupby([col_name, 'copy', index1_col, index2_col]).agg(aggregations)

    # reset the index
    df.reset_index(inplace=True)
    # get rid of duplicates by only keeping rows where the copy matches the col_name
    df = df[df[col_name]==df['copy']]
    # drop the copy column
    df.drop(columns=['copy'],inplace=True)
    # set the index back to just col_name
    df.set_index([col_name, index1_col, index2_col], inplace=True)    

    # if you do not have col_mapping but you do have col_order
    if col_mapping == None and col_order != None:
        # reset index to manipulate it
        df.reset_index(inplace=True)   
        # sort by col_order values ascending 
        df.sort_values(by=[col_name], inplace=True)
        # set index back in place
        df.set_index([col_name], inplace=True)
        # create dictionary to return order name to original data labels
        index_rename = {v: k for k, v in col_order.items()}
        # rename index values with dictionary
        df.rename(index=index_rename, level=0, inplace=True)
    else:
        pass

    # setting names of stats columns
    df.columns = stats_names

    # reshaping data

    # code to convert from wide to long format, and then pivot so the timepoints are columns and locs are rows

    # the index must be reset to a normal column in order to manipulate the data
    ## the inplace argument makes the change persist
    df.reset_index(inplace=True)

    # wide to long format--must be done before a pivot is possible
    ## will output a dataframe that gives a row with the index column and stats per index category per col_name category
    df = pd.melt(df, 
                 id_vars=[col_name, index1_col, index2_col], 
                 value_vars=stats_names)

    # pivot--will assign the col_name values to the columns and the index values to the index row
    df = df.pivot(index=[index1_col, index2_col],columns=[col_name, 'variable'],values='value')

    # replace nulls with 0
    if null_to_0 == None:
        pass
    else:
        for null_col_name in null_to_0:
            for col_num, df_col_name in enumerate(df.columns):
                if null_col_name in df_col_name:
                    df[df_col_name] = df[df_col_name].fillna(0)
            
    
    # cleaning up dataframe

    # this will resort the columns to keep header level 0 values together
    ## list of columns in desired order created with sorted()
    col_names = sorted(df)
    ## column ordering list applied to dataframe with reindex()
    df = df.reindex(columns=col_names)
    ## making our stats columns in the right order after using sorted() to fix level 0
    df = df.reindex(stats_names, axis=1, level=1)
   
    # relabeling columns without leading number
   
    if col_mapping == None:    
        pass
    elif col_order == None:
        pass
    else:        
        # reverse key and value pairs of col_order dict
        col_rename = {v: k for k, v in col_order.items()}  
        # putting labels back on columns
        df.rename(columns=col_rename, inplace=True) 

    # renaming index values
    if index1_mapping == None:
        pass 
    else:
        # if your index name is also the name of one of your columns (ex: you took a count of your groupby var)
        if df.index.names[0] in list(df.columns):
            # temporarily rename results col
            df.rename(columns={index1_col:'temp'}, inplace=True)
            # reset the index for manipulation
            df.reset_index(inplace=True)
            # map the desired values onto your index
            df[index1_col] = df[index1_col].map(index1_mapping)
            # set the index back
            df.set_index([index1_col, index2_col], inplace=True)
            # change the name of the column back
            df.rename(columns={'temp':index1_col}, inplace=True)
        else:
           # reset the index for manipulation
            df.reset_index(inplace=True)
            # map the desired values onto your index
            df[index1_col] = df[index1_col].map(index1_mapping)
            # set the index back
            df.set_index([index1_col, index2_col], inplace=True)

    if index2_mapping == None:
        pass 
    else:
        # if your index name is also the name of one of your columns (ex: you took a count of your groupby var)
        if df.index.names[1] in list(df.columns):
            # temporarily rename results col
            df.rename(columns={index2_col:'temp'}, inplace=True)
            # reset the index for manipulation
            df.reset_index(inplace=True)
            # map the desired values onto your index
            df[index2_col] = df[index2_col].map(index2_mapping)
            # set the index back
            df.set_index([index1_col, index2_col], inplace=True)
            # change the name of the column back
            df.rename(columns={'temp':index2_col}, inplace=True)
        else:
           # reset the index for manipulation
            df.reset_index(inplace=True)
            # map the desired values onto your index
            df[index2_col] = df[index2_col].map(index2_mapping)
            # set the index back
            df.set_index([index1_col, index2_col], inplace=True)
    
    # # code to order the index values in the order they are meant to be in for visualization and reporting

    # reordering row indices
    if reorder_row_indices == False:
        pass 
    else:
        # must reset index to manipulate it
        df.reset_index(inplace=True)
        
        # assigning int value to each index value in ascending order
        mapping1 = {index_order: i for i, index_order in enumerate(index1_ordered_list)}
        mapping2 = {index_order: i for i, index_order in enumerate(index2_ordered_list)}
        
        # mapping those int values onto the index variable to create a key
        key1 = df[index1_col].map(mapping1)
        key2 = df[index2_col].map(mapping2)
        
        # reordering the dataframe by the key
        ## creating int columns based off key values (iloc method will not work for multiindex reordering more than one of the indices)
        df['key1'] = key1
        df['key2'] = key2

        # sorting by the new columns and then dropping them
        df.sort_values(by=['key1','key2'], inplace=True)
        df.drop(['key1','key2'], axis=1, inplace=True)
        # setting the index back
        df.set_index([index1_col, index2_col], inplace=True)
    
    # renaming indices
    if index1_name == None:
        pass 
    else:
        df.index.set_names(index1_name, level=0, inplace=True)

    if index2_name == None:
        pass 
    else:
        df.index.set_names(index2_name, level=1, inplace=True)


    # remove unused extra levels
    ## create column object with unused levels removed
    header_cols = df.columns.remove_unused_levels()
    # assign that to the df columns
    df.columns = header_cols

    return df