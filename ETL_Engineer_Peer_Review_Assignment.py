#!/usr/bin/env python
# coding: utf-8

# <p style="text-align:center">
#     <a href="https://skills.network/?utm_medium=Exinfluencer&utm_source=Exinfluencer&utm_content=000026UJ&utm_term=10006555&utm_id=NA-SkillsNetwork-Channel-SkillsNetworkCoursesIBMDeveloperSkillsNetworkPY0221ENSkillsNetwork23455645-2022-01-01" target="_blank">
#     <img src="https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/assets/logos/SN_web_lightmode.png" width="200" alt="Skills Network Logo"  />
#     </a>
# </p>
# 

# # Peer Review Assignment - Data Engineer - ETL
# 

# Estimated time needed: **20** minutes
# 

# ## Objectives
# 
# In this final part you will:
# 
# *   Run the ETL process
# *   Extract bank and market cap data from the JSON file `bank_market_cap.json`
# *   Transform the market cap currency using the exchange rate data
# *   Load the transformed data into a seperate CSV
# 

# For this lab, we are going to be using Python and several Python libraries. Some of these libraries might be installed in your lab environment or in SN Labs. Others may need to be installed by you. The cells below will install these libraries when executed.
# 

# In[21]:


get_ipython().system('mamba install pandas==1.3.3 -y')
get_ipython().system('mamba install requests==2.26.0 -y')


# ## Imports
# 
# Import any additional libraries you may need here.
# 

# In[22]:




import glob
import requests
import pandas as pd
from pandas.io.json import json_normalize
from datetime import datetime


# As the exchange rate fluctuates, we will download the same dataset to make marking simpler. This will be in the same format as the dataset you used in the last section
# 

# In[23]:


get_ipython().system('wget https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMDeveloperSkillsNetwork-PY0221EN-SkillsNetwork/labs/module%206/Lab%20-%20Extract%20Transform%20Load/data/bank_market_cap_1.json')
get_ipython().system('wget https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMDeveloperSkillsNetwork-PY0221EN-SkillsNetwork/labs/module%206/Lab%20-%20Extract%20Transform%20Load/data/bank_market_cap_2.json')
get_ipython().system('wget https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMDeveloperSkillsNetwork-PY0221EN-SkillsNetwork/labs/module%206/Final%20Assignment/exchange_rates.csv')


# ## Extract
# 

# ### JSON Extract Function
# 
# This function will extract JSON files.
# 

# In[24]:


def extract_from_json(file_to_process):
    dataframe = pd.read_json(file_to_process)
    dataframe.head()
    return dataframe


# ## Extract Function
# 
# Define the extract function that finds JSON file `bank_market_cap_1.json` and calls the function created above to extract data from them. Store the data in a `pandas` dataframe. Use the following list for the columns.
# 

# In[52]:


columns=['Name','Market Cap (US$ Billion)']


# In[53]:


def extract():
    # Write your code here
   # extracted_data=pd.DataFrame(columns=['Name','Market Cap (US$ Billion)'])
   # for jsonfile in glob.glob("*.json"):
   #     extracted_data=extract_from_json(jsonfile)
        
    #return extracted_data
    
    extracted_data = pd.DataFrame(extract_from_json('bank_market_cap_1.json'),columns=['Name','Market Cap (US$ Billion)'])
    return extracted_data


# <b>Question 1</b> Load the file <code>exchange_rates.csv</code> as a dataframe and find the exchange rate for British pounds with the symbol <code>GBP</code>, store it in the variable  <code>exchange_rate</code>, you will be asked for the number. Hint: set the parameter  <code>index_col</code> to 0.
# 

# In[54]:


# Write your code here
df=pd.read_csv("exchange_rates.csv")

#df.rename( columns={'Unnamed: 0':'Pounds'}, inplace=True )
#df1=df.loc[df["Pounds"] == 'GBP']
#df1.head()

df.columns = ['Currency', 'Rates']
df.head()


# In[55]:


exchange_rate = df[df['Currency'] == 'GBP']['Rates'].values[0]
exchange_rate


# In[56]:


temp = pd.read_json('bank_market_cap_1.json')
temp['Market Cap (GBP$ Billion)'] = temp['Market Cap (US$ Billion)'].apply(lambda x: x* exchange_rate)
temp.head()


# ## Transform
# 
# Using <code>exchange_rate</code> and the `exchange_rates.csv` file find the exchange rate of USD to GBP. Write a transform function that
# 
# 1.  Changes the `Market Cap (US$ Billion)` column from USD to GBP
# 2.  Rounds the Market Cap (US$ Billion)\` column to 3 decimal places
# 3.  Rename `Market Cap (US$ Billion)` to `Market Cap (GBP$ Billion)`
# 

# In[57]:


def transform(data):
    # Write your code here

    data['Market Cap (US$ Billion)']= round(data['Market Cap (US$ Billion)']*exchange_rate,3)
    #print(data['Market Cap (US$ Billion)'])
    data=data.rename(columns={"Market Cap (US$ Billion)":"Market Cap (GBP$ Billion)"},inplace=False)
    return data


# ## Load
# 
# Create a function that takes a dataframe and load it to a csv named `bank_market_cap_gbp.csv`. Make sure to set `index` to `False`.
# 

# In[58]:


targetfile="bank_matket_cap_gbp.csv"
def load(targetfile,datatoload):
    # Write your code here
    datatoload.to_csv(targetfile, index=False)


# ## Logging Function
# 

# Write the logging function <code>log</code> to log your data:
# 

# In[59]:


def log(message):
    # Write your code here
    timestamp_format='%Y-%h-%d-%H%M%S'
    now=datetime.now()
    timestamp=now.strftime(timestamp_format)
    with open("logfile.txt","a") as f:
        f.write(timestamp+','+message+'\n')
    


# ## Running the ETL Process
# 

# Log the process accordingly using the following <code>"ETL Job Started"</code> and <code>"Extract phase Started"</code>
# 

# In[60]:


# Write your code here
log("ETL Job Started")


# ### Extract
# 

# <code>Question 2</code> Use the function <code>extract</code>, and print the first 5 rows, take a screen shot:
# 

# In[61]:




# Call the function here
log("Extract Phase Started")
extract_data=extract()
# Print the rows here
extract_data.head(5)


# Log the data as <code>"Extract phase Ended"</code>
# 

# In[62]:


# Write your code here
extracted_data = extract()
log("Extract phase Ended")


# ### Transform
# 

# Log the following  <code>"Transform phase Started"</code>
# 

# In[63]:


# Write your code here
log("Transform phase started")


# <code>Question 3</code> Use the function <code>transform</code> and print the first 5 rows of the output, take a screen shot:
# 

# In[64]:


# Call the function here

# Print the first 5 rows here
transformed_data = transform(extracted_data)
transformed_data.head(5)


# Log your data <code>"Transform phase Ended"</code>
# 

# In[65]:


# Write your code here
transformed_data = transform(extracted_data)
log("Transform phase Ended")


# ### Load
# 

# Log the following `"Load phase Started"`.
# 

# In[66]:


# Write your code here

log("Load phase Started")


# Call the load function
# 

# In[67]:


# Write your code here
load(targetfile,transformed_data)


# Log the following `"Load phase Ended"`.
# 

# In[68]:


# Write your code here
log("Load phase Ended")

log("ETL Job Ended")


# ## Authors
# 

# Ramesh Sannareddy, Joseph Santrcangelo and Azim Hirjani
# 

# ### Other Contributors
# 

# Rav Ahuja
# 

# ## Change Log
# 

# | Date (YYYY-MM-DD) | Version | Changed By        | Change Description                 |
# | ----------------- | ------- | ----------------- | ---------------------------------- |
# | 2020-11-25        | 0.1     | Ramesh Sannareddy | Created initial version of the lab |
# 

# Copyright © 2020 IBM Corporation. This notebook and its source code are released under the terms of the [MIT License](https://cognitiveclass.ai/mit-license?utm_medium=Exinfluencer&utm_source=Exinfluencer&utm_content=000026UJ&utm_term=10006555&utm_id=NA-SkillsNetwork-Channel-SkillsNetworkCoursesIBMDeveloperSkillsNetworkPY0221ENSkillsNetwork23455645-2022-01-01&cm_mmc=Email_Newsletter-\_-Developer_Ed%2BTech-\_-WW_WW-\_-SkillsNetwork-Courses-IBM-DA0321EN-SkillsNetwork-21426264&cm_mmca1=000026UJ&cm_mmca2=10006555&cm_mmca3=M12345678&cvosrc=email.Newsletter.M12345678&cvo_campaign=000026UJ).
# 
