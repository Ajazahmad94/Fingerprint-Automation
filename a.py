import pyodbc
import numpy as np
import pandas as pd
from datetime import datetime
import time



def PassNull(value): 
    if value is None:
        return "Null"
    else: 
        if  type(value)==str or 'Timestamp' in str(type(value)) or 'datetime.time' in str(type(value)) :
            return "'{0}'".format(value)
        else:
            return "{0}".format(value)
            
def PostData():
    current_date = datetime.now()
    df_marks = pd.read_csv('E:/log.csv')  #log file
    df_marks= df_marks[['Date','Update']]
    Update='failed'

    #Connecting Database
    cnxn = pyodbc.connect('Provider=MSDASQL;Driver={MySQL ODBC 5.1 Driver};Server=127.0.0.1;port=17770;Database=adms_db;User=root;Password=;Option=3;')
    checkinout= pd.read_sql("select * from checkinout;",cnxn)
    userinfo= pd.read_sql("select userid,badgenumber from userinfo;",cnxn)
    iclock= pd.read_sql("select * from iclock;",cnxn)
    cnxn.close()

    #Connecting Database
    employdb = pyodbc.connect("DRIVER={SQL server};SERVER=192.168.3.190;DATABASE=meksoftdb1;UID=sa;PWD=")
    emp_names= pd.read_sql("select id, nameAr from employee;",employdb)
    employdb.close()
    fingerPrintCtx = pyodbc.connect("DRIVER={SQL server};SERVER=192.168.3.190;DATABASE=fingerprintdb;UID=sa;PWD=")

    userinfo['badgenumber']=userinfo['badgenumber'].astype(int)
    userinfo['id']=userinfo['badgenumber']

    #Merging/Joining
    df= pd.merge(checkinout, (pd.merge(emp_names, userinfo, on='id' )), on='userid')
    data= pd.merge(df, iclock, on ='SN')
    fingerprint= data[['nameAr','checktime', 'badgenumber','Alias', 'DeptID','UserCount', 'DeviceName']] #Dropping unwanted coulmns
    fingerprint = fingerprint.where(pd.notnull(fingerprint), None)

    #Truncating before adding the data Again
    cur=fingerPrintCtx.cursor()
    cur.execute("TRUNCATE TABLE fingerprint") 

            
    length=len(fingerprint['nameAr'])
    try:
        for index in range(length):
            r = fingerprint.iloc[index]
            insertStatement= '''
            INSERT INTO [dbo].[fingerprint]([checktime] ,[nameAr],[badgenumber],[Alias],[DeptID],[UserCount],[DeviceName])VALUES ({0},{1},{2},{3},{4},{5},{6})
            '''.format(PassNull(r['checktime']),PassNull(r['nameAr']),PassNull(r['badgenumber']),PassNull(r['Alias']),PassNull(r['DeptID']),PassNull(r['UserCount']),PassNull(r['DeviceName']))
            fingerPrintCtx.execute(insertStatement)

        fingerPrintCtx.commit()
        print("We have successfully posted the data")
    except:   
        print("Failed to Update the Database ")
    finally: 
        fingerPrintCtx.close()
        Update='sucess'
        df_marks = df_marks.append({'Date':current_date,'Update':Update}, ignore_index=True)
        df_marks.to_csv('log.csv')



if __name__=="__main__":
    while(True):
        PostData()
        print("Data posted. {0}".format(datetime.now()))
        time.sleep(1800)