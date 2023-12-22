from pydantic import BaseModel
from typing import List,Dict
from sqlalchemy.orm import Session
from sqlalchemy import text,inspect
import pandas as pd
import numpy as np
import datetime
import calendar
import os
pd.options.mode.chained_assignment = None 

import numpy as np

from core.database import  get_db
from core.database.tables.users_tables import Users_tables
from core.database.tables.budgetitems import BudgetItem
from core.database.tables.Budget_test import BudgetItemtest
from .schemas import  Filters
from .query import budget_query,get_max_session_id,get_table_name
import traceback


    

class GloabalVariables(BaseModel):
    columns: List[str]
    max_col: List[str]
    sum_col: List[str]
    avg_col: List[str]
    int_cols: List[str]
    float_cols: List[str]
    heirarchy: List[str]
    tabs: Dict[str,List[str]]

class Parameters:


    db_columns = ["Channel",'country',"Region",'area','city',"StoreId","Store",'season',"Department","Family","SubFamily","Category",
    "DOM_COMM","Sub-Category","Extended-Sub-Category","Supplier","Sub-Category-Supplier",
    "Assembly code-Nickname","Status","END OF Life","Description","ItemLookupCode","HistoricalYear","BudgetYear",'Month','Week',"BDate",
    "Budget Amount", "Deficit","Revised Budget", "Budget Cost", "Budget%","BudgetVAct/frcast", "BudgetVAct/Frcst Percentage","BudgetVPY%","BudgetVPPY%",                # ---> For df
    "Units Buy By SKU","TotalSkuCount", 
    "Net Sales LY","PPrev netsale amount", "Netsale amount","Sales Actuals","SoldDate",
    "Quantity Actuals","Cost Actuals","Cost Of Goods LY","NtSoldQty-LY",
    "Cost Of Goods LLY","Quantity PPY","Stock OnHand Qty", "Units Buy By SKU LY", "Sales Actual/Forecast TY", "budget_amount_Ly", "Sales Actual/Forecast LY", 
    "Sales Actual/Forecast LLY", "Budget Gross Margin %"]


# NetSalesActual-TY --> Sales Actual
# prev_netsale_amount/NetSalesActual-LY -->Net Sales LY           
# NetSoldQuantityActuals-TY --> Quantity Actuals
# NetSoldCostActuals-TY --> Cost Actuals
# p_prev_netsale_amount/NetSalesActual-LLY --> PPrev netsale amount
# NetSoldQuantity-PPY --> Quantity PPY
# CostOfGoodsLY/NSOLDCOST-LY --> Cost Of Goods LY
# CostOfGoodsLLY/NSOLDCOST-LLY --> Cost Of Goods LLY
# BudgetCostTY --> Budget Cost --> Budget Amount LY
                # "Net Sales LY",,,,
                # "Net Sales LLY",
# Units Buy By Sku - Budget Qty

    columns = ["Channel",'country',"Region",'area','city',"StoreId","Store",'season',"Department","Family","SubFamily","Category",
    "DOM_COMM","Sub-Category","Extended-Sub-Category","Supplier","Sub-Category-Supplier", "Assembly code-Nickname","Status","END OF Life",
    "Description","ItemLookupCode","HistoricalYear","BudgetYear",'Month','Week',"BDate",
    "Budget Amount", "Deficit","Revised Budget", "Budget%", "BudgetVAct/frcast", "BudgetVAct/Frcst Percentage","BudgetVPY%","BudgetVPPY%",
    "Units Buy By Sku","TotalSkuCount", "Net Sales LY","PPrev netsale amount",
    "Netsale amount","Sales Actuals","SoldDate","Quantity Actuals","Cost Actuals","Cost Of Goods LY","NtSoldQty-LY",
    "Cost Of Goods LLY","Quantity PPY","StockOnHandQty", "Units Buy By SKU LY", "Sales Actual/Forecast TY", "Budget Cost",  
    "Net Sales Act/Forecast vs Budget % TY", "Net Sales Mix % LY", "Net Act/Forecast vs LY %", "Act/Forecast vs PPY %", 
    "Sales Actual/Forecast LY", "Sales Actual/Forecast LLY"
    # "Net Sold Act/Fct Qty LLY" ~~ cols after calculating df is different because "Net Sold Act/Fct Qty LLY" is ky err after put for aggregation
    ]
# ,"DOM COMM","Department"

    other_cols = ["LY Margin%","LLY Margin%", "Net Sales Mix % TY", "Sales Act/Forecast TY"]

# max_col, sum_col and avg_cols are for aggregation

#For line 327
    max_col = ["Channel","Family","SubFamily","Category","Sub-Category","Extended-Sub-Category","Supplier",
                 "Sub-Category-Supplier","Assembly code-Nickname",
                "Status","Description","END OF Life","ItemLookupCode","Store",
                'History month','History week',"History_date",'History quarter',"History day","Budget quarter",
                'Budget month','Budget week','Budget day','Budget date', "Sales Act/Forecast TY"]
                
                
# For __init__
    sum_col =  ["Budget Amount","Revised Budget", 'Budget%',"Units Buy By SKU","Total SKU Count",
                "Total SKU Count LY","Net Sales LY","PPrev netsale amount",
                "Net Sales Mix % TY", "Netsale amount","Sales Actuals",
                "Quantity Actuals","NtSoldQty-LY", "Quantity PPY",
                #Qty tb Columns
                # No need to be in this
                "Budget Quantity Mix %", "Budget QTY vs Act/Forecast QTY%", "Budget QTY vs Net sld QTY PY%",
                "Budget QTY vs Net sld QTY PPY%", "Resvised Budget Quantity %", "Resvised Budget Quantity vs Act/Frcst Qty %",
                # No need to be in this
                "Net Sold Qty Act/Fct TY", 
                "Quantity Mix%","Quantity Act/Forecast","Quantity Act/Forecast vs Budget%",
                "Quantity Act or Forecast /SKU", "Cost Actuals","Budget Cost","Cost/SKU","Cost Mix%",
                "Cost Act or Forecast /SKU","Cost Act/Forecast", "Cost Budget/SKU", "Margin Actuals",
                "Margin Act or Forecast /SKU","Margin Mix%","Margin Act/Forecast","Budget Margin%",
                "Budget Margin Mix%","Units Buy By SKU LY","Cost Of Goods LY",
                "Cost Of Goods LLY", "Sales Act/Forecast TY", "budget_amount_Ly", "Sales Actual/Forecast LY",
                #For qty Tab
                ]

    # 'Sales Act/Forecast',
    # "Budget Amount LY",
    #,"Budget Cost%"

    # From Quantity tab --> "Budget QTY%",

    avg_col = ["Deficit", 
                "Quantity Act/Forecast vs PPY%","Budget vs PY QTY%","Budget vs PPY QTY%",
                "Budget/SKU QTY",
                "Budget vs Act/Forecast Cost%","Budget vs PY Cost%","Budget vs PPY Cost%","Cost Act/Forecast vs Budget%","Cost Act/Forecast vs LY%",
                "Cost Act/Forecast vs PPY%","Cost Act/Forecast vs PPY %",
                "Quantity Act/Forecast vs LY%",
                
                "Budget Gross Margin %","Margin Act/Forecast vs PPY%","Margin Act/Forecast vs LY%","Margin Act/Forecast vs PPY%","Margin Act/Forecast vs LY%"
                ,"Margin Budget/SKU","Budget vs PY Margin%","LY Margin%","LLY Margin%", "Sales Actual/Forecast TY", "Net Sales Act/Forecast vs Budget % TY", 
                "Net Sales Mix % LY", "Net Act/Forecast vs LY %", "Act/Forecast vs PPY %", "Sales Actual/Forecast LLY"
                # For Qty tab
                
                ]

#Cols Taken out from Avg columns
# "Budget Gross Margin%"
# "Budget/SKU"
# 'Retail_Price'
# "Units/SKU LY"
# "Units/SKU",
# "Budget vs Act/Forecast %"
# "Budget vs PY%"
# "Budget vs PPY%"

    
    int_cols = ["Units Buy By SKU","Total SKU Count","Units Buy By SKU LY","Total SKU Count LY", "Revised Budget"]

    float_cols = ["Deficit",'Budget%', "Sales Actuals", "Net Sales Mix % TY", "Sales Actual/Forecast TY", 
                "budget_amount_Ly", "Net Sales Act/Forecast vs Budget % TY","Net Sales Mix % LY", "Net Act/Forecast vs LY %"]

# 'RelativeBudget%',
# 'Sales Act/Forecast' --> "Sales Actual/Forecast TY"
# "Budget vs Act/Forecast %"
# "Budget vs PY%",
# "Budget vs PPY%"
# "Retail_Price",
# "Units/SKU",
# "Units/SKU LY",

    heirarchy = ["Channel","Family","SubFamily","Category","Sub-Category","Extended-Sub-Category","Supplier","Sub-Category-Supplier"
                ,"Assembly code-Nickname","Status","END OF Life","ItemLookupCode"]

    editable_cols = ["Budget Amount","Budget%","Units/SKU","Budget/SKU","Budget vs PY%","Budget vs PPY%","Units Buy By SKU"
                    ,"Act/Forecast vs Budget%"]

# string = 0, bool = 1, float=2, int = 3 , date = 4
    budget_cols = {'ItemLookupCode':0,'LookupCode':0, 'Description':0, 'Department':0, 'CategoryName':0,
                    'Family':0, 'subFamily':0, 'Supplier':0, 'SubCategory':0, 'ExtendedSubCategory':0, 'SubCategorySupplier':0, 
                    'AssemblyCodeNickname':0, 'Status':1, 'ENDOFLife':0, 'Price':2, 'Price_Without_Tax':0,'AVCost':2, 
                    'NewItemLookupCode':0, 'DOM_COMM':0, 'ItemType':0, 'Company_ABC':0, 'Family_ABC':0, 'TenentID':0, 'BTFQTY':3, 
                     'Channel':0,'StoreId':3,'StoreName':0,'BDate':4, 'BudgetValue':2, 'BudgetCost':2, 'BudgetQTY':3}

    save_table_keys = ["ItemLookupCode","Channel","StoreId","Description","Department","Category","Family","SubFamily","Supplier","Sub-Category","Extended-Sub-Category",
                                "Sub-Category-Supplier","Assembly code-Nickname","END OF Life","DOM COMM","Store","Status","Budget date","Units Buy By SKU","Budget Amount","Budget Cost","Retail_Price"]
    db_table_keys = ["ItemLookupCode","Channel","StoreId","Description","Department","CategoryName","Family","subFamily","Supplier","SubCategory","ExtendedSubCategory",
                                "SubCategorySupplier","AssemblyCodeNickname","ENDOFLife","DOM_COMM","StoreName","Status","BDate","BudgetQTY","BudgetValue","BudgetCost","Price"]

    

    quantity_cols = ["Budget Quantity Mix %",
                    "Budget QTY vs Act/Forecast QTY%", "Budget QTY vs Net sld QTY PY%", "Budget QTY vs Net sld QTY PPY%",
                    "Resvised Budget Quantity %", "Resvised Budget Quantity vs Act/Frcst Qty %", "Net Sold Qty Act/Fct TY",
                    "Net Sold Qty Act/Fcst Mix % TY", "Net Sold Qty Act/Fcst vs Budget % TY", "Net Sold Quantity Sales Deficit",
                    # prev module columns
                    "Budget vs PY QTY%","Budget vs PPY QTY%",
                    "Budget/SKU QTY","Quantity Mix%",
                    "Quantity Act/Forecast","Quantity Act/Forecast vs Budget%","Quantity Act/Forecast vs LY%",
                    "Quantity Act or Forecast /SKU","Quantity Act/Forecast vs PPY%"]
    
    # From quantity tabs --> "Budget QTY%",

    # ra_quantity is for dsply in ft-nd
    ra_quantity = [
                    # Genus Columns
                    "Channel",'country',"Region",'area','city',"StoreId",
                    "Store",'season',"Department","Family","SubFamily","Category","DOM COMM","Sub Category",
                    "Extended SubCategory","Supplier","SubCategory Supplier","Assembly Code-NickName","Status","END OF Life",
                    "Description","ItemLookupCode",
                    # tab columns
                    # Bud Qty cols in bud tabs
                    "Units Buy By SKU",
                    "Budget Quantity Mix %", "Budget QTY vs Net sld QTY PY%", 
                    "Budget QTY vs Net sld QTY PPY%", "Resvised Budget Quantity %", "Resvised Budget Quantity vs Act/Frcst Qty %",
                    
                    # sold Qty cols in qty tabs
                    "Quantity Actuals", "Net Sold Quantity Mix % TY", "Net Sold Qty Act/Fct TY", "Net Sold Qty Act/Fcst Mix % TY",
                    "Net Sold Qty Act/Fcst vs Budget % TY", "Net Sold Quantity Sales Deficit", "NtSoldQty-LY", "Net Act/Fct Qty vs LY %", 
                    "Quantity PPY", "Act/Forecast vs PPY %"
                    # Prev occupied cols
                    # "Budget QTY vs Act/Forecast QTY%","Budget vs PY QTY%","Budget vs PPY QTY%",
                    # "Retail_Price","Units Buy/SKU","Total SKU Count","Units/SKU","Budget/SKU QTY","Quantity Mix%",
                    #  "NtSoldQty-LY", 
                    
                    # "Quantity Act/Forecast","Budget Amount","Quantity Act/Forecast vs Budget%","Quantity Act/Forecast vs LY%",
                    # "Quantity Act or Forecast /SKU","Quantity PPY","Quantity Act/Forecast vs PPY%"
                    ]
    # From Quantity tabs --> "Budget QTY%", 

    cost_cols = ["Budget Cost%","Budget vs Act/Forecast Cost%","Budget vs PY Cost%","Budget vs PPY Cost%",
                    "Cost/SKU","Cost Mix%",
                    "Cost Act/Forecast","Cost Act/Forecast vs Budget%","Cost Act/Forecast vs LY%",
                    "Cost Act or Forecast /SKU","Cost Act/Forecast vs PPY%","Cost Act/Forecast vs PPY %",
                    "Cost Budget/SKU"]

                  

# Needed to add net sold cost Mix % PPY
    ra_cost = ["Channel",'country',"Region",'area','city',"StoreId",
                "Store",'season',"Department","Family","SubFamily","Category","DOM COMM","Sub Category",
                "Extended SubCategory","Supplier","SubCategory Supplier","Assembly Code-NickName","Status","END OF Life",
                "Description","ItemLookupCode",
                # for bdget terms
                "Budget Cost", "Budget Cost Mix %", "Budget Cost vs Act/Fct Cost %", "Budget Cost vs Net Sold Cost PY %",
                #for revised budget terms
                "Revised Budget Cost %", "Revised Budget Cost vs Act/Fct Cost %",
                #Cost terms
                "Net Sold Cost Mix % TY", "Net Sold Cost Act/Fct TY", "Net Sold Cost Act/Fct Mix % TY", 
                "Net Sold Cost Act/Fct vs Budget % TY", "Net Sold Cost Mix % LY", "Net Sold Cost Act/Fct vs LY %",
                "Cost Of Goods LLY", "Net Sold Cost Act/Fct vs PPY %"
                # Prev Cols
                # "Budget Cost%","Budget vs Act/Forecast Cost%","Budget vs PY Cost%","Budget vs PPY Cost%",
                # "Retail_Price","Total SKU Count","Cost/SKU","Cost Budget/SKU","Cost Mix%","Cost Actuals",
                # "Cost Act/Forecast","Budget Amount","Cost Act/Forecast vs Budget%","Cost Act/Forecast vs LY%",
                # "Cost Act or Forecast /SKU","Cost Of Goods LLY","Cost Act/Forecast vs PPY%"
                ]
                
# Buy Cost By SKU , Cost/SKU,Cost Budget/SKU,Cost Mix%,Cost Actuals ,CostLY

    margin_cols = ["Budget Gross Margin %","Margin Actuals","Budget Margin%","Budget Margin Mix%",
                    "Budget vs Act/Forecast Margin%","Budget vs PY Margin%","Budget vs PPY Margin%",
                    "Buy Margin/SKU","Margin Budget/SKU","Margin Mix%",
                    "Margin Act/Forecast","Margin Act/Forecast vs LY%",
                    "Margin Act or Forecast /SKU","Margin Act/Forecast vs PPY%"]

                
                

    ra_margin = ["Channel",'country',"Region",'area','city',"StoreId",
                "Store",'season',"Department","Family","SubFamily","Category","DOM COMM","Sub Category",
                "Extended SubCategory","Supplier","SubCategory Supplier","Assembly Code-NickName","Status","END OF Life",
                "Description","ItemLookupCode",
                # Bd tabs
                "Budget Margin", "Buddget Margin Mix %", "Budget Margin vs Act/Fct Margin %",
                "Budget Margin vs Net Sold Margin PY%", "Budget Margin vs Net Sold Margin PPY %"

                # Revised Tab
                "Revised Budget Margin", "Revised Budget Margin %", 'Revised Budget Margin vs Act/Fct Margin %',
                # Nt Sold tab
                "Net Sold Margin Actuals TY", "Net Sold Margin Mix % TY", "Net Sold Margin Act/Fct TY",
                "Net Sold Margin Act/Fct Mix% TY", "Net Sold Margin Act/Fct vs Budget % TY",
                "Net Sold Margin Sales Deficit", 
                "Net Sold Margin LY", "Net Sold Margin Mix % LY", "Net Act/Fct Margin vs LY %", 
                "Net Sold Margin PPY", "Net Sold Margin Mix% PPY", "Act/Fct vs PPY %" 
                #Previous tabs
                # "Budget Gross Margin","Budget Margin%","Budget Margin Mix%","Budget vs Act/Forecast Margin%","Budget vs PY Margin%","Budget vs PPY Margin%",
                # "Retail_Price","Buy Margin/SKU","Total SKU Count","Margin Budget/SKU","Margin Mix%","Margin Actuals","LY Margin%","LLY Margin%",
                # "Margin Act/Forecast","Budget Amount","Margin Act/Forecast vs LY%",
                # "Margin Act or Forecast /SKU","Margin Act/Forecast vs PPY%"
                ]

    

    tabs = {
            "BudgetValue":columns,
            "BudgetCost":ra_cost,
            "BudgetQuantity":ra_quantity,
            "BudgetMargin":ra_margin 
            }

    SUB_FILTER = {'store':[],'region':[],'season':[],"area":[],'Channel':[],'budget_year':[],'Quarter':[],'month':[],'week':[],'date':[],'Day':[],
                    'historical_year':[],'history_Quarter':[],'history_month':[],'history_week':[],'history_Day':[],'history_dates':[]}
    filter_store = dict(zip(['Store','Region','Channel','Budget Year','Budget quarter','Budget month','Budget week','Budget day','Budget date','Historical Year','History quarter'
            ,'History month','History week','History day',"History date"],[[]]*15))
    





gloabal_vars = Parameters()




class Budget():
    columns = gloabal_vars.columns
    max_col = gloabal_vars.max_col
    sum_col = gloabal_vars.sum_col 
    avg_col = gloabal_vars.avg_col
    int_cols = gloabal_vars.int_cols
    float_cols = gloabal_vars.float_cols
    heirarchy = gloabal_vars.heirarchy
    tabs = gloabal_vars.tabs
    SUB_FILTER = gloabal_vars.SUB_FILTER
    filter_store = gloabal_vars.filter_store
    db_table_keys = gloabal_vars.db_table_keys
    save_table_keys = gloabal_vars.save_table_keys

    DATA              = pd.DataFrame()
    def get_from_db(self, filters: Filters, db: Session,email_address =None, session_id = 0):
        '''
        Get data from DB to dataframe and preprocessing dataframe
            Args:
                filter : filters to be applied on query
                db     : connection to the DB
            Returns:
                df     : data frame containing data
        '''

       
        all = f"""  SELECT * FROM users_tables """
        result = db.execute(text(all))
        rows = [list(row) for row in result]
        df = pd.DataFrame(columns=["index","email_address","module_id","session_id","table_name"], data=rows)


        # module_id = 'budget'
        # # email_address = 'Akshay@rebosolution.com'
        # all = f"""  DELETE FROM users_tables WHERE "email_address" = 'Akshay@rebosolution.com' AND "module_id" = 'budget'; """
        # delete_statement = f"DELETE FROM users_tables WHERE email_address = '{email_address}' AND module_id = '{module_id}'"

        # db.execute(text(delete_statement))
        # rows = [list(row) for row in result]
        
        
        module_id = 'budget'
        if session_id !=0:
            table_name,message = self.get_selected_table(db,email_address,module_id,session_id)
            print(table_name,message)
        else:
            table_name = "budget_items"
            message = "Loading default version..."

        

        result = db.execute(text(budget_query(filters,table_name)))
        rows = [list(row) for row in result]
        df = pd.DataFrame(columns=gloabal_vars.db_columns, data=rows)
#****************************************************************************************************** 
        # print(df, "Full database")
        # print(df.columns)
        print(df.isnull().sum())
#******************************************************************************************************
        for col in gloabal_vars.other_cols:
            df[col] = 0
        for col in gloabal_vars.quantity_cols:
            df[col] = 0
        for col in gloabal_vars.margin_cols:
            df[col] = 0
        for col in gloabal_vars.cost_cols:
            df[col] = 0
        # print(df.columns,"col b e")
        # for col in gloabal_vars.avg_col:
        #     df[col] = df[col].fillna(0).replace([np.inf, -np.inf],0)
        # for col in gloabal_vars.sum_col:
        #     df[col] = df[col].fillna(0).replace([np.inf, -np.inf],0)

        # df["Average KPI"] = 0
        # print(df['BDate'])
        df["Budget Year"] = df["BudgetYear"].fillna(0).astype(int)
        
        df["Budget date"] = df["BDate"].fillna("1900-01-01")
        df["dates"] = pd.to_datetime(df["BDate"],errors="coerce") 
        df['Budget month'] = df["dates"].dt.strftime('%B')
        df['Budget week'] = df["dates"].dt.strftime('%U')
        df['Budget quarter'] = df['dates'].dt.quarter.astype(str)
        df["Budget day"] = df["dates"] = df["dates"].dt.day_name().astype(str)

        

        df["History_date"] = df["HistoricalYear"].fillna("1900-01-01")
        # print(df["History_date"], "History week")
        # print(df.History_date.dtype)

        df["Historical Year"] = df["HistoricalYear"].fillna(0).astype(int)
        df["dates"] = pd.to_datetime(df["History_date"], errors = 'coerce') 
        df["History month"] = df["dates"].dt.strftime("%B")
        df["History week"] = df["dates"].dt.strftime("%U")
        df["History quarter"] = df["dates"].dt.quarter.astype(str)
        df["History day"] = df["dates"].dt.day_name().astype(str)


        # df[max_col] = df[max_col].astype(str)

        df = self.calculate_initial_frame(df,["ItemLookupCode","Channel","StoreId","Budget date","History_date"])
        group = []
        # print("Error happened after printing this line")
        #Change the SUB_FILTER List
        pd.set_option("display.Max_columns", None)
        # pd.set_option("display.Max_rows", None)
        self.SUB_FILTER = self.call_filter(df,self.SUB_FILTER,group,df,self.filter_store)
        # print("****************************************************************************************")
        # print(df.columns.to_list(), "for Sales Actual/Forecast TY 0")
        # print("****************************************************************************************")
        self.DATA,ds,channel_flag  = self.calculate_df(df,df,initial_call=True)
        # To view column "Sales Actual/Forecast TY"
        # print(self.DATA, "df DATA 1")
                                                    # pd.set_option("display.max_rows", None)
        DATA_COLS = self.DATA.columns.to_list()
        # print(DATA_COLS)
        
        self.DATA[gloabal_vars.max_col] = self.DATA[gloabal_vars.max_col].astype(str)
        return self.DATA,self.DATA,message

    def calculate_initial_frame(self,df,subset):
        ''' !!The operations are performed here before grouping!!'''


        df["Total SKU Count"] = (~df.duplicated(subset=subset,keep='first')).astype(int)
        df["Total SKU Count LY"] = df.groupby(subset)["Units Buy By SKU LY"].transform('sum')
        df["Total SKU Count LY"] = df["Total SKU Count LY"]*df["Total SKU Count"]
        df["Total SKU Count LY"] = (df["Total SKU Count LY"]>0).astype(int)

        # df["Total SKU Count"] = (~df.duplicated(subset=subset,keep='first')).astype(int)
        
        df["Stock OnHand Qty SKU Count"] = df.groupby(subset)["Stock OnHand Qty"].transform('sum')
        df["Stock OnHand Qty SKU Count"] = df["Stock OnHand Qty SKU Count"]*df["Total SKU Count"]
        df["Stock OnHand Qty SKU Count"] = (df["Stock OnHand Qty SKU Count"]>0).astype(int)

        df["Total SKU Count LY"] = df["Total SKU Count LY"] | df["Stock OnHand Qty SKU Count"]
       

        return df
#default when group [] display all
# on selection for first filter
    #show all options on it's level
        #on selection
            #show only the selected option for selcted level
            # use data for selected option
            #you can hide or unhide main seleciton option
                #on de selection
                #use data to group
    #show all options of it's level
                    


    def call_filter(self,data:pd.DataFrame,SUB_FILTER:dict,group:list,DATA:pd.DataFrame,filter_store):

        ''' We only append to filters keys are keys to flter'''


        try:
            keys = ['store','region','Channel','budget_year','Quarter','month','week','Day','date','history_year','history_Quarter',
            'history_month','history_week','history_Day','history_dates']
            values = ['Store','Region','Channel','Budget Year','Budget quarter','Budget month','Budget week','Budget day','Budget date','Historical Year','History quarter'
            ,'History month','History week','History day',"History_date"]



            last_slection = ''
            if group !=[]:
                last_slection = group[-1]

            for i,v in zip(keys,values):

                if v not in group:
                    
                    if v in ['Budget Year','Historical Year']:
                        SUB_FILTER[i]   = sorted(list(data[v].astype(float).unique()),key=lambda x:(x is None,x))
                    elif v in ['Budget date',"History_date"]:
                        SUB_FILTER[i]   = sorted(list(data[v].astype(str).unique()),key=lambda x:(x is None,x))
#----------------------> Error faced value is not in the list for history year 2021 -2023
                    elif v in ['Budget month','History month']:
                        SUB_FILTER[i]   = sorted(list(data[v].astype(str).unique()),key=lambda x:(x is None,list(calendar.month_name).index(x) if x !=np.nan else None))
                    else:
                        SUB_FILTER[i]   = sorted(list(data[v].unique()),key=lambda x:(x is None,x))
                    filter_store[v] = SUB_FILTER[i]
                else:
                    if v in ['Budget Year','Historical Year'] and v!= last_slection:
                        if group == []:
                            SUB_FILTER[i]        = sorted(list(DATA[v].astype(float).unique()),key=lambda x:(x is None,x))
                        else:
                            SUB_FILTER[i] = filter_store[v]

                    elif v in ['Budget date',"History_date"] and v != last_slection:
                        if group == []:
                            SUB_FILTER[i]   = sorted(list(DATA[v].astype(str).unique()),key=lambda x:(x is None,x))
                        else:
                            SUB_FILTER[i] = filter_store[v]

                    elif v in ['Budget month','History month'] and v != last_slection:
                        if group == []:
                            SUB_FILTER[i]   = sorted(list(DATA[v].astype(str).unique()),key=lambda x:(x is None,list(calendar.month_name).index(x) if x !=np.nan else None))
                        else:
                            SUB_FILTER[i] = filter_store[v]

                    else:
                        if v!=last_slection:
                            if group == []:
                                SUB_FILTER[i]        = sorted(list(DATA[v].unique()),key=lambda x:(x is None,x))
                            else:
                                SUB_FILTER[i] = filter_store[v]

        except Exception as e:
            print(f"error is {e}")
            print(traceback.format_exc())
            pass
        # print(SUB_FILTER['history_year'],"history yeaers")
        return SUB_FILTER

        
    # def call_filter(self,data:pd.DataFrame,SUB_FILTER:dict,group:list,DATA:pd.DataFrame,filter_store):

    #     ''' We only append to filters keys are keys to flter'''


    #     try:
    #         keys = ['store','region','Channel','budget_year','Quarter','month','week','Day','date','history_year','history_Quarter',
    #         'history_month','history_week','history_Day','history_dates']
    #         values = ['Store','Region','Channel','Budget Year','Budget quarter','Budget month','Budget week','Budget day','Budget date','Historical Year','History quarter'
    #         ,'History month','History week','History day',"History date"]


            
    #         last_slection = ''
    #         if group !=[]:
    #             last_slection = group[-1]
    #         for i,v in zip(keys,values):

    #             if v not in group:
    #                 if v in ['Budget Year','Historical Year']:
    #                     SUB_FILTER[i]        = sorted(list(data[v].astype(float).unique()),key=lambda x:(x is None,x))
    #                 elif v in ['Budget date',"History date"]:
    #                     SUB_FILTER[i]   = sorted(list(data[v].astype(str).unique()),key=lambda x:(x is None,x))
    #                 elif v in ['Budget month','History month']:
    #                     SUB_FILTER[i]   = sorted(list(data[v].astype(str).unique()),key=lambda x:(x is None,list(calendar.month_name).index(x) if x !=np.nan else None))
    #                 else:
    #                     SUB_FILTER[i]        = sorted(list(data[v].unique()),key=lambda x:(x is None,x))
    #                 filter_store[v] = SUB_FILTER[i]
    #             else:
    #                 if v in ['Budget Year','Historical Year'] and v!= last_slection:
    #                     if group == []:
    #                         SUB_FILTER[i]        = sorted(list(DATA[v].astype(float).unique()),key=lambda x:(x is None,x))
    #                     else:
    #                         SUB_FILTER[i] = filter_store[v]

    #                 elif v in ['Budget date',"History date"] and v != last_slection:
    #                     if group == []:
    #                         SUB_FILTER[i]   = sorted(list(DATA[v].astype(str).unique()),key=lambda x:(x is None,x))

    #                     else:
    #                         SUB_FILTER[i] = filter_store[v]

    #                 elif v in ['Budget month','History month'] and v != last_slection:
    #                     if group == []:
    #                         SUB_FILTER[i]   = sorted(list(DATA[v].astype(str).unique()),key=lambda x:(x is None,list(calendar.month_name).index(x) if x !=np.nan else None))
    #                     else:
    #                         SUB_FILTER[i] = filter_store[v]

    #                 else:
    #                     if v!=last_slection:
    #                         if group == []:
    #                             SUB_FILTER[i]        = sorted(list(DATA[v].unique()),key=lambda x:(x is None,x))
    #                         else:
    #                             SUB_FILTER[i] = filter_store[v]

    #     except Exception as e:
    #         print(f"error is {e}")
    #         print(traceback.format_exc())
    #         pass
    #     # print(SUB_FILTER['history_year'],"history yeaers")
    #     return SUB_FILTER



    
    def calculate_df(self,data: pd.DataFrame,ds:pd.DataFrame,initial_call=False):
        '''
        Calculation on the Dataframe
            Args:
                df : DataFrame to be calculated
            Returns:
                data: Calculated data frame

        '''
        channel_flag = False
        # print(self.second_grouping(data,ds),"this one")
        # print("****************************************************************************************")
        # print(data.columns.to_list(), "for Sales Actual/Forecast TY 1")
        # print("****************************************************************************************")

        ds,channel_flag = self.second_grouping(data,ds)
        # print("****************************************************************************************")
        # print(data.columns.to_list(), "for Sales Actual/Forecast TY 2")
        # print("****************************************************************************************")        # print(data['Budget%'],"percentages")
        # data['Budget%'] =(data['Budget%']*100)/(data['Budget%'].sum())
        total_budget = data['Budget Amount'].sum()
        data['Budget%'] =(data['Budget Amount']*100)/(total_budget)
        # print(data['Budget%'].sum(),"sumbufgg")
        #if data['Budget%']>=100,
            # then chnage that row value as data["Budget Amount"]= data["Budget Amount"].sum()
            # if data["Budget Amount"]>=data["Budget Amount"].sum(),
                #keep that row value as it is, data["Budget Amount"]=data["Budget Amount"].sum()
                # else
                # change oter row values as data["Budget Amount"] = 0

        #if df[budget_amoount] >df[budget_amoount].sum()
        pd.set_option("display.Max_rows", None)
        # print(ds["Sales Actual/Forecast TY"])
        print(ds["Sales Actuals"].sum(),"Sales Actuals")
        # print(ds["Budget Amount"])

        if not initial_call:
            data['Budget Amount'] = np.where(data['Budget%'] >= 100, total_budget,data['Budget Amount'])
            if total_budget in data['Budget Amount'].to_list():
                data['Budget Amount'] = np.where(data['Budget%'] >= 100, total_budget, np.where(data['Budget Amount'] >= total_budget, data['Budget Amount'], 0))


        # data["Budget Amount"] = np.where(data['Budget%']<=0,0,data["Budget Amount"])
        # print(data['Budget%'],"percentage")
        # data['Budget Amount']
        # data["Net Sales Mix % TY"] = data["Sales Actuals"].apply(lambda x: x / data["Sales Actuals"].sum() if x != 0 else 0)
        data["Net Sales Mix % TY"] = (data["Sales Actuals"])/data["Sales Actuals"].sum() #if data["Sales Actuals"] !=0 else 0
        data["Net Sales Act/Forecast Mix % TY"] = (data["Sales Actual/Forecast TY"]/data["Sales Actual/Forecast TY"].sum())*100
        data["Net Sales Act/Forecast vs Budget % TY"] = (data["Sales Actual/Forecast TY"]/data["Budget Amount"].sum())*100 # Budget Amount for Budget Amount TY
        data["Net Sales Mix % LY"]=(data["Sales Actual/Forecast LY"]/data["Sales Actual/Forecast LY"].sum()) * 100
        # Net Act/Forecast vs LY % = (Act or Forecast  LY / Total Sales Last Year) * 100 
        data["Net Act/Forecast vs LY %"] = (data["Sales Actual/Forecast LY"]/data["Net Sales LY"].sum()) * 100
        data["Act/Forecast vs PPY %"] = (data["Sales Actual/Forecast LLY"]/ data["PPrev netsale amount"].sum()) * 100
        """data["Exchange Rate"] = data["Preffered Currency"]* data["Exchange Rate"]"""
        # print(data["Sales Actual/Forecast LY"], data["Sales Actual/Forecast LY"].sum(), "LLLLLLLLLLLLLLLLYYYYYYYYYy")
        
        #************8************************************************************************************8
        # data["Budget vs Act/Forecast %"] = ((data["Budget Amount"]/data["Sales Act/Forecast"].replace(0,np.nan))*100)
        # data["Budget vs PY%"] = ((data["Budget Amount"]/data["Sales LY"].replace(0,np.nan))).fillna(0)*100
        # data["Budget vs PPY%"] = ((data["Budget Amount"]/data["PPrev netsale amount"].replace(0,np.nan))).fillna(0)*100
        #*********************************************************************************8*******************


        # data["Budget/SKU"] = data["Budget Amount"]/data["Total SKU Count"].replace(0,np.nan)
        # data['Units Buy By SKU'] = (data["Budget Amount"]/data['Retail_Price'].replace(0,np.nan)).replace([np.inf,-np.inf],np.nan).apply(lambda x:round(x) if not pd.isna(x) else 0)
        # data['Deficit'] =  data["Budget Amount"]-(data["Units Buy By SKU"]*data['Retail_Price']) 
        # data["Units/SKU"] = (data["Units Buy By SKU"]/data["Total SKU Count"].replace(0,np.nan)).replace([np.inf,-np.inf],np.nan).fillna(0)
        # data["Budget/SKU LY"] = data["Budget Amount LY"]/data["Total SKU Count LY"].replace(0,np.nan)
        # data["Units Buy By SKU LY"] = data["Units Buy By SKU LY"].astype(float)
        # data["Units/SKU LY"] = (data["Units Buy By SKU LY"]/data["Total SKU Count LY"].replace(0,np.nan)).replace([np.inf,-np.inf],np.nan).fillna(0)
        # data["Budget Vs Act/Forecast % LY"] = ( data["Budget Amount LY"]/data["Sales LY"].replace(0,np.nan))*100
        # data["Total SKU Count"] = 



        return data,ds,channel_flag 

    def aggregation(self):
        agg_dict = ({col : 'mean' for col in self.AVG_COLS})
        agg_dict.update({col: 'sum' for col in self.SUM_COLS})
        return agg_dict



    def change_percent(self,grouped_df,other_grouped_df,increase,colID):
        grouped_df[colID] = grouped_df[colID].fillna(0)
        # print(grouped_df[colID].sum())
        # summation = np.round(grouped_df[colID].sum(),2)
        summation = grouped_df[colID].sum()
        # print(summation)
    #   grouped_df[colID] = grouped_df[colID] + (grouped_df[colID]*increase)/summation
        
        grouped_df.loc[:, colID] = grouped_df[colID] + (grouped_df[colID] * increase) / summation
        # print(other_grouped_df[colID].sum())
        
        other_grouped_df[colID] = other_grouped_df[colID].fillna(0)
        # unsummation = np.round(other_grouped_df[colID].sum(),2)
        unsummation = other_grouped_df[colID].sum()
        # print(unsummation)

    
        # print(grouped_df[colID].sum())
        other_grouped_df.loc[:, colID] = other_grouped_df[colID] - (other_grouped_df[colID] * increase) / unsummation

        
        # print(other_grouped_df[colID].sum(),"sums 2")
        frames =  [grouped_df,other_grouped_df]
        df = pd.concat(frames)
        return df

    def change_value(self,grouped_df,other_grouped_df,  increase,colID):
        summation = grouped_df[colID].sum()
        summation  = np.where(summation == 0,np.nan,summation)
        grouped_df.loc[:, colID] = grouped_df[colID] + (grouped_df[colID] * increase) / summation
        return pd.concat([grouped_df,other_grouped_df])
    
    # Secondary filters inside Filters 
    # changed History_date
    def apply_secondary_filters(self,data: pd.DataFrame,filter_data: Filters,filter_conditions,sub_filter_states,groups):




        ''' We only get filters details from json response '''
        filter_condition = filter_conditions
        sub_filter_state = sub_filter_states
        key_map = {'Store_Name':'Store','region':'Region','Channel':'Channel','BudgetYear':'Budget Year','Quarter':'Budget quarter',
        'month':'Budget month','week':'Budget week','Day':'Budget day','BudgetDate':'Budget date','HistoricalYear':'Historical Year','history_Quarter':'History quarter',
        'history_month':'History month','history_week':'History week','history_Day':'History day','history_dates':"History_date",
        }
        group = groups

    
        try:
            for key,values in filter_data.secondary_filter.dict().items():
            
                if not values:
                    continue
                
                sub_filter_state = True
                if key_map[key] in ['Historical Year','Budget Year']:
                    values = [int(i) for i in values]
                    new_condition = data[key_map[key]].isin(values)
                else:
        
                    new_condition = data[key_map[key]].isin(values)
                group.append(key_map[key])

                if filter_condition  is None:
                    filter_condition = new_condition
                else:
                    filter_condition = filter_condition & new_condition
        except Exception as e:
            print(traceback.format_exc())
            print(f"Error applyting filters:{e}")
        
        if not sub_filter_state == True:
            group = []
            filter_condition = None
            sub_filter_state = False


        return filter_condition,sub_filter_state,group


    # def update_users_tables(self,db:Session,login_data):
    #     ''' It will update useras tables data with new record if the users record not found in db'''

    #     email_address = login_data['mail']
    #     module_id = 'budget'
    #     table_name = module_id + '_' + email_address.split('@')[0].lower()
    #     session_id = 0

    #     existing_record = db.query(Users_tables).filter(
    #         Users_tables.module_id == module_id,
    #         Users_tables.email_address == email_address,
    #         Users_tables.session_id == session_id,
    #         Users_tables.table_name == table_name
    #     ).first()

        # if not existing_record:
        #     new_record = Users_tables(email_address=email_address,module_id=module_id,session_id=session_id,table_name=table_name)
        #     db.add(new_record)
        #     db.commit()
        #     db.refresh(new_record)



    def write_to_db(self,db:Session,df:pd.DataFrame,budget_cols:dict):

        ''' This code will fetch the db table, then it will inner merge user data with db data.
        After it will create two seperate df with matching keys of both merged df's.Then these
        two df's will left merged and using left only label duplicates will be removed.Finally for the unique
        keys changes, non duplicate record will be updated in db '''

        keys = ['Channel','StoreId','ItemLookupCode','BDate']
        updated_rows_count = 0

        df = self.clean_keys(df,keys)
        db_df = pd.read_sql_query(db.query(BudgetItemtest).statement,con=db.bind)
        db_df = db_df.drop(['index'],axis=1)
        db_df,df = self.correct_size_type(budget_cols,db_df,df)
        result_df = df.merge(db_df,on = keys ,how = 'inner', suffixes=('_x', '_y'))

        columns = list(result_df.columns)
        first_frame = result_df[keys]
        second_frame = result_df[keys]
        for column in columns:
            if column.endswith("_x"):
                first_frame.loc[:,column[:-2]] = result_df[column]
            elif column.endswith("_y"):
                second_frame.loc[:,column[:-2]] = result_df[column]

        keys = ['Channel','StoreId','ItemLookupCode','BDate','BudgetValue','BudgetCost','BudgetQTY']

        ''' we are taking only these keys to filter duplicates, if you want to consider others as well ,
            you can go ahead add include them ,but you might need to handle the null cases according
             to datatypes'''

        result_df = first_frame.merge(second_frame,how ='left',on = keys ,indicator=True)
        result_df = result_df[result_df['_merge'] == 'left_only'].drop('_merge', axis=1)
        updated_rows_count = 0
        for _,row in result_df.iterrows():
            result = db.query(BudgetItemtest).filter(
                BudgetItemtest.Channel == row['Channel'],
                BudgetItemtest.StoreId == row['StoreId'],
                BudgetItemtest.ItemLookupCode == row['ItemLookupCode'],
                BudgetItemtest.BDate == row['BDate'],
                channel_flag
            ).update({

                 'BudgetValue':row['BudgetValue'],
                 'BudgetCost':row['BudgetCost'], 
                 'BudgetQTY':row['BudgetQTY']

            },synchronize_session=False)
            updated_rows_count += result
        db.commit()
        return updated_rows_count

    def correct_size_type(self,budget_cols,db_df,df):

        ''' This function used for data cleaning while doing save operation'''
         #string = 0, bool = 1, float=2, int = 3 , date = 4
        
        for i,v in budget_cols.items():
            if v == 0:
                db_df[i] = db_df[i].astype(str)
                db_df[i] = db_df[i].str.strip()
                df[i] = df[i].astype(str)
                df[i] = df[i].str.strip()
                
            elif v == 1:
                df[i] = df[i].bool
                db_df[i] = db_df[i].bool
            elif v == 2:
                df[i] = df[i].astype(float).fillna(0)
                db_df[i] = db_df[i].astype(float).fillna(0)
            elif v == 3:
                df[i] = df[i].astype(int).fillna(0)
                db_df[i] = db_df[i].astype(int).fillna(0)
            elif v == 4:
                df[i] = pd.to_datetime(df[i])
                db_df[i] = pd.to_datetime(db_df[i])
        return db_df,df


    def clean_keys(self,df,columns):
        ''' drop null rows of main keys'''
        for column in columns:
            df = df.dropna(subset=[column])
        return df

    def read_file(self,file,budget_cols):

        ''' This function read input file and convert it into desired form of data frame
            and then returns df along with status code and upload status'''

        budget_frame = pd.DataFrame()
        upload_status = None
        status_code = 200
        os.makedirs("uploads",exist_ok=True)
        file_name = os.path.join("uploads",file.filename)

        with open(file_name,"wb") as f:
            f.write(file.file.read())
        

        file_extension = os.path.splitext(file.filename)[1].lower()
        if file_extension in ('.xls','.xlsx'):
            try:
                upload_df = pd.read_excel(file_name)
            except:
                print(traceback.format_exc())
                upload_status = "Error in parsing Excel file"
                status_code = 422
        elif file_extension == '.csv':
            try:
                upload_df = pd.read_csv(file_name)
            except:
                upload_status = "Error in parsing csv file"
                status_code = 422
        else:
            upload_status = "Invalid file,Please upload .xls / .xlsx / .csv  file"
            status_code = 415


        if upload_status == None:
            for col in list(budget_cols.keys()):
                try:

                    budget_frame[col] = upload_df[col]
                except:
                    print(traceback.format_exc())
                    upload_status = f"missing column {col}"
                    status_code = 422
        return budget_frame,upload_status,status_code  

    def get_selected_table(self,db: Session,email_address:str,module_id:str,session_id:int):
        
        result = db.execute(text(get_table_name(email_address,module_id)))
        rows = [list(row) for row in result]
        df = pd.DataFrame(columns=['table_name','session_id'], data=rows)
        values = df['table_name'].values
        # print(df)
    
        message = ""
        if values.size == 0:
            table_name = "budget_items"
            message = "No saved version available,loading default version..."
        else:
            
            if session_id in list(df['session_id'].values):
                print("here1")
                
                table_name = df[df['session_id']==session_id]['table_name'].values[0]
                message = f"Loadin version {session_id}..."
            else:
                print("here2")
                table_name = df[df['session_id']==df['session_id'].max()]['table_name'].values[0]
                message = f"Selected version is not available, loading the latest version ..."

        return table_name,message

    #using module_id, mail_id select table names and session id
    #if none ,message only default version is available
    #if selected version/session_id is,message available version is selected
    #else only version session_id [is/are if len()=1 or 2] is available

    def get_session_id(self,db: Session,email_address:str,module_id:str):

        result = db.execute(text(get_max_session_id(email_address,module_id)))
        rows = [list(row) for row in result]
        df = pd.DataFrame(columns=["session_id",'table_name'], data=rows)
        values = df["session_id"].values
        table_maps = {'one':'two','two':'three','three':'one','zero':'one'}

        if values.size == 0:
            session_id = 1
            table_last_label = 'zero'
        else:
            table_name = df['table_name'].values[0]
            session_id = int(values[0]) + 1
            table_last_label = table_name.split('_')[-1]
        
        table_label = table_maps[table_last_label]
        return session_id,table_label
    
    #update users tables with new record
    def update_users_tables(self,db:Session,email_address,module_id,session_id,table_name):
        new_record = Users_tables(email_address=email_address,module_id=module_id,session_id=session_id,table_name=table_name)
        db.add(new_record)
        db.commit()
        db.refresh(new_record)

    #save the df to db as table
    def save_table(self,db:Session,df:pd.DataFrame,table_name:str):
        df = df[self.save_table_keys]
        rename_dict = dict(zip(self.save_table_keys,self.db_table_keys))
        df.rename(columns=rename_dict, inplace=True)
        df['BDate']  = pd.to_datetime(df['BDate'])

        memory_usage = df.memory_usage(deep=True)
        total_memory_usage = memory_usage.sum()
        total_memory_usage_mb = total_memory_usage / (1024 ** 2)

        # Display the results
        # print(f"Memory usage per column:\n{memory_usage}\n")
        print(f"Total memory usage of the DataFrame: {total_memory_usage} bytes")
        print(f"Total memory usage in megabytes: {total_memory_usage_mb} MB")

        df.to_sql(table_name,con=db.bind,if_exists='replace',index=False,method='multi', chunksize=1000)
        db.commit()
    """Quantity tab"""

    def calculate_quantity(self, data: pd.DataFrame):
        '''
        !!The operations are performed here after grouping!!
        Calculation on the Dataframe
            Args:
                df : DataFrame to be calculated
            Returns:
                data: Calculated data frame

        '''

# Budget vs Act/Forecast QTY%,Quantity LY, Quanity Buy By SKU ly
        # Adeded Fillna(0)
        # data["Units Buy By SKU LY"] = data["Units Buy By SKU LY"].fillna(0).astype(float).astype(int)
        # data["Budget QTY%"] =(data["Budget QTY%"]*100)/(data["Budget QTY%"].sum())
        # data["Units Buy By SKU"] = (data["Budget Amount"]/data["Retail_Price"].replace(0,np.nan)).replace([np.inf,-np.inf],np.nan).apply(lambda x:round(x) if not pd.isna(x) else 0)
        data["Budget Quantity Mix %"] = (data["Units Buy By SKU"]/data["Units Buy By SKU"].sum()) * 100
        # print("************************")
        # print(data["Units Buy By SKU"])
        # print(data["Budget Quantity Mix %"])
        # print("************************")
        print(data.columns.to_list(), "quant data list to check NtSoldQty-LY")
        print("Type of the unitsbysku", data["Units Buy By SKU"].apply(type).unique())
        print("Type of the Quantity act", data["Quantity Actuals"].apply(type).unique())

        print("Type of the NtSoldQty-LY", data["NtSoldQty-LY"].apply(type).unique())
        print("Type of the Quantity PPY", data["Quantity PPY"].apply(type).unique())
        # print("Revised Budget Quantity", data["Revised Budget Quantity"].apply(type).unique())
        print("Type of the Budget Amount", data["Budget Amount"].apply(type).unique())

        
        data["Budget QTY vs Act/Forecast QTY%"] = (data["Units Buy By SKU"]/data["Quantity Actuals"].astype(float)) * 100
        data["Budget QTY vs Net sld QTY PY%"] = data["NtSoldQty-LY"].astype(float)/data["Units Buy By SKU"]
        data["Budget QTY vs Net sld QTY PPY%"] = data["Quantity PPY"].astype(float)/data["Units Buy By SKU"]
        """Quantity Actuals from db"""
        #****************************************************************************************
        # thinhhk shd need Revised Budget Quantity in db_cols
        # data["Resvised Budget Quantity %"] = (data["Revised Budget Quantity"]/data["Revised Budget Quantity"].sum()) * 100

        # data["Resvised Budget Quantity vs Act/Frcst Qty %"] = (data["Revised Budget Quantity"]/data["Revised Budget Quantity"].sum()) * 100

        # calculation term Should added in Ra_quantity, sum/av/max col, columns list(data base)
        #****************************************************************************************
        data["Net Sold Quantity Mix % TY"] = (data["Quantity Actuals"]/data["Quantity Actuals"].sum()) * 100
        data["Net Sold Qty Act/Fct TY"] = data["Budget Amount"].fillna(0).astype(float) + data["Quantity Actuals"].fillna(0).astype(float)
        data["Net Sold Qty Act/Fcst Mix % TY"] = (data["Net Sold Qty Act/Fct TY"]/data["Net Sold Qty Act/Fct TY"].sum()) *100
        data["Net Sold Qty Act/Fcst vs Budget % TY"] = (data["Net Sold Qty Act/Fct TY"]/data["Units Buy By SKU"].sum()) *100
        data["Net Sold Quantity Sales Deficit"] = 0
        # data["Net Sold Quantity LY"] FROM DB
        #-------------------------------------------------
        #data["Net Sold Qty Mix % LY"] = data[""]        | FORMULA ERROR
        #-------------------------------------------------
        #-->Fmla of Net Sold Act/Fct Qty LY for nxt calc<--
        #-->  --> Nt Sold Act/Fct Qty LY = Budget Amount + Net Sold Qty LY 
        data["Nt Sold Act/Fct Qty LY"] = data["Budget Amount"].fillna(0).astype(float) + data["NtSoldQty-LY"].fillna(0).astype(float)
        #-->******************<--
        print("Type of the Nt Sold Act/Fct Qty LY", data["Nt Sold Act/Fct Qty LY"].apply(type).unique())
       
        # Fmla --> Net Act/Fct vs LY % = Act/Forecast Quantity LY / Total Sold Quantity LY
        data["Net Act/Fct Qty vs LY %"] = (data["Nt Sold Act/Fct Qty LY"] / (data["NtSoldQty-LY"].astype(float).sum())) * 100
         
        # data["Net Sold QTY PPY"]--> From Database
        # data["Net sold Qty Mix% ppy"] ---> Formula not given
        # --> Fmla of Net Sold Act/Fct Qty LLY for nxt calc
        # --> --> #-->  --> Nt Sold Act/Fct Qty LLY = Budget Amount + Net Sold Qty LLY 
        data["Net Sold Act/Fct Qty LLY"] =  data["Budget Amount"].fillna(0).astype(float) + data["Quantity PPY"].fillna(0).astype(float)
        
        data["Act/Forecast vs PPY %"] = (data["Net Sold Act/Fct Qty LLY"]/data["Quantity PPY"].astype(float).sum())* 100



        # data["Quantity Act/Forecast"] = data["Units Buy By SKU"]+data["Quantity Actuals"]
        # # Adeded Fillna(0)
        # data["Quantity Act/Forecast"] = data["Quantity Act/Forecast"].fillna(0).astype(float).astype(int)
        # data["Quantity Act/Forecast vs Budget%"] = ((data["Units Buy By SKU"]/data["Quantity Act/Forecast"].replace(0,np.nan)).replace([np.inf,-np.inf],np.nan).apply(lambda x:x if not pd.isna(x) else 0))*100
        # data["Quantity Act/Forecast vs LY%"] = ((data["Quantity Act/Forecast"] /data["Units Buy By SKU LY"].replace(0,np.nan)).replace([np.inf,-np.inf],np.nan).apply(lambda x:x if not pd.isna(x) else 0))*100
        # data["Quantity Act or Forecast /SKU"] = ((data["Quantity Act/Forecast"] /data["Total SKU Count"].replace(0,np.nan))*100)
        # data["Quantity Act/Forecast vs PPY%"] = ((data["Quantity Act/Forecast"] /data["Quantity PPY"].replace(0,np.nan)).replace([np.inf,-np.inf],np.nan).apply(lambda x:x if not pd.isna(x) else 0))*100
        # data["Budget/SKU QTY"] = data["Units Buy By SKU"]/data["Total SKU Count"].replace(0,np.nan)
        # data["Budget vs PY QTY%"] = ((data["Units Buy By SKU"]/data["Units Buy By SKU LY"].replace(0,np.nan))).fillna(0)*100
        # data["Budget vs PPY QTY%"] = ((data["Units Buy By SKU"]/data["Quantity PPY"].replace(0,np.nan))).fillna(0)*100
        # data["Quantity Mix%"] = (data["Units Buy By SKU LY"]*100)/(data["Units Buy By SKU LY"].sum())
        return data
        

        # ra_cost = [
        #            "Cost Act/Forecast vs Budget%",,
        #             ,"Cost Act/Forecast vs PPY%"]
    
    def calculate_cost(self, data: pd.DataFrame):
            '''
            !!The operations are performed here after grouping!!
            Calculation on the Dataframe
                Args:
                    df : DataFrame to be calculated
                Returns:
                    data: Calculated data frame

            '''

    # Budget vs Act/Forecast QTY%,Quantity LY, Quanity Buy By SKU ly
    # "Buy Cost By SKU","Cost/SKU","Cost Budget/SKU"

            data["Budget Cost"] = data["Budget Cost"].astype(float)
            data["Budget Cost Mix %"] = (data["Budget Cost"]/data["Budget Cost"].sum())*100  # Fmla --> Budget Cost/ Current Year Net Sold Cost
            data["Budget Cost vs Act/Fct Cost %"] = (data["Budget Cost"]/data["Cost Actuals"]) * 100 # Fmla --> (Budget Cost / Current yr nt Sold Cost)*100 for completed days
                                                                                                            # or (TY Budget cost / Budget Cost) * 100
            data["Budget Cost vs Net Sold Cost PY %"] = (data["Cost Of Goods LY"]/data["Budget Cost"]) * 100 # Fmla --> (PY Net Sold Cost/Budget Cost ) * 100 for completed days
                                                                                                            # or (Budget Cost LY/Budget Cost) * 100

            data["Budget Cost vs Net Sold Cost PPY %"] = (data["Cost Of Goods LY"]/data["Budget Cost"]) * 100 #Fmla --> (Sold Cost PPY/Budget Cost)*100 for completed days
                                                                                                                # or (Budget Cost PPY/Budget Cost) * 100
            # Need to calculated
            data["Revised Budget Cost %"] = 0 # Fmla --> (Revised Budget Cost/Total Revised Budget Cost) * 100

            data["Revised Budget Cost vs Act/Fct Cost %"] = 0 # Fmla --> (Revised Budget Cost/ TY Net Sold Cost) * 100 for completed days
                                                                # Fmla2 -->(TY Revised Budget Cost/Revised Budget Cost) * 100
            data["Net Sold Cost Mix % TY"] = (data["Cost Actuals"]/data["Cost Actuals"].sum()) * 100

            # Term for next calc
            data["Net Sold Cost Act/Fct TY"] = data["Budget Amount"].fillna(0).astype(float) + data["Cost Actuals"].fillna(0).astype(float) #--> Budget Amount + Sold Cost TY

            data["Net Sold Cost Act/Fct Mix % TY"] =  (data["Net Sold Cost Act/Fct TY"]/data["Net Sold Cost Act/Fct TY"].sum()) * 100 # --> Fmla (Sold Cost Act/Fct LY/ Total Sold Cost Act/Fct LY) * 100
            
            data["Net Sold Cost Act/Fct vs Budget % TY"] = (data["Net Sold Cost Act/Fct TY"]/data["Budget Cost"]) * 100
            
            # Term for Next Calc in LY Cost Act/Fcst
            data["Net Sold Cost Act/Fct LY"] = data["Budget Amount"].fillna(0).astype(float) + data["Cost Of Goods LY"].fillna(0).astype(float)
            data["Net Sold Cost Mix % LY"] =  (data["Net Sold Cost Act/Fct LY"]/data["Net Sold Cost Act/Fct LY"].sum()) * 100
            data["Net Sold Cost Act/Fct vs LY %"] = (data["Net Sold Cost Act/Fct LY"]/data["Cost Of Goods LY"].sum())*100
        
            # Term for Next Calc in LY Cost Act/Fcst
            data["Net Sold Cost Act/Fct LLY"] = data["Budget Amount"].fillna(0).astype(float) + data["Cost Of Goods LLY"].fillna(0).astype(float)
            data["Net Sold Cost Act/Fct vs PPY %"] =  (data["Net Sold Cost Act/Fct LLY"]/data["Cost Of Goods LLY"].sum())*100 
            # data["Budget Cost%"] =(data["Budget Cost"]*100)/(data["Budget Cost"].sum())
            # data["Cost Act/Forecast"] = data["Budget Cost"]+data["Cost Actuals"]
            # data["Budget vs Act/Forecast Cost%"] = ((data["Budget Cost"]/data["Cost Act/Forecast"].replace(0,np.nan))*100)
            # data["Cost Act/Forecast vs LY%"] = ((data["Cost Act/Forecast"] /data["Cost Of Goods LY"].replace(0,np.nan))*100)
            # data["Cost Act or Forecast /SKU"] = ((data["Cost Act/Forecast"] /data["Total SKU Count"].replace(0,np.nan))*100)
            # data["Cost Act/Forecast vs PPY %"] = ((data["Cost Act/Forecast"] /data["Cost Of Goods LLY"].replace(0,np.nan))*100)
            # data["Cost Budget/SKU"] = data["Budget Cost"]/data["Total SKU Count"].replace(0,np.nan)
            # data["Budget vs PY Cost%"] = ((data["Budget Cost"]/data["Cost Of Goods LY"].replace(0,np.nan))).fillna(0)*100
            # data["Budget vs PPY Cost%"] = ((data["Budget Cost"]/data["Cost Of Goods LLY"].replace(0,np.nan))).fillna(0)*100
# Float Div by 0 Error
            # data["Cost Mix%"] = (data["Cost Of Goods LY"]*100)/(data["Cost Of Goods LY"].sum())

            return data
            

    def calculate_margin(self, data: pd.DataFrame):
            '''
            !!The operations are performed here after grouping!!
            Calculation on the Dataframe
                Args:
                    df : DataFrame to be calculated
                Returns:
                    data: Calculated data frame

            '''
# --------------------------------------------------------------------------------------------------------------
            data["Margin Actuals"] = (data["Sales Actuals"] - data["Cost Actuals"])/data["Sales Actuals"].fillna(0)
            print(data["Budget Gross Margin %"].sum(), "sum of Bd----------->")
#"Buy Margin/SKU"
            # Budget Gross Margin = Budget Margin
            # data["Budget Margin"] =  data["Budget Gross Margin %"]/100
            data["Buddget Margin Mix %"] = 0
            data["Budget Margin vs Act/Fct Margin %"] = 0
            data["Budget Margin vs Net Sold Margin PY%"] = 0 
            data["Budget Margin vs Net Sold Margin PPY %"] = 0

            # Revised Tab
            data["Revised Budget Margin"] = 0
            data["Revised Budget Margin %"]= 0
            data['Revised Budget Margin vs Act/Fct Margin %'] = 0
            
            # Nt Sold tab
            data["Net Sold Margin Actuals TY"] = data["Margin Actuals"]
            data["Net Sold Margin Mix % TY"] = (data["Margin Actuals"]/(data["Margin Actuals"].sum()))*100
            data["Net Sold Margin Act/Fct TY"] = data["Sales Actuals"] - data["Cost Actuals"] # For nxt fmla
            data["Net Sold Margin Act/Fct Mix% TY"] = (data["Net Sold Margin Act/Fct TY"]/data["Net Sold Margin Act/Fct TY"].sum())*100
            # Need to get Budget Margin
            data["Net Sold Margin Act/Fct vs Budget % TY"] = 0
            data["Net Sold Margin Sales Deficit"] = 0 
            
            data["Net Sold Margin LY"] = 0 # ==> No Given equation given
            
            data["Net Sold Margin Act/Fct LY"] = data["Net Sales LY"] - data["Cost Of Goods LY"]
            data["Net Sold Margin Mix % LY"] = (data["Net Sold Margin Act/Fct LY"]/data["Net Sold Margin Act/Fct LY"].sum()) * 100
            data["Net Act/Fct Margin vs LY %"] = 0 # ===> Sold Margin LY not getted 
            data["Net Sold Margin PPY"] = 0 # ==> Not get
            data["Net Sold Margin Mix% PPY"] = 0 # ==> Not Gett
            data["Act/Fct vs PPY %"] = 0 #==> Not Getten

# --------------------------------------------------------------------------------------------------------------
            # data["Margin Actuals"] = (data["Sales Actuals"] - data["Cost Actuals"])/data["Sales Actuals"]
            # # data["Budget Gross Margin"] = data["Budget Gross Margin%"]/100
            # data["Budget Margin Mix%"] =(data["Budget Gross Margin"]*100)/(data["Budget Gross Margin"].sum())
            # data["Budget Margin%"] = (data["Budget Gross Margin"]/data["Budget Amount"])*100
            # data["Margin Act/Forecast"] = data["Budget Gross Margin"]+data["Margin Actuals"]

            # data["Budget vs Act/Forecast Margin%"] = ((data["Budget Gross Margin"]/data["Margin Act/Forecast"].replace(0,np.nan))*100)
            # data["Margin Act/Forecast vs LY%"] = ((data["Margin Act/Forecast"] /(data["LY Margin%"]).replace(0,np.nan))*100)
            # data["Margin Act or Forecast /SKU"] = ((data["Margin Act/Forecast"] /data["Total SKU Count"].replace(0,np.nan))*100)
            # data["Margin Act/Forecast vs PPY%"] = ((data["Margin Act/Forecast"] /(data["LLY Margin%"]).replace(0,np.nan))*100)
            # data["Margin Budget/SKU"] = data["Budget Gross Margin"]/data["Total SKU Count"].replace(0,np.nan)
            # data["Budget vs PY Margin%"] = ((data["Budget Gross Margin"]/(data["LY Margin%"]).replace(0,np.nan))).fillna(0)*100
            # data["Budget vs PPY Margin%"] = ((data["Budget Gross Margin"]/(data["LLY Margin%"]).replace(0,np.nan))).fillna(0)*100
            # # data["Margin Mix%"] = (data["Margin Actuals"]*100)/(data["Margin Actuals"].sum())
            # print(data["Margin Actuals"])

            return data
    def second_grouping(self,data,ds):
        channel_flag = False
        subs_count = 0
        group = []
        for col in data.columns:
            if col in self.max_col:
                group.append(col)
        try:
            sub_group = []
            sub_val = []
            cols = []
            vals = []

            if len(data)>0:
                temp = data.loc[0]
                for col in group:
                    if col not in self.heirarchy:
                        sub_group.append(col)
                        sub_val.append(temp[col])
                    else:
                        cols.append(col)
                        vals.append(temp[col])

                group = cols+sub_group[::-1]

                last_filter = 0
                if len(cols)>0:
                    last_filter = self.heirarchy.index(cols[-1])
                    if last_filter < len(self.heirarchy)-1:
                        last_filter = self.heirarchy.index(cols[-1]) + 1
                    else:
                        last_filter = self.heirarchy.index(cols[-1])
                elif len(sub_group)>0:
                    last_filter = 0
                if len(group)>0:
                    if group[0] != "Channel":
                        return ds,channel_flag
                if len(group) == 0:
                    return ds,channel_flag
                print(group)

                if len(group)>1:
                    filter = None
                    for col,val in zip(cols[1:]+sub_group,vals[1:]+sub_val):
                        if filter is None:
                            if col == "Budget date":
                                ds[col]=ds[col].astype(str)
                                filter = ds[col]  == str(val)
                            else:
                                filter = ds[col]  == val
                        else:
                            if col == "Budget date":
                                ds[col]=ds[col].astype(str)
                                filter = filter & (ds[col] == str(val))
                            else:
                                filter = filter & (ds[col] == val)
                            
                    if not ds[filter].empty:
                        ds = ds[filter]

                channel_flag = True
                ds = self.calculate_initial_frame(ds,["ItemLookupCode"])
                return ds,channel_flag
            else:
                return ds,channel_flag


        except:
            print(traceback.format_exc())



    # def calculate_bottom_first_tab(self,data):
    #     data["Budget/SKU"] = data["Budget Amount"]/data["Total SKU Count"] if data["Total SKU Count"] !=0 else 0
    #     data["Budget vs PY%"] = (data["Budget Amount"]/data["Sales LY"])*100 if data["Sales LY"] !=0 else 0
    #     data["Budget vs PPY%"] = (data["Budget Amount"]/data["PPrev netsale amount"])*100 if data["PPrev netsale amount"] !=0 else 0
    #     data["Units/SKU"] = data["Units Buy By SKU"]/data["Total SKU Count"] if data["Total SKU Count"] !=0 else 0
    #     data["Units/SKU LY"] = data["Units Buy By SKU LY"]/data["Total SKU Count LY"] if data["Total SKU Count LY"] !=0 else 0
    #     return data

        

    # def calculate_bottom_qty_tab(self,data):
    #     data["Quantity Act/Forecast vs Budget%"] = (data["Units Buy By SKU"]/data["Quantity Act/Forecast"])*100 if data["Quantity Act/Forecast"] !=0 else 0
    #     data["Quantity Act or Forecast /SKU"] = data["Quantity Act/Forecast"]/data["Total SKU Count"] if data["Total SKU Count"] !=0 else 0
    #     data["Quantity Act/Forecast vs LY%"] = (data["Quantity Act/Forecast"]/data["Units Buy By SKU LY"])*100 if data["Units Buy By SKU LY"] !=0 else 0
    #     data["Quantity Act/Forecast vs PPY%"] = (data["Quantity Act/Forecast"]/data["Quantity PPY"])*100 if data["Quantity PPY"] !=0 else 0
    #     data["Budget/SKU QTY"] = data["Units Buy By SKU"]/data["Total SKU Count"] if data["Total SKU Count"] !=0 else 0
    #     data["Budget vs PY QTY%"] = (data["Units Buy By SKU"]/data["Units Buy By SKU LY"])*100 if data["Units Buy By SKU LY"] !=0 else 0
    #     data["Budget vs PPY QTY%"] = (data["Units Buy By SKU"]/data["Quantity PPY"])*100 if data["Quantity PPY"] !=0 else 0
    #     return data



    # def calculate_bottom_cost_tab(self,data):

    #     data["Cost Act/Forecast"] = data["Budget Cost"]+data["Cost Actuals"]
    #     data["Budget vs Act/Forecast Cost%"] = (data["Budget Cost"]/data["Cost Act/Forecast"])*100 if data["Cost Act/Forecast"] !=0 else 0
    #     data["Cost Act/Forecast vs LY%"] = (data["Cost Act/Forecast"] /data["Cost Of Goods LY"])*100 if data["Cost Of Goods LY"] !=0 else 0
    #     data["Cost Act or Forecast /SKU"] = (data["Cost Act/Forecast"] /data["Total SKU Count"])*100 if data["Total SKU Count"] !=0 else 0
    #     data["Cost Act/Forecast vs PPY %"] = (data["Cost Act/Forecast"] /data["Cost Of Goods LLY"])*100 if data["Cost Of Goods LLY"] !=0 else 0
    #     data["Cost Budget/SKU"] = data["Budget Cost"]/data["Total SKU Count"] if data["Total SKU Count"] !=0 else 0
    #     data["Budget vs PY Cost%"] = (data["Budget Cost"]/data["Cost Of Goods LY"])*100 if data["Cost Of Goods LY"] !=0 else 0
    #     data["Budget vs PPY Cost%"] = (data["Budget Cost"]/data["Cost Of Goods LLY"])*100 if data["Cost Of Goods LLY"] !=0 else 0

    #     return data

    # def calculate_bottom_margin_tab(self,data):
    

    #     data["Margin Actuals"] = (data["Sales Actuals"] - data["Cost Actuals"])/data["Sales Actuals"] if data["Sales Actuals"] !=0 else 0
    #     data["Budget Margin%"] = (data["Budget Gross Margin"]/data["Budget Amount"])*100 if data["Budget Amount"] !=0 else 0
    #     data["Margin Act/Forecast"] = data["Budget Gross Margin"]+data["Margin Actuals"]
    #     data["Budget vs Act/Forecast Margin%"] = (data["Budget Gross Margin"]/data["Margin Act/Forecast"])*100 if data["Margin Act/Forecast"] !=0 else 0
    #     data["Margin Act/Forecast vs LY%"] = (data["Margin Act/Forecast"] / data["LY Margin%"])*100 if data["LY Margin%"] !=0 else 0
    #     data["Margin Act or Forecast /SKU"] = (data["Margin Act/Forecast"] /data["Total SKU Count"])*100 if data["Total SKU Count"] !=0 else 0
    #     data["Margin Act/Forecast vs PPY%"] = (data["Margin Act/Forecast"] /data["LLY Margin%"])*100 if data["LLY Margin%"] !=0 else 0
    #     data["Margin Budget/SKU"] = data["Budget Gross Margin"]/data["Total SKU Count"] if data["Total SKU Count"] !=0 else 0
    #     data["Budget vs PY Margin%"] = (data["Budget Gross Margin"]/data["LY Margin%"])*100 if data["LY Margin%"] !=0 else 0
    #     data["Budget vs PPY Margin%"] = (data["Budget Gross Margin"]/data["LLY Margin%"])*100 if data["LLY Margin%"] !=0 else 0

    #     return data


        