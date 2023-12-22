from fastapi import APIRouter, Depends, WebSocket,File,UploadFile,Form
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session
from sqlalchemy import text
import pandas as pd
import json
import numpy as np
import math
import datetime
import calendar
import traceback
from typing import Dict
import os

from core.database import  get_db
from .schemas import  Filters,LoginData
from .query import budget_query, filter_details
# from .querys import custom_agg
# from .TableChanges import table_changes,change_percent,change_value
from .para import Parameters,Budget


session = Session()
ssp = APIRouter(prefix="/ssp")  

DATA = pd.DataFrame()
ds = pd.DataFrame()
channel_flag = False
TEMP = {'key':DATA}
message = ''    


gloabal_vars = Parameters()


Budget = Budget()
write_to_db = Budget.write_to_db
update_users_tables = Budget.update_users_tables
change_percent = Budget.change_percent
change_value = Budget.change_value
read_file = Budget.read_file
get_session_id = Budget.get_session_id
save_table = Budget.save_table
tabs = json.dumps(gloabal_vars.tabs)


columns = gloabal_vars.columns
max_col = gloabal_vars.max_col
sum_col = gloabal_vars.sum_col
avg_col = gloabal_vars.avg_col
int_cols = gloabal_vars.int_cols
float_cols = gloabal_vars.float_cols
budget_cols = gloabal_vars.budget_cols


heirarchy = gloabal_vars.heirarchy

# SubFiltersGroup = gloabal_vars.


# upload_df = pd.DataFrame()
# budget_frame = pd.DataFrame()


@ssp.post("/upload")
async def use_upload_file(file: UploadFile = File(...), login_data: str = Form(...), db: Session = Depends(get_db)):

    login_data = json.loads(login_data)
    budget_frame,upload_status,status_code = read_file(file,budget_cols)

    if upload_status == None:
        try:
            updated_rows_count = write_to_db(db,budget_frame,budget_cols)
            # update = update_users_tables(db,login_data)
            
        except:
            print(traceback.format_exc())
            upload_status = "Error while saving to db"
            status_code = 400

    if upload_status == None:
        upload_status = f"Data saved successfully. {updated_rows_count} row(s) have been updated."
        status_code = 200
    print(upload_status)
    return JSONResponse(content={"status":upload_status},status_code=status_code)



@ssp.post("/save")
async def save_data(login_data:LoginData,db: Session = Depends(get_db)):

    df = TEMP['key']
    email_address = login_data.mail
    module_id = 'budget'
    print(email_address,"emailss")
    session_id,table_label = get_session_id(db,email_address,module_id)
    table_name = module_id + '_' + email_address.split('@')[0].lower() + '_' + table_label 
    update = update_users_tables(db,email_address,module_id,session_id,table_name)
    save_table(db,df,table_name)

    return JSONResponse(content={"message":"done"})






@ssp.get("/sub_filters")
async def sub_filters():
    # SubFilters = gloabal_vars.sub_filter
    return JSONResponse(content=Budget.SUB_FILTER)
# DATA
# data
# sub_data
@ssp.websocket("/get_data_ws")
async def get_data_ws(websocket: WebSocket,  db: Session = Depends(get_db)):
    DATA = Budget.DATA
    await websocket.accept()
    while True:
        data_filter = await websocket.receive_json()
        print(data_filter)
        filters = Filters(**data_filter)
        
        if data_filter["fetch_from_db"]:
                # email_address = data_filter["email"]
                # session_id = data_filter["version"]
                DATA,ds,message = Budget.get_from_db(filters,db)
                
                data = DATA
                sub_data = DATA

        group = []
        sub_filter_state = False
        filter_condition = None 

        TEMP['key'] = DATA
        # print(data_filter,"status")
        filter_condition,sub_filter_state,group = Budget.apply_secondary_filters(DATA, filters, filter_condition, sub_filter_state, group)
        # print("****************8*************apply filter data**************************")
        # print(filter_condition, sub_filter_state, group)
        # print("****************8*************apply filter data**************************")
        # get (None False []) 
        if data_filter['table_changes'] != {}:
            row = data_filter["table_changes"]["row"]
            columnID = data_filter["table_changes"]["columnId"]
            newValue = data_filter["table_changes"]["newValue"]
            columns_to_filter = []
            values_to_filter = []

            for i in group+heirarchy:
                if i in row:
                    columns_to_filter.append(i)
                    values_to_filter.append(data_filter['table_changes']["row"][i])

            
            child = filter_condition
            other_filter_condition = None
            parent = None
            for col, val in zip(columns_to_filter, values_to_filter):
                if child is None:
                    child = (DATA[col] == val)
                    other_filter_condition = ~(DATA[col] == val)
                    parent = None

                else:
                    parent = child
                    other_filter_condition = child & (DATA[col] != val)
                    child = child & (DATA[col] == val)
     

            original = row[columnID]
            if row[columnID] == None:
                original = 0
            increase = newValue - original  



            if columnID == "BudgetvsACT_FCT":

                budget_amount = (newValue*row["Sales Act/Forecast"])/100
                if parent is None: budget_amount_sum = DATA["Budget Amount"].sum()
                else: budget_amount_sum = DATA.loc[parent]["Budget Amount"].sum()
                newValue = (100*budget_amount)/budget_amount_sum
                columnID = "Budget%"

            if columnID == "Budget Amount":
   
                if parent is None and child is not None:
                    DATA.loc[child]["Budget Amount"] = (DATA.loc[child]["Budget%"] * newValue)/100

                elif child is None:
                    DATA["Budget Amount"] = (DATA["Budget%"] * newValue)/100 
                else:
                    ''' for child/grouped '''
                    DATA.loc[child]["Budget Amount"] = (DATA.loc[child]["Budget%"] * newValue)/100
         

            if columnID == "Budget vs PPY%":
                
                if (len(columns_to_filter)) ==1 and ((sub_filter_state ==True)):
                    DATA["Budget Amount"] = (DATA["PPrev netsale amount"] * newValue)/100

                elif child is None:
                    DATA["Budget Amount"] = (DATA["PPrev netsale amount"] * newValue)/100
                    columnID == "Budget Amount"

                else:

                    ''' for child/grouped '''
                    budget_amount = (row["PPrev netsale amount"] * newValue)/100

                    if parent is None: budget_amount_sum = DATA["Budget Amount"].sum()
                    else: budget_amount_sum = DATA.loc[parent]["Budget Amount"].sum()
                    if budget_amount < budget_amount_sum:
                        newValue = (100*budget_amount)/budget_amount_sum
                        columnID = "Budget%"



            if columnID == "Units/SKU":
                budget_amount = (newValue * row["Retail_Price"]*row["Total SKU Count"])
                if parent is None: budget_amount_sum = DATA["Budget Amount"].sum()
                else: budget_amount_sum = DATA.loc[parent]["Budget Amount"].sum()
                if budget_amount < budget_amount_sum:
                    newValue = (100*budget_amount)/budget_amount_sum
                    columnID = "Budget%"

            
            if columnID == "Units Buy By SKU":
                budget_amount = newValue * row['Retail_Price']
                if parent is None: budget_amount_sum = DATA["Budget Amount"].sum()
                else: budget_amount_sum = DATA.loc[parent]["Budget Amount"].sum()
                if budget_amount < budget_amount_sum:
                    newValue = (100*budget_amount)/budget_amount_sum
                    columnID = "Budget%"

            if columnID == "Budget/SKU":
                budget_amount = newValue * row["Total SKU Count"]
                if parent is None: budget_amount_sum = DATA["Budget Amount"].sum()
                else: budget_amount_sum = DATA.loc[parent]["Budget Amount"].sum()
    
                if budget_amount < budget_amount_sum:
                    newValue = (100*budget_amount)/budget_amount_sum
                    columnID = "Budget%"

            if columnID == "Budget vs PY%":
                if (len(columns_to_filter)) ==1 and ((sub_filter_state ==True)):
                    DATA["Budget Amount"] = (DATA["Sales LY"] * newValue)/100

                elif child is None:
                    DATA["Budget Amount"] = (DATA["Sales LY"] * newValue)/100
                    columnID == "Budget Amount"

                else:
                    ''' for child/grouped '''
                    budget_amount = (row["Sales LY"] * newValue)/100

                    if parent is None: budget_amount_sum = DATA["Budget Amount"].sum()
                    else: budget_amount_sum = DATA.loc[parent]["Budget Amount"].sum()
                    if budget_amount < budget_amount_sum:
                        newValue = (100*budget_amount)/budget_amount_sum
                        columnID = "Budget%"
        
            if columnID == 'Budget%':
                s = data['RelativeBudget%'].sum()
                increase = (s*newValue/100) - (s*row[columnID]/100)

                if sub_filter_state ==True:
                    DATA['Budget%']=(DATA['Budget%']/DATA['Budget%'].sum())*100
                    DATA['RelativeBudget%'] = DATA['Budget%']
                    DATA.loc[parent] = change_percent(grouped_df=DATA.loc[child],other_grouped_df=DATA.loc[other_filter_condition],increase= increase, colID= columnID)
                else:
                    if len(columns_to_filter) == 1:
                        DATA = change_percent(grouped_df=DATA.loc[child],other_grouped_df=DATA.loc[other_filter_condition],increase= increase, colID= columnID)
                        # summation = DATA["Budget Amount"].sum()
                        # DATA['Budget%'] = (DATA["Budget Amount"]/summation)*100
                    else:
                        DATA.loc[parent] = change_percent(grouped_df=DATA.loc[child],other_grouped_df=DATA.loc[other_filter_condition],increase= increase, colID= columnID)
                        # summation = DATA["Budget Amount"].sum()
                        # DATA['Budget%'] = (DATA["Budget Amount"]/summation)*100 

            elif columnID != None :

                if child is None:
                    ''' since grouping column is empty it won't go to grouping block, so we had to change the 'data' to DATA
            manually, also need to do aggregation inside here,though it's messy '''
                    agg_dict = {col : 'mean' for col in avg_col}
                    agg_dict.update({col: 'sum' for col in sum_col})
                    sums = DATA.agg(agg_dict)
                    data = pd.DataFrame(sums.values, sums.index).T


                else:
                    if sub_filter_state == True:

                        if len(columns_to_filter) == 1:
                            ''' to manage 1 inner block,if 1 group'''
                            DATA = change_percent(grouped_df=DATA.loc[child],other_grouped_df=DATA.loc[other_filter_condition],increase= increase, colID= columnID)
                            data = DATA.loc[filter_condition].groupby(group,as_index=False).agg(agg_dict)
                            summation = data["Budget Amount"].sum()
                            data['Budget%'] = (data["Budget Amount"]/summation)*100 
                        else:

                            DATA.loc[parent] = change_percent(grouped_df=DATA.loc[child],other_grouped_df=DATA.loc[other_filter_condition],increase= increase, colID= columnID)
                            summation = DATA["Budget Amount"].sum()
                            DATA['Budget%'] = (DATA["Budget Amount"]/summation)*100 
 
                    else:
                        if len(columns_to_filter) == 1:
                            ''' to manage 1 inner block,if 1 group'''
                            DATA = change_percent(grouped_df=DATA.loc[child],other_grouped_df=DATA.loc[other_filter_condition],increase= increase, colID= columnID)
                            summation = DATA["Budget Amount"].sum()
                            DATA['Budget%'] = (DATA["Budget Amount"]/summation)*100
                        else:
                            ''' to manage more than 1 inner block,if more than 1 group'''
                            DATA.loc[parent] = change_percent(grouped_df=DATA.loc[child],other_grouped_df=DATA.loc[other_filter_condition],increase= increase, colID= columnID)
                            summation = DATA["Budget Amount"].sum()
                            DATA['Budget%'] = (DATA["Budget Amount"]/summation)*100 

#calculate the budget sum                            
#re calculate the budget amount
# recalculte the ba with new budget %
#get the difference,of budget amount

            if columnID == 'Budget%':
                summation = DATA["Budget Amount"].sum()
                # print(summation,"Sum")
                # print(DATA['Budget%'],"percentage5")
                DATA["Budget Amount"] = (DATA[columnID]*summation)/100
                # DATA["Budget Amount"] = np.where(DATA['Budget%']<=0,0,DATA["Budget Amount"])

                # correctd_summation = DATA["Budget Amount"].sum()
                # print(correctd_summation,"corSum")
                # diff = summation - correctd_summation 
                # print(diff,"dfiif")
                # df1 = DATA[DATA["Budget Amount"]>0]
                # df2 = DATA[DATA["Budget Amount"]<=0]
                # df1["temp"] = (df1["Budget Amount"]/df1["Budget Amount"].sum())*100
                # df1["Budget Amount"] = df1["Budget Amount"] + (diff*df1["temp"])
                # ck = df1["Budget Amount"].sum()
                # print(ck,summation,ck-summation,"dct check")
                # df1 = df1.drop("temp", axis=1)


                # df= pd.concat([df1,df2])
                # DATA["Budget Amount"] = df["Budget Amount"]
                
                # DATA["Budget Amount"] = (DATA[columnID]*summation)/100

                # DATA["Budget Amount"] = DATA["Budget Amount"] + (diff*DATA['Budget%'])


                DATA['Units Buy By SKU'] = (DATA["Budget Amount"]/DATA['Retail_Price'].replace(0,np.nan)).replace([np.inf,-np.inf],np.nan).apply(lambda x:round(x) if not pd.isna(x) else 0) 
                DATA['Deficit'] = DATA["Budget Amount"] - (DATA['Retail_Price']*DATA['Units Buy By SKU'])
                DATA['RelativeBudget%'] = (100*DATA['Budget Amount']/DATA['Budget Amount'].sum()).fillna(0)
                DATA["Budget vs Act/Forecast %"] = ((DATA["Budget Amount"]/DATA["Sales Actuals"])*100).fillna(0)
                DATA["Sales Act/Forecast"] = DATA["Budget vs Act/Forecast %"]/100
                DATA["Budget vs PY%"] = ((DATA["Budget Amount"]/DATA["Sales LY"])*100).fillna(0)
                DATA["Budget vs PPY%"] = ((DATA["Budget Amount"]/DATA["PPrev netsale amount"])*100).fillna(0)
                DATA["Units/SKU"] = (DATA["Units Buy By SKU"]/DATA["Total SKU Count"].replace(0,np.nan)).replace([np.inf,-np.inf],np.nan).fillna(0)
                DATA["Budget/SKU"] = DATA["Budget Amount"]/DATA["Total SKU Count"].replace(0,np.nan)

            


        # DATA = table_changes(DATA,data,data_filter,filter_condition)
        # data = expand(DATA,data_filter ,filter_details ,filter_condition, sub_filter_state)
   

       

        if data_filter["group_by"]['status'] and data_filter['table_changes'] == {}:
            # for g in group:
            #     print(DATA[g].unique(),"uniq")
            # print(data_filter["group_by"]["columns"],"dats")
            # print(group,"groupin")
            if not len(data_filter["group_by"]["columns"]) == 0:
                for i in  data_filter['group_by']['columns']:
                    if i in filter_details:
                        print(i,"groupin2")
                        group.append(filter_details[i])
                print('groupby', group)
                limit = max_col.index(group[-1])
                agg_dict = {col : 'mean' for col in avg_col}
                agg_dict.update({col: 'sum' for col in sum_col})
                if filter_condition is not None:
                    try:
                        print("group_group None",group)
                        data = DATA.loc[filter_condition].groupby(group,as_index=False).agg(agg_dict)
                        Budget.SUB_FILTER = Budget.call_filter(DATA.loc[filter_condition],Budget.SUB_FILTER,group,DATA,Budget.filter_store)
                        
                    except Exception as e:
                        print(f"error is {e}")
                        pass

                else:
                    try:
                        data =  DATA.groupby(group,as_index=False).agg(agg_dict)
                    except:
                        pass
                    # Budget.SUB_FILTER = call_filter(DATA.loc[filter_condition])

            else:
                agg_dict = {col : 'mean' for col in avg_col}
                agg_dict.update({col: 'sum' for col in sum_col})
                if not sub_filter_state:
                    try:
                        datas = Budget.calculate_initial_frame(DATA,["ItemLookupCode"])
                        sums = datas.agg(agg_dict)
                        data = pd.DataFrame(sums.values, sums.index).T
                    except:
                        pass
                    # Budget.SUB_FILTER = call_filter(DATA.loc[filter_condition])

                else:
                    try:
                        item_group = group + ["ItemLookupCode"]
                        datas = Budget.calculate_initial_frame(DATA,item_group)
                        data = datas.loc[filter_condition].groupby(group,as_index=False).agg(agg_dict)
                        Budget.SUB_FILTER = Budget.call_filter(DATA.loc[filter_condition],Budget.SUB_FILTER,group,DATA,Budget.filter_store)
        
                    except:
                        pass
                    # print(Budget.SUB_FILTER)


            


        if data_filter["expand"]['status']:
            columns_to_filter =[]
            values_to_filter = []
            selected_row = data_filter['expand']["row"]
            for i in selected_row:
                if i in heirarchy:
                    columns_to_filter.append(i)
                    values_to_filter.append(selected_row[i])
                    last_filter = heirarchy.index(columns_to_filter[0])
            group_to_filter = []
            group_value_to_filter = []
            for i in selected_row:
                if i in group:
                    group_to_filter.append(i)
                    group_value_to_filter.append(selected_row[i])
            for col, val in zip(columns_to_filter+group_to_filter, values_to_filter+group_value_to_filter):
                if filter_condition is None:
                    try:
                        filter_condition = (DATA[col] == val)
                    except Exception as e:
                        print(f"wrong heirarchy: {e}")
                else:
                    filter_condition = filter_condition & (DATA[col] == val)
  
            if columns_to_filter == []:
                last_filter = 0
                agg_dict = ({col : 'mean' for col in avg_col})
                agg_dict.update({col: 'sum' for col in sum_col})
                group.append(heirarchy[last_filter])
                group = list(set(group))
                item_group = group + ["ItemLookupCode"]
                if sub_filter_state:
                    try:
                        datas = Budget.calculate_initial_frame(DATA,item_group)
                        # data = datas.loc[filter_condition].groupby(group,as_index=False).agg(agg_dict)
                        data = datas.loc[filter_condition].groupby(group,as_index=False).agg(agg_dict)
                        Budget.SUB_FILTER = Budget.call_filter(DATA.loc[filter_condition],Budget.SUB_FILTER,group,DATA,Budget.filter_store)

                    except:
                        pass
                else:
                    datas = Budget.calculate_initial_frame(DATA,item_group)
                    data = datas.groupby(group,as_index=False).agg(agg_dict)
            else:

                try:
                    groups = max_col[limit:max_col.index(columns_to_filter[0])+1]
                except:
                    groups = max_col[:max_col.index(columns_to_filter[0])+1]

                agg_dict = {col: 'max' for col in groups}
                agg_dict.update({col : 'mean' for col in avg_col})
                agg_dict.update({col: 'sum' for col in sum_col})
                group.append(heirarchy[last_filter+1])
                
                item_group = group+["ItemLookupCode"]

       
                item_group = list(set(item_group + heirarchy[:last_filter+1]))
                datas = Budget.calculate_initial_frame(DATA,item_group)
                print("****************************datas.cols W*******************************")
                print(datas.columns.to_list())
                print("****************************datas.cols X*******************************")
                data = datas.loc[filter_condition].groupby(group,as_index=False).agg(agg_dict)
                print("****************************data.cols X*******************************")
                print(data.columns.to_list())
                print("****************************data.cols X*******************************")
                Budget.SUB_FILTER = Budget.call_filter(datas.loc[filter_condition],Budget.SUB_FILTER,group,DATA,Budget.filter_store)



        try:
            """Call tabs"""
            # if data_filter['tab_name'] == 'raBudgetValue':
            # data = Budget.calculate_df(data,ds)
            print("Sales ACT/Forecast vanished after this line")
            data,dtemp,channel_flag = Budget.calculate_df(data,ds)
            # if data_filter['tab_name'] == 'raBudgetMargin':
            data = Budget.calculate_margin(data)
            # if data_filter['tab_name'] == 'raBudgetQuantity':
            data = Budget.calculate_quantity(data)
            # if data_filter['tab_name'] == 'raBudgetMargin':
            data = Budget.calculate_cost(data)
            # data = Budget.calculate_df(data)
            agg_dict = ({col : 'mean' for col in avg_col})
            agg_dict.update({col: 'sum' for col in sum_col})
            # bottom_column              = (data.agg(agg_dict))
            # bottom_column[int_cols]    = bottom_column[int_cols].fillna(0).astype(int)
            # bottom_column[float_cols]  = bottom_column[float_cols].fillna(0).astype(float).round(2)

            if channel_flag == False:
                print("here it was the falg")
                bottom_column              = (data.agg(agg_dict))
                bottom_column = Budget.calculate_bottom_first_tab(bottom_column.to_dict())
                bottom_column = Budget.calculate_bottom_margin_tab(bottom_column)
                bottom_column = Budget.calculate_bottom_cost_tab(bottom_column)
                bottom_column = Budget.calculate_bottom_qty_tab(bottom_column)


            else:
                print("no it was the flag")
                print(agg_dict)
                bottom_column = (dtemp.agg(agg_dict))
                bottom_column = Budget.calculate_bottom_first_tab(bottom_column.to_dict())
                bottom_column = Budget.calculate_bottom_margin_tab(bottom_column)
                bottom_column = Budget.calculate_bottom_cost_tab(bottom_column)
                bottom_column = Budget.calculate_bottom_qty_tab(bottom_column)

        


                

                # bottom_dict = {}
                # temp_main_agg              = (data.agg(agg_dict))
                # bottom_temp = dtemp.agg(agg_dict).to_dict()
                # print(bottom_temp)
                # for col in list(bottom_temp.keys()):
                #     if col in non_article_cols:
                #         bottom_temp[col] = temp_main_agg[col]

                # bottom_column = pd.Series(bottom_temp)
                # print(bottom_column["SKU/Article Efficency"],"eff")
                # bottom_column[int_cols]    = bottom_column[int_cols].astype(int)
                # bottom_column[float_cols]  = bottom_column[float_cols].astype(float).round(2)
                # print(bottom_column)
                # df.columns
                # bottom_column = bottom_temp
            bottom_column = pd.Series(bottom_column)
            bottom_column[int_cols]    = bottom_column[int_cols].astype(int)

        except:
            print(traceback.format_exc())
            bottom_column = data

            
        print(data.columns.to_list)
        print(sum_col)

        sums = data[sum_col].sum().to_json()
        size = len(data) 

        if size == 1:
            editable_cols = json.dumps(["Budget Amount","Budget vs PY%","Budget vs PPY%"])
        else:
            editable_cols = json.dumps(gloabal_vars.editable_cols)

 
        if "sort" in data_filter:
            try:
                sort_columnid = data_filter["sort"]["id"]
                if data_filter["sort"]["desc"] == False:
                    datas = data.sort_values(by =[sort_columnid], ascending= True)[filters.page_number*filters.page_size:(filters.page_number+1)*filters.page_size].round(2)
                elif data_filter["sort"]["desc"] == True:
                    datas =data.sort_values(by =[sort_columnid], ascending= False)[filters.page_number*filters.page_size:(filters.page_number+1)*filters.page_size].round(2)
            except:
                datas = data.iloc[(filters.page_number*filters.page_size):((filters.page_number+1)*filters.page_size)].round(2)
            # print(datas.columns.to_list(), "final datas")
            datas[float_cols] = datas[float_cols].fillna(0).astype(float).round(2)
            datas[int_cols]   = datas[int_cols].fillna(0).astype(int)
            data_json = f"""{datas.to_json(orient='split')[:-1]},"message":{json.dumps(message)},"editable_cols":{editable_cols},"tabs":{tabs} ,"items":{size},"total":{bottom_column.to_json()} {datas.to_json(orient='split')[-1]}"""

            await websocket.send_text(data_json)