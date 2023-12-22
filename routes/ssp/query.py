import pandas as pd
import numpy as np
from datetime import datetime, timedelta

filter_details = {
        "sales_channel":"Channel",
        "product_family":"Family",
        "sub_families":"SubFamily",
        "category":"Category",
        "sub_category":"SubCategory",
        "suppliers":"Supplier",
        "sku":"ItemLookupCode"}

#on save create new transaction instead new table

def custom_agg(series):
    if pd.api.types.is_numeric_dtype(series):
        return series.sum()


from datetime import datetime, timedelta

def form_intervals_by_year(start_date, stop_date):
    start_date = datetime.strptime(start_date, "%Y-%m-%d")
    stop_date = datetime.strptime(stop_date, "%Y-%m-%d")
    intervals = []
    cur_year = datetime.now().year
    l_year = cur_year - 1
    ll_year = l_year - 1
    years = [cur_year,l_year,ll_year]
    
    for year in years:
        current_date = start_date
        while current_date <= stop_date:
            if current_date.year == year:
                interval_start = current_date
                while current_date.year == year and current_date <= stop_date:
                    current_date += timedelta(days=1)
                interval_end = current_date - timedelta(days=1)
                intervals.append((interval_start.strftime('%Y-%m-%d'), interval_end.strftime('%Y-%m-%d')))
            else:
                current_date += timedelta(days=1)

    return intervals,years


# start_date = datetime(2022, 1, 15)
# stop_date = datetime(2023, 2, 10)
def get_sales_year(start_date, stop_date):
    st = ""
    intervals,years = form_intervals_by_year(start_date, stop_date)
    interval_map = {}
    for interval in intervals:
        dt = datetime.strptime(interval[0], "%Y-%m-%d")
        interval_map[dt.year] = interval
    
    for year in years:                   
        if year not in interval_map:
            interval_map[year] = 0      # TY                                                                LY                                                  LLY
                                        # SalesActual --> NetSalesActual-TY              prev_netsale_amount --> prev_netsale_amount/NetSalesActual-LY        p_prev_netsale_amount --> p_prev_netsale_amount/NetSalesActualLLY
                                        # QuantityActuals --> NetSoldQuantityActuals-TY  CostOfGoodsLY       --> CostOfGoodsLY/NSOLDCOST-LY                   CostOfGoodsLLY        --> CostOfGoodsLLY/NSOLDCOST-LLY
                                        # CostActuals --> NetSoldCostActuals-TY          SoldQtyLY           --> NtSoldQty-LY                                 QuantityPPY           --> NetSoldQuantity-PPY
    labels = dict(zip(years,[['"NetSalesActual-TY"','"NetSoldQuantityActuals-TY"','"NetSoldCostActuals-TY"'],['"prev_netsale_amount/NetSalesActual-LY"','"CostOfGoodsLY/NSOLDCOST-LY"','"NtSoldQty-LY"','"GrossSalesLY"'],['"p_prev_netsale_amount/NetSalesActual-LLY"','"CostOfGoodsLLY/NSOLDCOST-LLY"','"NetSoldQuantity-PPY"']]))
    metrics = dict(zip(years,[['"Net_Sales"','"Quantity"','"Item_Cost"'],['"Net_Sales"','"Item_Cost"','"Quantity"','"Gross_Sales"'],['"Net_Sales"','"Item_Cost"','"Quantity"']]))
	

    mins = intervals[0][0]
    maxs = intervals[0][1]
    interval_map = dict(sorted(interval_map.items()))
    for key,val in interval_map.items():
        if interval_map[key] == 0:
            for val in  labels[key]:
                st += f"""0 AS {val}, """
        else:
            mins = min(mins,interval_map[key][0])
            maxs = max(maxs,interval_map[key][1])
            for i in range(len(labels[key])):
                st += f"""SUM(CASE WHEN "Sold_Date" BETWEEN '{interval_map[key][0]}' AND '{interval_map[key][1]}' THEN {metrics[key][i]} ELSE 0 END) AS {labels[key][i]}, """
    # print(",".join(st.split(",")[:-1]),[mins,maxs])
    return ",".join(st.split(",")[:-1]),[mins,maxs]


def budget_query(filter,table_name):
    filters_for_query = ""
    filters = filter.model_dump()
    filter_details = {
    "sales_channel":"Channel",
    "product_family":"Family",
    "sub_families":"SubFamily",
    "category":"Category",
    "sub_category":"SubCategory",
    "suppliers":"Supplier",
    "sku":"ItemLookupCode"}
    ts = "hi"

    for key,value in filter_details.items():
        if len(filters[key]) == 1:
            filters_for_query += f""" and "{value}" = '{filters[key][0]}' """
        elif len(filters[key]) > 1:
            filters_for_query += f""" and "{value}" in {tuple(filters[key])} """
    ms = "sh"
	
    pp_year2 = (datetime.strptime(filter.history_date_range.fro, '%Y-%m-%d') - timedelta(days=365)).strftime('%Y-%m-%d')
    sales_query,dates = get_sales_year(filter.history_date_range.fro, filter.history_date_range.to)
    print(sales_query, "sales_query")
    print(dates, "datesssss")
    query = f"""
	WITH 
	StockTable AS(
			SELECT "ItemLookupCode", "StoreID",MAX("Region") AS "Region",
    			SUM("Quantity") AS "StockOnHandQty"
			FROM stock
			GROUP BY "ItemLookupCode", "StoreID"
	),
	budget AS(
			SELECT "ItemLookupCode",
				   "Channel",
				   "StoreId",
					MAX("Description") AS "Description",
					MAX("Department") AS "Department",
					MAX("CategoryName") AS "Category",
					MAX("Family") AS "Family",
					MAX("subFamily") AS "SubFamily",
					MAX("Supplier") AS "Supplier",
					MAX("SubCategory") AS "SubCategory",
					MAX("ExtendedSubCategory") AS "ExtendedSubCategory",
					MAX("SubCategorySupplier") AS "SubCategorySupplier",
					MAX("AssemblyCodeNickname") AS "AssemblyCodeNickName",
					MAX("ENDOFLife") AS "ENDOFLife",
					MAX("DOM_COMM") AS "DOM_COMM",
					MAX("StoreName") AS "Store",
					MAX("Status") AS "Status",
            		DATE_PART('Year', MAX("BDate"))  AS "BudgetYear",
					SUM("BudgetQTY") AS "BudgetQTY",
					MAX("Price") AS "Price",
       				SUM("BudgetValue") AS "BudgetAmount",
					"BDate",
					1 AS "TotalSkuCount",
					SUM("BudgetQTY") AS "UnitsBuyBySku",
					SUM("BudgetCost") AS "BudgetCostTY",
					(SUM("BudgetValue")-SUM("BudgetCost"))/NULLIF(SUM("BudgetValue"),0)::FLOAT*100 AS "BudgetGrossMarginTY%"
        	FROM budget_items 
			WHERE "BDate" BETWEEN '{filter.forecast_date_range.fro}' AND '{filter.forecast_date_range.to}'
				AND "BudgetValue" IS NOT NULL AND "BudgetCost" IS NOT NULL 
				AND "BudgetValue"> "BudgetCost" AND "Price" != 0 AND "Price" IS NOT NULL
-- 							{filters_for_query}
        	GROUP BY "ItemLookupCode", "Channel","StoreId","BDate"
     ),
	HistBudgetTable AS(
        	SELECT "ItemLookupCode","Channel","StoreId",
				SUM("BudgetQTY") AS "BudgetQTYLy",
				SUM("BudgetQTY") AS "UnitsBuyBySkuLY",
				SUM("BudgetValue") AS "budget_amountLy",
				1 AS "TotalSkuCountLY"

			FROM budget_items
			WHERE "BDate" BETWEEN '{filter.history_date_range.fro}' AND '{filter.history_date_range.to}'
				AND "BudgetValue" IS NOT NULL AND "BudgetCost" IS NOT NULL {filters_for_query} AND
				"BudgetValue"> "BudgetCost"
				GROUP BY "ItemLookupCode", "Channel","StoreId"
	),
-- 	 We select PPY, PY, TY net sales from maximum and minimum intervals '{dates[0]}' AND '{dates[1]}' 
	
     salestable_hist AS(
	 		SELECT "Item_Code","Channel", "TRX_Store_Id",
-- error		  SUM("BudgetQTY") AS "UnitsBuyBySkuLY",
					{sales_query}, 
--     	          0 AS "p_prev_netsale_amount/NetSalesActual-LLY",
--				  0 AS "CostOfGoodsLLY/NSOLDCOST-LLY",
--				  0 AS "NetSoldQuantity-PPY",
--				  0 AS "prev_netsale_amount/NetSalesActual-LY",
--				  0 AS "CostOfGoodsLY/NSOLDCOST-LY",
--				  0 AS "NtSoldQty-LY",
--				  0 AS "GrossSalesLY",
--				  SUM(CASE WHEN "Sold_Date" BETWEEN '2023-12-01' AND '2023-12-15' THEN "Net_Sales" ELSE 0 END) AS "NetSalesActual-TY",
--				  SUM(CASE WHEN "Sold_Date" BETWEEN '2023-12-01' AND '2023-12-15' THEN "Quantity" ELSE 0 END) AS "NetSoldQuantityActuals-TY",
--				  SUM(CASE WHEN "Sold_Date" BETWEEN '2023-12-01' AND '2023-12-15' THEN "Item_Cost" ELSE 0 END) AS "NetSoldCostActuals-TY",
		 		DATE_PART('Year',MAX("Sold_Date")) AS "HistoricalYear",
				0 AS "netsale_amount",
        		MAX("Sold_Date") AS "SoldDate"
			FROM sales_21_23
			WHERE "Sold_Date" BETWEEN '{dates[0]}' AND '{dates[1]}'  
				AND	"Channel" IN (SELECT "Channel" FROM budget) 
		 		AND	"TRX_Store_Id" IN (SELECT "StoreId" FROM budget) 
		 		AND "Item_Code" IN (SELECT "ItemLookupCode" FROM budget)
			GROUP BY "Item_Code","Channel", "TRX_Store_Id"		 
	 ),
	 Ra_table AS(
	 		SELECT 
				t1."ItemLookupCode",t1."Channel",t1."StoreId",t1."Store",t1."Department",t1."Category",
				t1."Family",t1."SubFamily",t1."DOM_COMM",t1."Supplier",t1."SubCategory",t1."ExtendedSubCategory",
				t1."SubCategorySupplier", t1."AssemblyCodeNickName",t1."Description",t1."Status",
				t1."ENDOFLife",t1."BDate", t1."BudgetYear", t1."BudgetAmount", t1."UnitsBuyBySku", t1."TotalSkuCount",

				(t1."BudgetAmount"/(SELECT SUM("BudgetAmount") FROM budget))::FLOAT*100 AS "Budget%",
				((t1."BudgetAmount")/NULLIF(t2."netsale_amount",0))::FLOAT*100 AS "BudgetVAct/Frcst Percentage",
				COALESCE(t1."BudgetAmount"/NULLIF(t2."prev_netsale_amount/NetSalesActual-LY",0),0)*100 AS "BudgetVPY%",
				COALESCE(t1."BudgetAmount"/NULLIF(t2."CostOfGoodsLLY/NSOLDCOST-LLY",0),0)*100 AS "BudgetVPPY%",
				t1."BudgetCostTY" AS "BudgetAmountTY", t1."BudgetGrossMarginTY%",

				t2."NetSalesActual-TY", t2."prev_netsale_amount/NetSalesActual-LY",t2."p_prev_netsale_amount/NetSalesActual-LLY",
				t2."NetSoldCostActuals-TY", t2."CostOfGoodsLLY/NSOLDCOST-LLY", t2."CostOfGoodsLY/NSOLDCOST-LY",
				t2."NetSoldQuantityActuals-TY", t2."NtSoldQty-LY", t2."NetSoldQuantity-PPY",
				t2."HistoricalYear", t2."SoldDate",		 
				((t2."NetSalesActual-TY")/NULLIF(t2."NetSalesActual-TY",0))::FLOAT*100 AS "nt sales Act/ForecastvsTY%",
				((t2."NetSalesActual-TY")/NULLIF(t2."prev_netsale_amount/NetSalesActual-LY",0))::FLOAT*100 AS "nt sales Act/ForecastvsLY%",
				((t2."NetSalesActual-TY")/NULLIF(t2."p_prev_netsale_amount/NetSalesActual-LLY",0))::FLOAT*100 AS "nt sales Act/ForecastvsPPY%",
				t2."prev_netsale_amount/NetSalesActual-LY"/NULLIF(t2."NtSoldQty-LY",0) AS "FinalPrice",
				(t2."prev_netsale_amount/NetSalesActual-LY"/NULLIF((SELECT SUM("prev_netsale_amount/NetSalesActual-LY") FROM salestable_hist),0))*100::FLOAT AS "NetSalesMixLY%",
				(t2."NetSalesActual-TY"/NULLIF((SELECT SUM("NetSalesActual-TY") FROM salestable_hist),0))*100::FLOAT AS "NetSalesMixTY%",
				t2."netsale_amount"
		 
	 		 FROM budget t1 
			 LEFT JOIN salestable_hist t2
				ON t1."ItemLookupCode" = t2."Item_Code"
				AND t1."Channel" = t2."Channel"
				AND t1."StoreId" = t2."TRX_Store_Id"
),
-- SalesAndBudget t1, StockTable t2
	CurrentYearTable AS(
			SELECT t1."Channel",t2."Region",t1."StoreId",t1."Store",t1."Department",t1."Category",t1."Family",t1."SubFamily",
				t1."DOM_COMM",t1."Supplier",t1."SubCategory",t1."ExtendedSubCategory",t1."SubCategorySupplier",
				t1."AssemblyCodeNickName",t1."Status",t1."ENDOFLife",t1."ItemLookupCode",t1."Description",t1."BDate",
				t1."BudgetYear",t1."HistoricalYear",
				t1."BudgetAmount", 0 AS "Deficit", t1."BudgetAmountTY",
				t1."Budget%",
				t1."BudgetVAct/Frcst Percentage",t1."BudgetVPY%",t1."BudgetVPPY%",
				t1."UnitsBuyBySku",1 AS "SKU_COUNT",
				t1."netsale_amount",
				t1."prev_netsale_amount/NetSalesActual-LY",t1."p_prev_netsale_amount/NetSalesActual-LLY",
				t1."NetSalesActual-TY",t1."TotalSkuCount",t1."SoldDate",t1."NetSoldQuantityActuals-TY",t1."NetSoldCostActuals-TY",t1."CostOfGoodsLY/NSOLDCOST-LY",t1."NtSoldQty-LY",
				t1."CostOfGoodsLLY/NSOLDCOST-LLY", t1."NetSoldQuantity-PPY", 
				COALESCE(t1."NetSalesActual-TY", 0) + COALESCE(t1."BudgetAmount",0) AS "Sales Actual/Forecast TY",
				t1."BudgetGrossMarginTY%",

				t2."StockOnHandQty"
			FROM Ra_table t1
			LEFT JOIN StockTable t2
				ON t1."ItemLookupCode" = t2."ItemLookupCode"
				AND t1."StoreId" = t2."StoreID"
),

 HIstbudandcur AS (
	SELECT t1."Channel",'country',t1."Region",'area','city',t1."StoreId",t1."Store",'season',t1."Department",t1."Family",t1."SubFamily",t1."Category",
		t1."DOM_COMM",t1."SubCategory",t1."ExtendedSubCategory",t1."Supplier",t1."SubCategorySupplier",
		t1."AssemblyCodeNickName",t1."Status",t1."ENDOFLife",t1."Description",t1."ItemLookupCode",t1."HistoricalYear",t1."BudgetYear",'Month','Week',t1."BDate",
        t1."BudgetAmount", 0 AS "Deficit", 'Revised Budget',
		t1."Budget%",t1."BudgetVAct/Frcst Percentage"/100 AS "BudgetVAct/frcast",
		t1."BudgetVAct/Frcst Percentage",t1."BudgetVPY%",t1."BudgetVPPY%",
		t1."UnitsBuyBySku",t1."TotalSkuCount",
		t1."prev_netsale_amount/NetSalesActual-LY",t1."p_prev_netsale_amount/NetSalesActual-LLY",
		t1."netsale_amount",t1."NetSalesActual-TY",t1."SoldDate",t1."NetSoldQuantityActuals-TY",t1."NetSoldCostActuals-TY",t1."CostOfGoodsLY/NSOLDCOST-LY",t1."NtSoldQty-LY",
        t1."CostOfGoodsLLY/NSOLDCOST-LLY",t1."NetSoldQuantity-PPY",t1."StockOnHandQty",
		t1."BudgetAmountTY", 
		COALESCE(t1."prev_netsale_amount/NetSalesActual-LY",0) + COALESCE(t2."budget_amountLy", 0) AS "Sales Actual/Forecast LY",
		COALESCE(t1."p_prev_netsale_amount/NetSalesActual-LLY",0) + COALESCE(t1."BudgetAmount", 0) AS "Sales Actual/Forecast LLY",
		t1."BudgetGrossMarginTY%",

		t2."UnitsBuyBySkuLY", t1."Sales Actual/Forecast TY", t2."budget_amountLy"
	FROM CurrentYearTable t1
	LEFT JOIN HistBudgetTable t2
 		ON t1."ItemLookupCode" = t2."ItemLookupCode"
		AND t1."StoreId" = t2."StoreId"
		AND t1."Channel" = t2."Channel"
 ) 

SELECT
		t1."Channel",'country',t1."Region",'area','city',t1."StoreId",t1."Store",'season',t1."Department",t1."Family",t1."SubFamily",t1."Category",
		t1."DOM_COMM",t1."SubCategory",t1."ExtendedSubCategory",t1."Supplier",t1."SubCategorySupplier",
		t1."AssemblyCodeNickName",t1."Status",t1."ENDOFLife",t1."Description",t1."ItemLookupCode",t1."HistoricalYear",t1."BudgetYear",'Month','Week',t1."BDate",
        t1."BudgetAmount", 0 AS "Deficit", 0 AS "Revised Budget", t1."BudgetAmountTY",
		t1."Budget%",t1."BudgetVAct/Frcst Percentage"/100 AS "BudgetVAct/frcast",
		t1."BudgetVAct/Frcst Percentage",t1."BudgetVPY%",t1."BudgetVPPY%",
		t1."UnitsBuyBySku",t1."TotalSkuCount",
		t1."prev_netsale_amount/NetSalesActual-LY",t1."p_prev_netsale_amount/NetSalesActual-LLY",
		t1."netsale_amount",t1."NetSalesActual-TY",t1."SoldDate",t1."NetSoldQuantityActuals-TY",t1."NetSoldCostActuals-TY",t1."CostOfGoodsLY/NSOLDCOST-LY",t1."NtSoldQty-LY",
        t1."CostOfGoodsLLY/NSOLDCOST-LLY",t1."NetSoldQuantity-PPY",t1."StockOnHandQty", t1."UnitsBuyBySkuLY",
		t1."Sales Actual/Forecast TY" , t1."budget_amountLy", t1."Sales Actual/Forecast LY", t1."Sales Actual/Forecast LLY", t1."BudgetGrossMarginTY%"
		FROM HIstbudandcur t1
		LEFT JOIN salestable_hist t2
		ON t1."ItemLookupCode" = t2."Item_Code"
		AND t1."StoreId" = t2."TRX_Store_Id"
		AND t1."Channel" = t2."Channel"
	"""
    # "Channel",'country',"Region",'area','city',"StoreId","Store",'season',"Department","Family","SubFamily","Category",
    # "DOM_COMM","Sub-Category","Extended-Sub-Category","Supplier","Sub-Category-Supplier",
    # "Assembly code-Nickname","Status","END OF Life","Description","ItemLookupCode","HistoricalYear","BudgetYear",'Month','Week',"BDate",
    # "Budget Amount", "Deficit", "Budget Cost", "Budget%","BudgetVAct/frcast", "BudgetVAct/Frcst Percentage","BudgetVPY%","BudgetVPPY%",
    # "Units Buy By SKU","TotalSkuCount", "Net Sales LY","PPrev netsale amount",
    # "Netsale amount","Sales Actuals","SoldDate","Quantity Actuals","Cost Actuals","Cost Of Goods LY","NtSoldQty-LY",
    # "Cost Of Goods LLY","Quantity PPY","Stock OnHand Qty", "Units Buy By SKU LY", "Sales Actual/Forecast TY", "Sales Actual/Forecast LLY"
    return query

def get_max_session_id(email_address,module_id):
  email_address = email_address.replace('"',"'")
  module_id = module_id.replace('"',"'")

  query = f"""  SELECT "session_id","table_name" FROM users_tables
WHERE "session_id" IN (SELECT COALESCE(MAX("session_id"),0) FROM users_tables WHERE "email_address" 
  = '{email_address}' AND module_id = '{module_id}') """
  return query


def get_table_name(email_address,module_id):
  email_address = email_address.replace('"',"'")
  module_id = module_id.replace('"',"'")

  query = f"""  SELECT "table_name","session_id" FROM users_tables
    WHERE "email_address" 
  = '{email_address}' AND "module_id" = '{module_id}' """
  return query






