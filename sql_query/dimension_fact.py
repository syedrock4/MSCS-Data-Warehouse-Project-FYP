
truncate_processing_zone_Dim_Customer = """
truncate table processing_zone.Dim_Customer
"""

insert_processing_zone_Dim_Customer = """
insert into processing_zone.Dim_Customer
select  sc.*,ca.customercategoryname from  scm_raw_zone.customers sc 
left join scm_raw_zone.customercategories ca on sc.customercategoryid = ca.customercategoryid

"""




truncate_processing_zone_Dim_Location = """
truncate table processing_zone.Dim_Location
"""

insert_processing_zone_Dim_Location = """
insert into processing_zone.Dim_Location
select  c.cityname,c.stateprovinceid,sp.stateprovincename,sp.salesterritory,cn.countryname,cn.formalname,cn.countrytype,
cn.continent ,cn.region ,cn.subregion ,c.eod_date
from   scm_raw_zone.cities c 
 join scm_raw_zone.stateprovinces sp  on c.stateprovinceid  = sp.stateprovinceid 
 join scm_raw_zone.countries cn  on cn.countryid  = sp.countryid 

"""




truncate_processing_zone_Dim_Product = """
truncate table processing_zone.Dim_Product
"""

insert_processing_zone_Dim_Product = """
insert into processing_zone.Dim_Product
select stockitemid,stockitemname,taxrate,unitprice,marketingcomments,eod_date from scm_raw_zone.stockitems 

"""




truncate_processing_zone_Dim_Datetime = """
truncate table processing_zone.Dim_Datetime
"""

insert_processing_zone_Dim_Datetime = """
insert into processing_zone.Dim_Datetime


WITH RECURSIVE date_generator(date_value) AS (
    SELECT
        '2023-07-01'::DATE
    UNION ALL
    SELECT
        (date_value + INTERVAL '1 day')::DATE
    FROM
        date_generator
    WHERE
        (date_value + INTERVAL '1 day')::DATE <= '2025-04-25'::DATE
),
date_details AS (
    SELECT
        date_value,
        EXTRACT(YEAR FROM date_value) AS year,
        CEIL(EXTRACT(MONTH FROM date_value) / 3.0) AS quarter,
        EXTRACT(MONTH FROM date_value) AS month,
        EXTRACT(DAY FROM date_value) AS day,
        EXTRACT(DOW FROM date_value) AS dayofweek,
        EXTRACT(WEEK FROM date_value) AS weekofyear,
        CASE
            WHEN date_value = DATE '2023-07-01' OR date_value = DATE '2025-04-25' THEN TRUE
            ELSE FALSE
        END AS isholiday
    FROM
        date_generator
),
data_to_insert AS (
    SELECT
        year * 10000 + month * 100 + day AS timekey,
        date_value AS date,
        year,
        quarter,
        month,
        day,
        dayofweek,
        weekofyear,
        isholiday
    FROM
        date_details
)


SELECT
    timekey,
    date,
    year,
    quarter,
    month,
    day,
    dayofweek,
    weekofyear,
    isholiday
FROM
    data_to_insert
  
"""








truncate_processing_zone_dim_Supplier = """
truncate table processing_zone.dim_Supplier
"""

insert_processing_zone_dim_Supplier = """
insert into processing_zone.dim_Supplier
select sc.supplierid,sc.suppliername,sc.supplierreference,sc.paymentdays,sc.phonenumber,sc.eod_date  from scm_raw_zone.suppliers sc

"""



truncate_processing_zone_dim_transaction_type = """
truncate table processing_zone.dim_transaction_type
"""

insert_processing_zone_dim_transaction_type = """
insert into processing_zone.dim_transaction_type
select sc.transactiontypeid,sc.transactiontypename,sc.eod_date  from scm_raw_zone.transactiontypes sc

"""





truncate_processing_zone_dim_payment_method = """
truncate table processing_zone.dim_payment_method
"""

insert_processing_zone_dim_payment_method = """
insert into processing_zone.dim_payment_method
select sc.paymentmethodid, sc.paymentmethodname,sc.eod_date from scm_raw_zone.paymentmethods sc

"""





truncate_processing_zone_Fact_Sales = """
truncate table processing_zone.Fact_Sales
"""

insert_processing_zone_Fact_Sales = """
insert into processing_zone.Fact_Sales
select  
    o.OrderID,
    c.CustomerID AS CustomerKey,
    ol.StockItemID AS ProductKey,
    o.SalespersonPersonID AS SalespersonKey,
    sp.StateProvinceID AS TerritoryKey,
    COUNT(ol.OrderLineID) AS OrderCount,
    SUM(ol.UnitPrice * ol.Quantity) AS TotalAmount,
    SUM(ol.TaxRate * ol.Quantity) AS TotalTax,
    SUM((ol.UnitPrice - ol.TaxRate) * ol.Quantity) AS TotalDiscount,
    SUM(ol.Quantity) AS TotalQuantity,
    SUM((ol.UnitPrice - ol.TaxRate - ol.Quantity) * ol.Quantity) AS TotalProfit,
    AVG(ol.UnitPrice) AS AverageUnitPrice,
    o.eod_date 
FROM scm_raw_zone.Orders o
left JOIN scm_raw_zone.OrderLines ol ON o.OrderID = ol.OrderID
left JOIN processing_zone.dim_customer  c ON o.CustomerID = c.CustomerID
left JOIN processing_zone.dim_location  sp ON sp.StateProvinceID = o.PickedByPersonID
left join processing_zone.dim_product dp on dp.stockitemid   = ol.StockItemID
GROUP BY  o.OrderID,c.CustomerID, ol.StockItemID, o.SalespersonPersonID, sp.StateProvinceID,o.eod_date 

"""







truncate_processing_zone_FactSupplier_Transactions = """
truncate table processing_zone.FactSupplier_Transactions
"""

insert_processing_zone_FactSupplier_Transactions = """
insert into processing_zone.FactSupplier_Transactions
SELECT 
    st.suppliertransactionid,
    st.SupplierID AS SupplierKey,
    st.PaymentMethodID AS PaymentMethodKey,
    st.TransactionTypeID AS TransactionTypeKey,
    st.TransactionAmount,
    st.TaxAmount,
    st.OutstandingBalance,
    st.amountexcludingtax,
    st.IsFinalized,
    st.TransactionDate,
    CASE WHEN DATEDIFF(day, st.TransactionDate, st.FinalizationDate) > 30 THEN 1 ELSE 0 END AS IsLatePayment,
    CASE WHEN st.IsFinalized = 1 THEN 'Settled' ELSE 'Pending' END AS SettlementStatus,
    st.eod_date
FROM scm_raw_zone.SupplierTransactions st 
left join processing_zone.dim_transaction_type dtt  on st.transactiontypeid  = dtt.transactiontypeid 
left join processing_zone.dim_payment_method dpm  on st.paymentmethodid  = dpm.paymentmethodid 
left join processing_zone.dim_supplier ds on st.supplierid  = ds.supplierid 

"""









