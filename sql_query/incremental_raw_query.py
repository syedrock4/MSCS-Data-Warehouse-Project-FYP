truncate_scm_raw_zone_cities = """
truncate table scm_raw_zone.cities
"""

insert_scm_raw_zone_cities = """

insert  into  scm_raw_zone.cities (cityid,
cityname,
stateprovinceid,eod_date)

with dwh as (
select 
cityid,
cityname,
stateprovinceid
from scm_raw_zone.cities c 
),
insert_upd as (

select 
a.cityid,
a.cityname,
a.stateprovinceid
from 
staging.cities a
left join dwh b  on a.cityid  = b.cityid 
where b.cityid  is null  or
a.cityname <> b.cityname or 
a.stateprovinceid <> b.stateprovinceid 

)

select a.*,CURRENT_DATE AS eod_date from (
select * from dwh
union  
select * from insert_upd
)a
 """





truncate_scm_raw_zone_countries = """
truncate table scm_raw_zone.countries
"""

insert_scm_raw_zone_countries = """

insert  into  scm_raw_zone.countries (
countryid,
countryname,
formalname,
countrytype,
continent,
region,
subregion,
eod_date

)


with dwh as (
select 
countryid,
countryname,
formalname,
countrytype,
continent,
region,
subregion
from scm_raw_zone.countries c 
),
insert_upd as (

select 
a.countryid,
a.countryname,
a.formalname,
a.countrytype,
a.continent,
a.region,
a.subregion
from 
staging.countries a
left join dwh b  on a.countryid  = b.countryid 
where b.countryid  is null  or

a.countryname <>  b.countryname or
a.formalname <>  b.formalname or
a.countrytype <>  b.countrytype or 
a.continent <>  b.continent or
a.region <>  b.region or
a.subregion <>  b.subregion

)

select a.*,CURRENT_DATE AS eod_date from (
select * from dwh
union  
select * from insert_upd
)a
 """



truncate_scm_raw_zone_customercategories = """
truncate table scm_raw_zone.customercategories
"""

insert_scm_raw_zone_customercategories = """


insert  into  scm_raw_zone.customercategories (
customercategoryid,
customercategoryname,
lasteditedby,
eod_date

)



with dwh as (
select 
customercategoryid,
customercategoryname,
lasteditedby
from scm_raw_zone.customercategories c 
),
insert_upd as (

select 
a.customercategoryid,
a.customercategoryname,
a.lasteditedby
from 
staging.customercategories a
left join dwh b  on a.customercategoryid  = b.customercategoryid 
where b.customercategoryid  is null  or

a.customercategoryname <> b.customercategoryname or 
a.lasteditedby <> b.lasteditedby 

)

select a.*,CURRENT_DATE AS eod_date from (
select * from dwh
union  
select * from insert_upd
)a
 """




truncate_scm_raw_zone_customers = """
truncate table scm_raw_zone.customers
"""

insert_scm_raw_zone_customers = """


insert  into  scm_raw_zone.customers (
customerid,
customername,
customercategoryid,
phonenumber,
faxnumber,
postaladdressline1,
isstatementsent,
isoncredithold,
paymentdays,
eod_date

)



with dwh as (
select 
customerid,
customername,
customercategoryid,
phonenumber,
faxnumber,
postaladdressline1,
isstatementsent,
isoncredithold,
paymentdays
from scm_raw_zone.customers c 
),
insert_upd as (

select 
a.customerid,
a.customername,
a.customercategoryid,
a.phonenumber,
a.faxnumber,
a.postaladdressline1,
a.isstatementsent,
a.isoncredithold,
a.paymentdays
from 
staging.customers a
left join dwh b  on a.customerid  = b.customerid 
where b.customerid  is null  or

a.customername  <>    b.customername or
a.customercategoryid  <>    b.customercategoryid or
a.phonenumber  <>    b.phonenumber or
a.faxnumber  <>    b.faxnumber or
a.postaladdressline1  <>    b.postaladdressline1 or
a.isstatementsent  <>    b.isstatementsent or
a.isoncredithold  <>    b.isoncredithold or
a.paymentdays  <>    b.paymentdays 

)

select a.*,CURRENT_DATE AS eod_date from (
select * from dwh
union  
select * from insert_upd
)a
 """




truncate_scm_raw_zone_orderlines = """
truncate table scm_raw_zone.orderlines
"""

insert_scm_raw_zone_orderlines = """


insert  into  scm_raw_zone.orderlines (
orderlineid,
orderid,
stockitemid,
description,
packagetypeid,
quantity,
unitprice,
taxrate,
pickedquantity,
pickingcompletedwhen,
lasteditedby,
lasteditedwhen,
eod_date

)



with dwh as (
select 
orderlineid,
orderid,
stockitemid,
description,
packagetypeid,
quantity,
unitprice,
taxrate,
pickedquantity,
pickingcompletedwhen,
lasteditedby,
lasteditedwhen
from scm_raw_zone.orderlines c 
),
insert_upd as (

select 
a.orderlineid,
a.orderid,
a.stockitemid,
a.description,
a.packagetypeid,
a.quantity,
a.unitprice,
a.taxrate,
a.pickedquantity,
a.pickingcompletedwhen,
a.lasteditedby,
a.lasteditedwhen
from 
staging.orderlines a
left join dwh b  on a.orderlineid  = b.orderlineid 
where b.orderlineid  is null  or

a.orderid  <>   b.orderid or 
a.stockitemid  <>   b.stockitemid or 
a.description  <>   b.description or 
a.packagetypeid  <>   b.packagetypeid or 
a.quantity  <>   b.quantity or 
a.unitprice  <>   b.unitprice or 
a.taxrate  <>   b.taxrate or 
a.pickedquantity  <>   b.pickedquantity or 
a.pickingcompletedwhen  <>   b.pickingcompletedwhen or 
a.lasteditedby  <>   b.lasteditedby or 
a.lasteditedwhen  <>   b.lasteditedwhen 

)

select a.*,CURRENT_DATE AS eod_date from (
select * from dwh
union  
select * from insert_upd
)a
 """




truncate_scm_raw_zone_orders = """
truncate table scm_raw_zone.orders
"""

insert_scm_raw_zone_orders = """

insert  into  scm_raw_zone.orders (
salespersonpersonid,
pickingcompletedwhen,
pickedbypersonid,
orderid,
orderdate,
lasteditedwhen,
lasteditedby,
isundersupplybackordered,
internalcomments,
expecteddeliverydate,
deliveryinstructions,
customerpurchaseordernumber,
customerid,
contactpersonid,
comments,eod_date)

with dwh as (
select 
salespersonpersonid,
pickingcompletedwhen,
pickedbypersonid,
orderid,
orderdate,
lasteditedwhen,
lasteditedby,
isundersupplybackordered,
internalcomments,
expecteddeliverydate,
deliveryinstructions,
customerpurchaseordernumber,
customerid,
contactpersonid,
comments
from scm_raw_zone.orders c 
),
insert_upd as (

select 
a.salespersonpersonid,
a.pickingcompletedwhen,
a.pickedbypersonid,
a.orderid,
a.orderdate,
a.lasteditedwhen,
a.lasteditedby,
a.isundersupplybackordered,
a.internalcomments,
a.expecteddeliverydate,
a.deliveryinstructions,
a.customerpurchaseordernumber,
a.customerid,
a.contactpersonid,
a.comments
from 
staging.orders a
left join dwh b  on a.salespersonpersonid  = b.salespersonpersonid 
where b.salespersonpersonid  is null  or

a.pickingcompletedwhen <> b.pickingcompletedwhen  or 
a.pickedbypersonid <> b.pickedbypersonid  or 
a.orderid <> b.orderid  or 
a.orderdate <> b.orderdate  or 
a.lasteditedwhen <> b.lasteditedwhen  or 
a.lasteditedby <> b.lasteditedby  or 
a.isundersupplybackordered <> b.isundersupplybackordered  or 
a.internalcomments <> b.internalcomments  or 
a.expecteddeliverydate <> b.expecteddeliverydate  or 
a.deliveryinstructions <> b.deliveryinstructions  or 
a.customerpurchaseordernumber <> b.customerpurchaseordernumber  or 
a.customerid <> b.customerid  or 
a.contactpersonid <> b.contactpersonid  or 
a.comments <> b.comments  

)

select a.*,CURRENT_DATE AS eod_date from (
select * from dwh
union  
select * from insert_upd
)a
 """





truncate_scm_raw_zone_paymentmethods = """
truncate table scm_raw_zone.paymentmethods
"""

insert_scm_raw_zone_paymentmethods = """

insert  into  scm_raw_zone.paymentmethods (
paymentmethodid,
paymentmethodname,
lasteditedby,eod_date)

with dwh as (
select 
paymentmethodid,
paymentmethodname,
lasteditedby
from scm_raw_zone.paymentmethods c 
),
insert_upd as (

select 
a.paymentmethodid,
a.paymentmethodname,
a.lasteditedby
from 
staging.paymentmethods a
left join dwh b  on a.paymentmethodid  = b.paymentmethodid 
where b.paymentmethodid  is null  or
a.paymentmethodname <>  b.paymentmethodname or 
a.lasteditedby <>  b.lasteditedby 

)

select a.*,CURRENT_DATE AS eod_date from (
select * from dwh
union  
select * from insert_upd
)a
 """






truncate_scm_raw_zone_people = """
truncate table scm_raw_zone.people
"""

insert_scm_raw_zone_people = """

insert  into  scm_raw_zone.people (
personid,
fullname,
isemployee,
issalesperson,
userpreferences,
phonenumber,
faxnumber,
emailaddress,eod_date)

with dwh as (
select 
personid,
fullname,
isemployee,
issalesperson,
userpreferences,
phonenumber,
faxnumber,
emailaddress
from scm_raw_zone.people c 
),
insert_upd as (

select 
a.personid,
a.fullname,
a.isemployee,
a.issalesperson,
a.userpreferences,
a.phonenumber,
a.faxnumber,
a.emailaddress
from 
staging.people a
left join dwh b  on a.personid  = b.personid 
where b.personid  is null  or

a.fullname <>  b.fullname  or 
a.isemployee <>  b.isemployee  or 
a.issalesperson <>  b.issalesperson  or 
a.userpreferences <>  b.userpreferences  or 
a.phonenumber <>  b.phonenumber  or 
a.faxnumber <>  b.faxnumber  or 
a.emailaddress <>  b.emailaddress  



)

select a.*,CURRENT_DATE AS eod_date from (
select * from dwh
union  
select * from insert_upd
)a
 """





truncate_scm_raw_zone_stateprovinces = """
truncate table scm_raw_zone.stateprovinces
"""

insert_scm_raw_zone_stateprovinces = """

insert  into  scm_raw_zone.stateprovinces (
stateprovinceid,
stateprovincecode,
stateprovincename,
countryid,
salesterritory,eod_date)

with dwh as (
select 
stateprovinceid,
stateprovincecode,
stateprovincename,
countryid,
salesterritory
from scm_raw_zone.stateprovinces c 
),
insert_upd as (

select 
a.stateprovinceid,
a.stateprovincecode,
a.stateprovincename,
a.countryid,
a.salesterritory
from 
staging.stateprovinces a
left join dwh b  on a.stateprovinceid  = b.stateprovinceid 
where b.stateprovinceid  is null  or

a.stateprovincecode <>  b.stateprovincecode or 
a.stateprovincename <>  b.stateprovincename or 
a.countryid <>  b.countryid or 
a.salesterritory <>  b.salesterritory 



)

select a.*,CURRENT_DATE AS eod_date from (
select * from dwh
union  
select * from insert_upd
)a
 """





truncate_scm_raw_zone_stockitems = """
truncate table scm_raw_zone.stockitems
"""

insert_scm_raw_zone_stockitems = """

insert  into  scm_raw_zone.stockitems (
stockitemid,
stockitemname,
supplierid,
unitpackageid,
outerpackageid,
taxrate,
unitprice,
recommendedretailprice,
typicalweightperunit,
marketingcomments,eod_date)

with dwh as (
select 
stockitemid,
stockitemname,
supplierid,
unitpackageid,
outerpackageid,
taxrate,
unitprice,
recommendedretailprice,
typicalweightperunit,
marketingcomments
from scm_raw_zone.stockitems c 
),
insert_upd as (

select 
a.stockitemid,
a.stockitemname,
a.supplierid,
a.unitpackageid,
a.outerpackageid,
a.taxrate,
a.unitprice,
a.recommendedretailprice,
a.typicalweightperunit,
a.marketingcomments
from 
staging.stockitems a
left join dwh b  on a.stockitemid  = b.stockitemid 
where b.stockitemid  is null  or
a.stockitemname <> b.stockitemname  or 
a.supplierid <> b.supplierid  or 
a.unitpackageid <> b.unitpackageid  or 
a.outerpackageid <> b.outerpackageid  or 
a.taxrate <> b.taxrate  or 
a.unitprice <> b.unitprice  or 
a.recommendedretailprice <> b.recommendedretailprice  or 
a.typicalweightperunit <> b.typicalweightperunit  or 
a.marketingcomments <> b.marketingcomments  



)

select a.*,CURRENT_DATE AS eod_date from (
select * from dwh
union  
select * from insert_upd
)a
 """






truncate_scm_raw_zone_suppliers = """
truncate table scm_raw_zone.suppliers
"""

insert_scm_raw_zone_suppliers = """

insert  into  scm_raw_zone.suppliers (
supplierid,
suppliername,
suppliercategoryid,
primarycontactpersonid,
alternatecontactpersonid,
deliverycityid,
postalcityid,
supplierreference,
paymentdays,
phonenumber,eod_date)

with dwh as (
select 
supplierid,
suppliername,
suppliercategoryid,
primarycontactpersonid,
alternatecontactpersonid,
deliverycityid,
postalcityid,
supplierreference,
paymentdays,
phonenumber
from scm_raw_zone.suppliers c 
),
insert_upd as (

select 
a.supplierid,
a.suppliername,
a.suppliercategoryid,
a.primarycontactpersonid,
a.alternatecontactpersonid,
a.deliverycityid,
a.postalcityid,
a.supplierreference,
a.paymentdays,
a.phonenumber
from 
staging.suppliers a
left join dwh b  on a.supplierid  = b.supplierid 
where b.supplierid  is null  or
a.suppliername <>  b.suppliername  or 
a.suppliercategoryid <>  b.suppliercategoryid  or 
a.primarycontactpersonid <>  b.primarycontactpersonid  or 
a.alternatecontactpersonid <>  b.alternatecontactpersonid  or 
a.deliverycityid <>  b.deliverycityid  or 
a.postalcityid <>  b.postalcityid  or 
a.supplierreference <>  b.supplierreference  or 
a.paymentdays <>  b.paymentdays  or 
a.phonenumber <>  b.phonenumber 


)

select a.*,CURRENT_DATE AS eod_date from (
select * from dwh
union  
select * from insert_upd
)a
 """



 

truncate_scm_raw_zone_suppliertransactions = """
truncate table scm_raw_zone.suppliertransactions
"""

insert_scm_raw_zone_suppliertransactions = """

insert  into  scm_raw_zone.suppliertransactions (
suppliertransactionid,
supplierid,
transactiontypeid,
paymentmethodid,
supplierinvoicenumber,
transactiondate,
amountexcludingtax,
taxamount,
transactionamount,
outstandingbalance,
finalizationdate,
isfinalized,eod_date)

with dwh as (
select 
suppliertransactionid,
supplierid,
transactiontypeid,
paymentmethodid,
supplierinvoicenumber,
transactiondate,
amountexcludingtax,
taxamount,
transactionamount,
outstandingbalance,
finalizationdate,
isfinalized
from scm_raw_zone.suppliertransactions c 
),
insert_upd as (

select 
a.suppliertransactionid,
a.supplierid,
a.transactiontypeid,
a.paymentmethodid,
a.supplierinvoicenumber,
a.transactiondate,
a.amountexcludingtax,
a.taxamount,
a.transactionamount,
a.outstandingbalance,
a.finalizationdate,
a.isfinalized
from 
staging.suppliertransactions a
left join dwh b  on a.suppliertransactionid  = b.suppliertransactionid 
where b.suppliertransactionid  is null  or
a.supplierid  <> b.supplierid   or
a.transactiontypeid  <> b.transactiontypeid   or
a.paymentmethodid  <> b.paymentmethodid   or
a.supplierinvoicenumber  <> b.supplierinvoicenumber   or
a.transactiondate  <> b.transactiondate   or
a.amountexcludingtax  <> b.amountexcludingtax   or
a.taxamount  <> b.taxamount   or
a.transactionamount  <> b.transactionamount   or
a.outstandingbalance  <> b.outstandingbalance   or
a.finalizationdate  <> b.finalizationdate   or
a.isfinalized  <> b.isfinalized   

)

select a.*,CURRENT_DATE AS eod_date from (
select * from dwh
union  
select * from insert_upd
)a
 """





truncate_scm_raw_zone_transactiontypes = """
truncate table scm_raw_zone.transactiontypes
"""

insert_scm_raw_zone_transactiontypes = """

insert  into  scm_raw_zone.transactiontypes (
transactiontypeid,
transactiontypename,
lasteditedby,eod_date)

with dwh as (
select 
transactiontypeid,
transactiontypename,
lasteditedby
from scm_raw_zone.transactiontypes c 
),
insert_upd as (

select 
a.transactiontypeid,
a.transactiontypename,
a.lasteditedby
from 
staging.transactiontypes a
left join dwh b  on a.transactiontypeid  = b.transactiontypeid 
where b.transactiontypeid  is null  or

a.transactiontypename <> b.transactiontypename or
a.lasteditedby <> b.lasteditedby 


)

select a.*,CURRENT_DATE AS eod_date from (
select * from dwh
union  
select * from insert_upd
)a
 """


