# MSCS-Data-Warehouse-Project-FYP
# Instruction
* First Step: We need to setup AWS account for this we need to create AWS account and we are going to use on S3 Datalake and Redshift services in AWS.
* Second Step : In second step we need to setup Apache Airflow in Local PC, we are going to use airlfow for writing ETL pipeline and orchestration.
For Apache Airflow : https://github.com/aws/aws-mwaa-local-runner
We can access airlfow using : http://localhost:8080
* Third Step : In this step we need to downlaod MSCS-Data-Warehouse-Project-FYP repository in Dag folder
Once all these setup done we are reading to utilize this project.


Architecture of Data Warehouse and Its Logic Data Lake: AWS S3 Data Warehouse: Redshift ETL Tool: Airflow Dimension: SCD Type 1 Fact: Fact_inventory_transaction and Fact_orderdetails Raw zone: full dump plus incrementally Logic: In first time we will load the full dump data into warehouse then we will load or add incrementally data logic (insert and update).

# Power BI Dashboard
![image](https://github.com/user-attachments/assets/a23fbe1f-d4e0-4f1f-81b8-783e49ecc9b9)
