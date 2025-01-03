# MSCS-Data-Warehouse-Project-FYP
# Instruction
* First Step: We need to setup AWS account for this we need to create AWS account and we are going to use on S3 Datalake and Redshift services in AWS.
* Second Step : In second step we need to setup Apache Airflow in Local PC, we are going to use airlfow for writing ETL pipeline and orchestration.
For Apache Airflow : https://github.com/aws/aws-mwaa-local-runner
We can access airlfow using : http://localhost:8080
* Third Step : In this step we need to downlaod MSCS-Data-Warehouse-Project-FYP repository in Dag folder
  
Once all these setup done we are reading to utilize this project.

# Loom Video Link:
https://www.loom.com/share/b62f5bafc48a4ac385ae2b4c1ca55645?sid=f077faf0-d401-4f34-83dd-7220947f5564

Architecture of Data Warehouse and Its Logic, Data Lake: AWS S3, Data Warehouse: Redshift, ETL Tool: Airflow ,Dimension: SCD Type 1 Fact: Fact and Fact_orderdetails Raw zone: full dump plus incrementally Logic: In first time we will load the full dump data into warehouse then we will load or add incrementally data logic (insert and update).

# Scalable Data Warehouse Development using AWS Redshift, Airflow, S3, and Power BI  Archtitecture
![image](https://github.com/user-attachments/assets/fc24a0a8-577a-4bd7-b8e0-69ed24c34168)

# Data Warehouse Model Start Schema
* Fact Sales Analysis
![image](https://github.com/user-attachments/assets/d10fa66d-b494-4fd8-87f8-6d17a53bb788)

* Fact Transaction Analysis
  ![image](https://github.com/user-attachments/assets/e4a8be2b-d51e-44e5-8977-232d99998250)

# Power BI Dashboard 

![image](https://github.com/user-attachments/assets/d089f849-c070-476f-bf1c-8d9421e1db2d)




