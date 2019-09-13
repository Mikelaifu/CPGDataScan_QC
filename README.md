# CPGDataScan_QC

Apply Python Program to Scan and Qc CPG/Retail Structured Data

What?

It's a standard QC tool for Data Extract, to ensure the file structure (Raw file we receives extract team/Final file we deliver to client) are consistent from update to update

How?

System Requirement:
* Linux
* Python version 2.6.5 and Up

Files Requirement:
* At less 1 Fact/Data, Time Meta, Geog Meta and Product Meta.

Functionality?

1. Header Check   
2. Meta File Duplicate/Uniqueness Check 
3. Key Regular Expressions Check
4. Fact File Check:
* All records has the same number of column count 
* All key exist in Meta 
* No duplicate record (issue related to Model)
5. All Fact file Duplicate Check (issue related to SS)
6. No duplicate records for all fact files combine.
