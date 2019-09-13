# By Mike Wu
import sys
from sys import argv
import glob
import os.path
import multiprocessing
from multiprocessing import Queue
import re
import datetime
import glob
import csv
import itertools
import time
from time import time
import subprocess
import shutil


#Header = "Yes"
#HeaderChk = "Yes"
#FactHeader = "HeaderPath/Header_Fact.txt"
#GeogHeader = "HeaderPath/Header_Geog.txt"
#ProdHeader = "HeaderPath/Header_Prod.txt"
#TimeHeader = "HeaderPath/Header_Time.txt"
#FactFile = "FactPath/*.txt"
#TimeKeyCol=2
#GeogKeyCol=1
#ProdKeyCol=0
#FactColumnCnt=16
#TimeMetaFile="TimemetaPath/*meta.txt"
#TimeMetaKeyCol=0
#TimeKeyReExp="\d"
#GeogMetaFile="GeogmetaPath/*geog_meta.txt"
#GeogMetaKeyCol=0
#GeogKeyReExp="\d"
#ProdMetaFile="ProdmetaPath/*product.txt*"
#ProdMetaKeyCol=2
#ProdKeyReExp="\d"
#Delimiter="|"
#RunSubFile = "Yes"
#PID = "123"
#ProjectPath = "/ILD_Extracts/irs-prod/Crossmark"
#RunStatusLog= "/ILD_Extracts/irs-prod/Crossmark/Status/Running.test.log"

Header = argv[1]
HeaderChk = argv[2]
FactHeader = argv[3]
GeogHeader = argv[4]
ProdHeader = argv[5]
TimeHeader = argv[6]
FactFile = argv[7]
TimeKeyCol = int(argv[8])
GeogKeyCol = int(argv[9])
ProdKeyCol = int(argv[10])
FactColumnCnt = int(argv[11])
TimeMetaFile = argv[12]
TimeMetaKeyCol = int(argv[13])
TimeKeyReExp = argv[14]
GeogMetaFile = argv[15]
GeogMetaKeyCol = int(argv[16])
GeogKeyReExp = argv[17]
ProdMetaFile = argv[18]
ProdMetaKeyCol = int(argv[19])
ProdKeyReExp = argv[20]
Delimiter = argv[21]
RunSubFile = argv[22]
PID = argv[23]
ProjectPath = argv[24]
RunStatusLog= argv[25]

TimeMetaKey = set()
GeogMetaKey = set()
ProdMetaKey = set()
m_Msg = Queue()
m_Status = Queue()

ProjectTmpPath = ProjectPath + "/Etc/tmp/" + PID
####  Function
def Get_1_Key(inFile, delimit, key):
    content = []
    with open(inFile, "r") as csv_file:
        reader = csv.reader(csv_file, delimiter=delimit)
        if Header == "Yes":  # Skip Header
            next(reader)
        for row in reader:
            content.append(row[key])
    return (content)


def Get_3_Key(inFile, delimit, key1, key2, key3):
    content = []
    with open(inFile, "r") as csv_file:
        reader = csv.reader(csv_file, delimiter=delimit)
        if Header == "Yes":  # Skip Header
            next(reader)
        for row in reader:
            content.append(row[key1] + Delimiter + row[key2] + Delimiter + row[key3])
    return (content)


def Get_Dup(mylist):
    myset = set()
    for x in mylist:
        if x not in myset:
            myset.add(x)
        else:
            return (x)
    return ("Error")


def Get_Invalid_Key(keyset, keyexp):
    p = re.compile(keyexp)
    for x in keyset:
        if not p.match(x):
            return (x)
    return "Pass"


def Uniq_chk_old(inFile):
    global Data_Scarn_Status
    global Uniq_chk_Er
    Uniq_chk_Er = ""
    content = open(inFile, 'r').readlines()
    content_set = set(content)
    if len(content) != len(content_set):
        Data_Scarn_Status = "Fail"
        content_set.clear()
        for i in content:
            if i not in content_set:
                content_set.add(i)
            else:
                Uniq_chk_Er = i
                return ("Fail")
    return ("Pass")

def Clear_tmp_dir():
    if os.path.exists(ProjectTmpPath ):
        shutil.rmtree(ProjectTmpPath)
    
def Create_Tmp_Dir(): 
    if not os.path.exists(ProjectPath + "/Etc/tmp" ):
        os.makedirs(ProjectPath + "/Etc/tmp")
    Clear_tmp_dir()
    if not os.path.exists(ProjectTmpPath ):
        os.makedirs(ProjectTmpPath)   

def printLog(my_string):
    with open(RunStatusLog,'a') as log_file:
        log_file.write(str(my_string) + "\n")
        print(my_string)       
        
################ Header Check Function ###################
def Compare_Header(RawFile, HeaderFile):
    with open(RawFile, "r") as rf:
        with open(HeaderFile, "r") as cf:
            if rf.readline() != cf.readline():
                return ("Fail")
            else:
                return ("Pass")


def Header_Chk():
    mystatus = "Pass"
    printLog("---------- Header Chk ----------")
    for RowFile in glob.iglob(FactFile):
        if Compare_Header(RowFile, FactHeader) == "Pass":
            printLog(RowFile + " ...Pass")
        else:
            printLog(RowFile + " ...Fail")
            mystatus = "Fail"
    for RowFile in glob.iglob(TimeMetaFile):
        if Compare_Header(RowFile, TimeHeader) == "Pass":
            printLog(RowFile + " ...Pass")
        else:
            printLog(RowFile + " ...Fail")
            mystatus = "Fail"
    for RowFile in glob.iglob(GeogMetaFile):
        if Compare_Header(RowFile, GeogHeader) == "Pass":
            printLog(RowFile + " ...Pass")
        else:
            printLog(RowFile + " ...Fail")
            mystatus = "Fail"
    for RowFile in glob.iglob(ProdMetaFile):
        if Compare_Header(RowFile, ProdHeader) == "Pass":
            printLog(RowFile + " ...Pass")
        else:
            printLog(RowFile + " ...Fail")
            mystatus = "Fail"
    if mystatus == "Pass":
        printLog("Header Chk Complete: Pass")
        return ("Pass")
    else:
        printLog("Header Chk Complete: Fail")
        return ("Fail")


################ Meta File Ck #######################
def Meta_File_Dup_Chk():
    global TimeMetaKey, GeogMetaKey, ProdMetaKey
    mystatus = "Pass"
    printLog("---------- Meta File Duplicate Chk ----------")
    key_list = []
    for RowFile in glob.iglob(GeogMetaFile):
        key_list.extend(Get_1_Key(RowFile, Delimiter, GeogMetaKeyCol))
    GeogMetaKey = set(key_list)  # Create Uniq list
    if len(key_list) != len(GeogMetaKey):
        printLog("Geog ...Fail")
        printLog(Get_Dup(key_list))
        mystatus = "Fail"
    else:
        printLog("Geog ...Pass")
    key_list = []
    for RowFile in glob.iglob(TimeMetaFile):
        key_list.extend(Get_1_Key(RowFile, Delimiter, TimeMetaKeyCol))
    TimeMetaKey = set(key_list)  # Create Uniq list
    if len(key_list) != len(TimeMetaKey):
        printLog("Time ...Fail")
        printLog(Get_Dup(key_list))
        mystatus = "Fail"
    else:
        printLog("Time ...Pass")
    key_list = []
    for RowFile in glob.iglob(ProdMetaFile):
        key_list.extend(Get_1_Key(RowFile, Delimiter, ProdMetaKeyCol))
    ProdMetaKey = set(key_list)  # Create Uniq list
    if len(key_list) != len(ProdMetaKey):
        printLog("Prod ...Fail")
        printLog(Get_Dup(key_list))
        mystatus = "Fail"
    else:
        printLog("Prod ...Pass")
    if mystatus == "Pass":
        printLog("Meta File Duplicate Chk Complete: Pass")
        return ("Pass")
    else:
        printLog("Meta File Duplicate Chk Complete: Fail")
        return ("Fail")


def Meta_Key_Exp_Chk():
    mystatus = "Pass"
    printLog("---------- Meta Key ReExp Chk ----------")
    invalid_key = Get_Invalid_Key(GeogMetaKey, GeogKeyReExp)
    if invalid_key != "Pass":
        printLog("Geog ...Fail")
        printLog(invalid_key)
        mystatus = "Fail"
    else:
        printLog("Geog ...Pass")
    invalid_key = Get_Invalid_Key(TimeMetaKey, TimeKeyReExp)
    if invalid_key != "Pass":
        printLog("Time ...Fail")
        printLog(invalid_key)
        mystatus = "Fail"
    else:
        printLog("Time ...Pass")
    invalid_key = Get_Invalid_Key(ProdMetaKey, ProdKeyReExp)
    if invalid_key != "Pass":
        printLog("Prod ...Fail")
        printLog(invalid_key)
        mystatus = "Fail"
    else:
        printLog("Prod ...Pass")
    if mystatus == "Pass":
        printLog("Meta Key ReExp Chk Complete: Pass")
        return ("Pass")
    else:
        printLog("Meta Key ReExp Chk Complete: Fail")
        return ("Fail")


################ Fact File Ck #######################
def Fact_Sub_File_Chk(inFile, qstatus, qmsg):
    geogconten = set()
    timeconten = set()
    prodconten = set()
    allcontens = set()
    cnt = 0
    t = time()
    base = os.path.basename(inFile)
    mymsg = base
    mystatus = "Pass"
    with open(inFile, "r") as inf:
        if RunSubFile == "Yes":
            if Header == "Yes":
                factdata = inf.readlines()[1:]  # Skip Header
            else:
                factdata = inf.readlines()
            for line in factdata:
                cnt = cnt + 1
                cols = line.split(Delimiter)
                geogconten.add(cols[GeogKeyCol])
                timeconten.add(cols[TimeKeyCol])
                prodconten.add(cols[ProdKeyCol])
                allcontens.add(cols[GeogKeyCol] + Delimiter + cols[TimeKeyCol] + Delimiter + cols[ProdKeyCol])
                if line.count(Delimiter) != FactColumnCnt: 
            	    mystatus = "Fail"
        else:
            if Header == "Yes":
                skipline = inf.readline()  # Skip Header
            for line in inf:
                cnt = cnt + 1
                cols = line.split(Delimiter)
                geogconten.add(cols[GeogKeyCol])
                timeconten.add(cols[TimeKeyCol])
                prodconten.add(cols[ProdKeyCol])
                allcontens.add(cols[GeogKeyCol] + Delimiter + cols[TimeKeyCol] + Delimiter + cols[ProdKeyCol])
                if line.count(Delimiter) != FactColumnCnt: 
            	    mystatus = "Fail"
        if mystatus == "Fail":
            mymsg = mymsg + " " + "Column Count -Fail, "
        else:
            mymsg = mymsg + " " + "Column Count -Pass, "  
        mymsg = mymsg + " " + "Dup Chk "
        if cnt != len(allcontens):
            mymsg = mymsg + "-" + "Fail, "
            mystatus = "Fail"
        else:
            mymsg = mymsg + " " + "Pass, "
        mymsg = mymsg + " " + "Geog in Meta Chk "
        if not geogconten.issubset(GeogMetaKey):
            mymsg = mymsg + "-" + "Fail, "
            mystatus = "Fail"
        else:
            mymsg = mymsg + "-" + "Pass, "
        mymsg = mymsg + " " + "Time in Meta Chk "
        if not timeconten.issubset(TimeMetaKey):
            mymsg = mymsg + "-" + "Fail, "
            mystatus = "Fail"
        else:
            mymsg = mymsg + "-" + "Pass, "
        mymsg = mymsg + " " + "Prod in Meta Chk "
        
        if not prodconten.issubset(ProdMetaKey):
            for x in prodconten:
            	if x not in ProdMetaKey:
            	     print(x)
            mymsg = mymsg + "-" + "Fail, "
            mystatus = "Fail"
        else:
            mymsg = mymsg + "-" + "Pass"
    printLog(ProjectTmpPath + "/" + base + " ..." + mystatus)
    f = open(ProjectTmpPath + "/" + base, 'w')
    for x in allcontens:
        f.write(x + "\n")
    if mystatus == "Fail":
        qmsg.put(mymsg)
    qstatus.put(mystatus)


def Fact_File_Chk():
    printLog("---------- Fact File Chk ----------")
    mproces = []
    m_status_list = []
    m_msg_list = []
    for f in glob.iglob(FactFile):
        proces = multiprocessing.Process(target=Fact_Sub_File_Chk, args=(f, m_Status, m_Msg))
        mproces.append(proces)
        proces.start()
    for m in mproces:
        m.join()
    while not m_Status.empty():
        m_status_list.append(m_Status.get())
    while not m_Msg.empty():
        m_msg_list.append(m_Msg.get())
    if "Fail" in m_status_list:
        printLog("\n".join(m_msg_list))
        printLog("Fact File Chk Complete: Fail")
        return ("Fail")
    else:
        printLog("Fact File Chk Complete: Pass")
        return ("Pass")


################ All Fact File Ck #######################
def All_Fact_File_Dup_Ck():
    printLog("---------- All Fact File Duplicate Chk ----------")
    mystatus = "Pass"
    x = 0
    myallkeydata = set()
    mycurentallkeydatalen = 0
    for f in glob.iglob(ProjectTmpPath + "/*"):
        base = os.path.basename(f)
        with open(f, "r") as inf:
            keydata = inf.readlines()
            if x == 0:
                myallkeydata = set(keydata)
                printLog(base + " ...Pass")
                x = x + 1
            else:
                mycurentallkeydatalen = len(myallkeydata) + len(keydata)
                myallkeydata.update(keydata)
                if mycurentallkeydatalen != len(myallkeydata):
                    printLog (base + " ...Fail")
                    mystatus = "Fail"
                else:
                    printLog (base + " ...Pass")
    myallkeydata.clear()
    keydata = []
    if mystatus == "Fail":
        printLog ("All Fact File Duplicate Chk Complete: Fail")
        return ("Fail")
    else:
        printLog ("All Fact File Duplicate Chk Complete: Pass")
        return ("Pass")


# ----- Program Start -----
if __name__ == "__main__":
    t = time()
    Create_Tmp_Dir()
    if HeaderChk == "Yes":
        if Header_Chk() == "Fail":
            printLog("Data Scan Result: Fail\n")
            printLog(time() - t)
            Clear_tmp_dir()
            sys.exit()
        printLog(time() - t)
    t = time()
    if Meta_File_Dup_Chk() == "Fail":
        printLog("Data Scan Result: Fail\n")
        printLog(time() - t)
        Clear_tmp_dir()
        sys.exit()
    printLog(time() - t)
    t = time()
    if Meta_Key_Exp_Chk() == "Fail":
        printLog("Data Scan Result: Fail\n")
        printLog(time() - t)
        Clear_tmp_dir()
        sys.exit()
    printLog(time() - t)
    t = time()
    if Fact_File_Chk() == "Fail":
        printLog("Data Scan Result: Fail\n")
        printLog(time() - t)
        Clear_tmp_dir()
        sys.exit()
    printLog(time() - t)
    t = time()
    if All_Fact_File_Dup_Ck() == "Fail\n":
    	printLog("Data Scan Result: Fail\n")
        printLog(time() - t)
        Clear_tmp_dir()
        sys.exit()
    printLog(time() - t)
    printLog("Data Scan Result: Pass\n")
    Clear_tmp_dir()
    sys.exit()
