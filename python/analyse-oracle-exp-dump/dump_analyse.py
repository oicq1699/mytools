

from fileinput import filename
import io
import os
import sys

# parent_dir=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# sys.path.append(parent_dir)


import getopt
import enum
from typing import Any
import logging
from logging.handlers import RotatingFileHandler
from typing import List, Tuple, Dict

from common.byteutil import ByteUtil
from common.properties import Properties
from common.result import  Result


# Create a formatter.
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

rootLogger = logging.getLogger("")
rootLogger.setLevel(logging.INFO)
# Create the rotating file handler. Limit the size to 1000000Bytes ~ 1MB .
rootLoggerHandler = RotatingFileHandler("dump-analyse.log", mode='a', maxBytes=10000000, encoding=None, delay=0)
rootLoggerHandler.setLevel(logging.INFO)

# Add handler and formatter.
rootLoggerHandler.setFormatter(formatter)
rootLogger.addHandler(rootLoggerHandler)


insertLogger = logging.getLogger("InsertLogger")
insertLogger.setLevel(logging.INFO)
# Create the rotating file handler. Limit the size to 1000000Bytes ~ 1MB .
insertLoggerHandler = RotatingFileHandler("dump-analyse-insertsql.log", mode='a', maxBytes=10000000, encoding=None, delay=0)
insertLoggerHandler.setLevel(logging.INFO)

# Add handler and formatter.
insertLoggerHandler.setFormatter(formatter)
insertLogger.addHandler(insertLoggerHandler)



# logging.basicConfig(level=logging.DEBUG #设置日志输出格式
#                     ,filename="dump-analyse.log" #log日志输出的文件位置和文件名
#                     ,filemode="w" #文件的写入格式，w为重新写入文件，默认是追加
#                     ,format="%(asctime)s - %(name)s - %(levelname)-9s - %(filename)-8s : %(lineno)s line - %(message)s" #日志输出的格式
#                     # -8表示占位符，让输出左对齐，输出长度都为8位
#                     ,datefmt="%Y-%m-%d %H:%M:%S" #时间输出的格式
#                     )






########## 常量 #########################

DEFAULT_CHARSET="ascii"
oracleCharsetCodeMapDict={
    b'\x54\x03':"gbk",
    b'\x69\x03':"utf-8",
    b'\x01\x00':"ascii"
    
}
 
############# 类定义 ######################

 



class InsertSqlSegment:
    sql:str
    startidx:int



class OracleField:
    type:str=None
    #这是定义长度，实际数据不一定会有这么长
    defineLen:int=0
    charset:str=None
   
    
    def readMetaInfo(self,f:io.BufferedReader)->bool:
        return False
    
    def readData(self,f:io.BufferedReader)->str:
        return None

class OracleVarchar2Field(OracleField):
    def readMetaInfo(self, f: io.BufferedReader)->bool:
        self.type="varchar2"
        self.defineLen= ByteUtil.littleOrderBytes2UnsignedInt(f.read(2))
        #编码
        self.charset= oracleCharsetCodeMapDict.get(f.read(2),DEFAULT_CHARSET)
        #这2个字节不知道是啥
        f.read(2)
        return True
    
    def readData(self, f: io.BufferedReader)->str: 
        bsize= f.read(2)
        #null
        if(bsize==b'\xfe\xff'):
            return 'null'
             
        
        size= ByteUtil.littleOrderBytes2Int(bsize)
        if size>self.defineLen:
            print("读取数据错误,实际长度大于定义长度, file offset=",hex(f.tell()))
            return None   
                
        return  "'"+f.read(size).decode(self.charset, 'ignore')+"'"
         
        
class OracleCharField(OracleField):
    def readMetaInfo(self, f: io.BufferedReader)->bool:
        self.type="varchar2"
        self.defineLen= ByteUtil.littleOrderBytes2UnsignedInt(f.read(2))
        #编码
        self.charset= oracleCharsetCodeMapDict.get(f.read(2),DEFAULT_CHARSET)
        #这2个字节不知道是啥
        f.read(2)
        return True
    
    def readData(self, f: io.BufferedReader)->str:
        bsize= f.read(2)
        #null
        if(bsize==b'\xfe\xff'):
            return 'null'
             
        
        size= ByteUtil.littleOrderBytes2Int(bsize)
        
        if size>self.defineLen:
            print("读取数据错误,实际长度大于定义长度, file offset=",hex(f.tell()))
            return None    
        
        return  "'"+f.read(size).decode(self.charset, 'ignore')+"'"

class OracleNumberField(OracleField):
    def readMetaInfo(self, f: io.BufferedReader)->bool:
        self.type="number"
        self.defineLen= ByteUtil.littleOrderBytes2UnsignedInt(f.read(2))
        return True

    def readData(self, f: io.BufferedReader)->str:
        bsize= f.read(2)
        #data is null
        if(bsize==b'\xfe\xff'):
            return 'null'
             
        
        size= ByteUtil.littleOrderBytes2Int(bsize)
        
        #data is 0
        if size==1 :
            if f.read(1)==b'\x80':
                return '0' 
                
            else:
                print("data read error,file offset=",hex(f.tell()))
                return None
        
        if size>self.defineLen:
            print("读取数据错误,实际长度大于定义长度, file offset=",hex(f.tell()))
            return None            
       
        high=f.read(1)[0]
         #data > 0   
        if high> 0x80:
            bdata=f.read(size-1)
            idata=0
            power=high-0xc1
            for d in bdata:
                idata+=(d-1)*(100**power)
                power=power-1
            
            return str(idata)
        else:
            #data < 0   
            bdata=f.read(size-2)
            signed=f.read(1)
            
            idata=0
            power=0x3e-high
            for d in bdata:
                idata+=(d-0x65)*(100**(power))
                power=power-1
            return str(idata)            
         

class OracleDateField(OracleField):
    def readMetaInfo(self, f: io.BufferedReader)->bool:
        self.type="date"
        self.defineLen= ByteUtil.littleOrderBytes2UnsignedInt(f.read(2))
        return True

    def readData(self, f: io.BufferedReader)->str:
        bsize= f.read(2)
        #data is null
        if(bsize==b'\xfe\xff'):
            return 'null'
            
        
        size= ByteUtil.littleOrderBytes2Int(bsize)

        # format error
        if size!=7:
            print("date 长度不正确,预期7,实际:",size,",file offset:",hex(f.tell()))
            return None

        data="to_date('"
        # century
        data+=str((f.read(1)[0]-100))
        # year
        data+="{:0>2d}".format(f.read(1)[0]-100)    
        # month
        data+="-{:0>2d}".format(f.read(1)[0])  
        # day
        data+="-{:0>2d}".format(f.read(1)[0])  
        #hour
        data+=" {:0>2d}:".format(f.read(1)[0]-1)  
        #minute
        data+="{:0>2d}:".format(f.read(1)[0]-1)  
        #second
        data+="{:0>2d}".format(f.read(1)[0]-1)  
        
        data+="', 'yyyy-MM-dd HH24:MI:ss')"
        return data
                        
    

class OracleTimestampField(OracleField):
    def readMetaInfo(self, f: io.BufferedReader)->bool:
        self.type="timestamp"
        self.defineLen= ByteUtil.littleOrderBytes2UnsignedInt(f.read(2))
        return True

    def readData(self, f: io.BufferedReader)->str:
        bsize= f.read(2)
        #data is null
        if(bsize==b'\xfe\xff'):
            return 'null'
          
        
        size= ByteUtil.littleOrderBytes2Int(bsize)

        # format error
        if size!=7 and size!=11:
            print("date 长度不正确,预期7,实际:",size,",file offset:",hex(f.tell()))
            return None

        data="to_timestamp('"
        # century

        data+=str((f.read(1)[0]-100))
        # year
        data+="{:0>2d}".format(f.read(1)[0]-100)    
        # month
        data+="-{:0>2d}".format(f.read(1)[0])  
        # day
        data+="-{:0>2d}".format(f.read(1)[0])  
        #hour
        data+=" {:0>2d}:".format(f.read(1)[0]-1)  
        #minute
        data+="{:0>2d}:".format(f.read(1)[0]-1)  
        #second
        data+="{:0>2d}".format(f.read(1)[0]-1)  
        
        if size==7:
            data+="', 'yyyy-MM-dd HH24:MI:ss')"
            return data
        data+="."
        data+=str(ByteUtil.bigOrderBytes2UnsignedInt(f.read(4)))[0:3]   
        data+="', 'yyyy-MM-dd HH24:MI:ss.ff')"
        
        return data
 

 


        
FIELD_TYPES={
    b'\x01\x00':OracleVarchar2Field,
    b'\x02\x00':OracleNumberField,
    b'\x0c\x00':OracleDateField,
    b'\xb4\x00':OracleTimestampField,
    b'\x60\x00':OracleCharField
    
}


def getInsertSql(f:io.BufferedReader,charsetName:str)->Result:
    startidx=f.tell()
    exitNum=0
    ret=ByteUtil.readBytes(f, b'\x0a',2048)
    while ret.isSuccess():
        insertSql=bytes(ret.data).decode(charsetName, 'ignore');
        #print(insertSql)
        if insertSql.strip().startswith("INSERT INTO"):
            break
        startidx=f.tell()
        ret=ByteUtil.readBytes(f, b'\x0a')
    
    if ret.isError():
        print("获取 insert sql 语句时发生错误(可能已经没有更多的insert sql了)：",ret.msg,",file start index=",hex(startidx))
        return Result.errorResult(data=ret.data ,msg=ret.msg)
    if exitNum==2:
        return Result.errorResult(msg="读取到EXIT指定，文件已结束。")
    
    sqlseg= InsertSqlSegment()
    sqlseg.sql=insertSql
    sqlseg.startidx=startidx
    return Result.successResult(data=sqlseg);
        
        

def readFieldInfo(f: io.BufferedReader)->OracleField:
    fieldTypeBytes=f.read(2);
    field=FIELD_TYPES.get(fieldTypeBytes,OracleField)()
    success=field.readMetaInfo(f);
    if success:
        return field
    else:
        return None
    
    
def readFieldTypes(f:io.BufferedReader,printDetail:bool=False)->Result:
    colCount= ByteUtil.littleOrderBytes2UnsignedInt(f.read(2))
    if printDetail==True:
        print("字段总数：",colCount)
    tableFields=[]
    for i in range(colCount):
        theFieldInfo=readFieldInfo(f)
        if(theFieldInfo==None):
            return Result.errorResult(msg="field "+str(i)+" type unknown,exit. file offset="+hex(f.tell()))

        if printDetail==True:        
            print("field ",i,"type:", theFieldInfo.type,",len:",theFieldInfo.defineLen,",charset:",theFieldInfo.charset)
        tableFields.append(theFieldInfo);

    #字段定义与记录之间的四字节0的分隔符
    spearate=  f.read(4)
    if spearate!=b'\x00\x00\x00\x00':
        print("预期的字段定义与数据值中间的分隔符未出现，程序退出。file offset=",hex(f.tell()))
        return Result.errorResult(msg="预期的字段定义与数据值中间的分隔符未出现，程序退出。file offset="+hex(f.tell()))
    
    return Result.successResult(data=tableFields)    

       
def readFieldsData(f:io.BufferedReader,fieldtypes:List[OracleField],sql:str,printDetail:bool=False,outfile:io.TextIOWrapper=None)->bool:
    global fileStartIdx
    recCount=1;
    
    #没有记录的情况
    blen=f.read(2)
    fileStartIdx = f.tell();
    if blen==b'\xff\xff':
        print("0条记录")
        return True
    else:
        f.seek(f.tell()-2)
        fileStartIdx = f.tell();
    
    
    while True:
        # print("\b"*100,end="")
        #print("\r读取第",recCount,"条记录:",end="") 
        if printDetail==True:
            print("读取第",recCount,"条记录:",hex(f.tell()))
        else:
           # print("\b"*2000,end="")
            print("\r读取第",recCount,"条记录(",hex(f.tell()),"):",end="")
            rootLogger.info("读取第"+str(recCount)+"条记录("+hex(f.tell())+"):")
        currSql=sql        
        fieldIdx=1;    
        for ft in fieldtypes:
            fileStartIdx = f.tell();
            if(printDetail==False):
                #print(" >",fieldIdx,"(",hex(fileStartIdx),")",end="")
                rootLogger.info(" >>>"+str(fieldIdx)+"("+hex(fileStartIdx)+")")                  
            fieldValue=ft.readData(f)
            currSql=currSql.replace(":"+str(fieldIdx),fieldValue,1)
           
            fieldIdx+=1
            #print(ft.readData(f),"\t")
        
        if printDetail==True:
            print("")
            print(currSql)

 
        if outfile!=None:
            outfile.write(currSql)
            
        
        fileStartIdx = f.tell();
        blen=f.read(2)
        #判断是否结束
        if blen==b'\x00\x00':
            fileStartIdx = f.tell();
            blen=f.read(2)
            if blen==b'\xff\xff':
                return True
            else:
                f.seek(f.tell()-2)
                fileStartIdx = f.tell();
        else:
            print("")
            print("没有记录中止标记，file offset=",hex(f.tell()))  
            return False
        
        recCount+=1
                          
        



#--------------------------------
global fileStartIdx
fileStartIdx =0

def main():

    
    global fileStartIdx
    
    rootLogger.info("开始分析文件")
    
   
    print("从",os.path.abspath("app.properties"),"中读取配置...")  
    if os.path.exists(os.path.abspath("app.properties"))==False:
        print("请先设置[",os.path.abspath("app.properties"),"] 文件")
        exit(1)
        
    pros= Properties(os.path.abspath("app.properties"))
    fileName:str=pros.get("dump-file",None)
    if(fileName==None):
        print("dump-file not set")
        return 
    
    print("dump-file=",fileName)
    
    rowIndex:int=int(pros.get("row-start-index","0"),16)
    print("rowIndex=",rowIndex)
    
    insertSqlIndex:int = int(pros.get("insert-sql-index","0"),16)
    print("insertSqlIndex=",insertSqlIndex)
    
    printDetail:bool=pros.get("debug",False)
    print("debug=",printDetail)
    
    
    
    outfileName=pros.get("out-file",None)
    print("out-file=",outfileName)
    outfile=None
    if outfileName!=None:
        outfile:io.TextIOWrapper=open(outfileName,'w')
    
    
    
    # options, args = getopt.getopt(sys.argv[1:], "-d-o:-i:-r:", longopts=['debug','outfile=','insertsqlidx=','rowidx=',])
    # if len(args)>0:
    #     fileName=args[0]
    # else:
        #fileName= "20220216-1105.dmp"
        # fileName= "house.dmp"

    # for opt_name,opt_value in options:
    #     if opt_name in ('-d','--debug'):
    #         printDetail =True
    #         continue
    #     if opt_name in ('-r','--rowidx'):
    #         rowIndex=int(opt_value,16)
    #         continue
    #     if opt_name in ('-i','--insertsqlidx'):
    #         insertSqlIndex=int(opt_value,16)
    #         continue
             
    #     if opt_name in ('-o','--outfile'):
    #         print("outfilename=",opt_value)
    #         outfile = open(opt_value,'w')
    if os.path.exists(fileName)==False:
        print("dump 文件",fileName,"不存在")
        exit(1)
        
    with open(fileName, "rb") as f:
        
        try:
            
            a = f.read(1);

            if (a[0] != 0x03):
                print("第一个字节不是预期值，文件可能不是dump文件。：", a)
            fileStartIdx = f.tell();

            thecharsetCode=f.read(2)
            fileStartIdx = f.tell();
            
            currentCharsetName=oracleCharsetCodeMapDict.get(thecharsetCode[::-1]);
            print("dump 文件字符集:",currentCharsetName)

            ret: Result = None
            for num in range(4):
                ret = ByteUtil.readBytes(f, b'\x0a')
                if (ret.isError()):
                    print(ret.msg)
                    return
                print((bytes(ret.data[0:-1]).decode(currentCharsetName, 'ignore')))
            entityOffset=bytes(ret.data).decode(currentCharsetName, 'ignore');
            
            print("获取到dump数据进入点位置:",entityOffset)
            f.seek(int(entityOffset));
            fileStartIdx = f.tell();
            #读第一段带长度字节 到 +00：00        
            nextLen=  ByteUtil.littleOrderBytes2UnsignedInt(f.read(2));
            while(nextLen>0):
                f.read(nextLen);
                fileStartIdx = f.tell();
                nextLen=  ByteUtil.littleOrderBytes2UnsignedInt(f.read(2));

            #读第二段带长度字节到 DISABLE:ALL
            nextLen= ByteUtil.littleOrderBytes2UnsignedInt(f.read(2));
            while(nextLen>0):
                f.read(nextLen);
                fileStartIdx = f.tell();
                nextLen=  ByteUtil.littleOrderBytes2UnsignedInt(f.read(2));
            
            if insertSqlIndex>0  :
                f.seek(insertSqlIndex)
                fileStartIdx = f.tell();
                print("insertSqlIndex=",hex(insertSqlIndex))

            # 接下来是一段找不到长度定义的字节了,直接强行读到insert算了
            ret=getInsertSql(f,currentCharsetName)
            
            while ret.isSuccess():
                sdata=ret.data
                print("--------------------------------")
                print("insert sql start index:",hex(sdata.startidx))
                print(sdata.sql)
                
                rootLogger.info("-----------------------------------")
                insertLogger.info("(offset="+hex(sdata.startidx)+") "+sdata.sql)
 
                
                ft_ret=readFieldTypes(f)
                fileStartIdx = f.tell();
                
                if(ft_ret.isError()):
                    print(ft_ret.msg)
                    return
                tableFields=ft_ret.data
                if rowIndex>0:
                    f.seek(rowIndex)
                    print("insertSqlIndex=",hex(rowIndex))
                readFieldsData(f,tableFields,sql=sdata.sql, printDetail=printDetail,outfile=outfile)
                print("");
                insertSqlIndex=0
                ret=getInsertSql(f,currentCharsetName)
            

            if outfile!=None:
                outfile.close()
                
        except Exception as e:
            print("catch Exception: fileStart offset=",hex(fileStartIdx),",file offset=",hex(f.tell()))   
            rootLogger.error("catch Exception: fileStart offset="+hex(fileStartIdx),",file end offset=",hex(f.tell()))
            if outfile!=None:
                outfile.close()
            raise e
        
            
        
        
        

        
        
        

    # entity = f.read(4);
    # print(str(entity))




 
 

if __name__ == '__main__':
    main()
    # pass  什么都不干时，用pass，例如只有if语句，后面什么事也没干，这时不加pass就要报错
