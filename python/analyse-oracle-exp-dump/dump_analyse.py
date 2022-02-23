import io
import os
import sys

parent_dir=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)


import getopt
import enum
from typing import Any
import logging
from typing import List, Tuple, Dict

from common.byteutil import ByteUtil
from common.result import ResultCode, Result


logging.basicConfig(level=logging.DEBUG #设置日志输出格式
                    ,filename="analyse-dump.log" #log日志输出的文件位置和文件名
                    ,filemode="w" #文件的写入格式，w为重新写入文件，默认是追加
                    ,format="%(asctime)s - %(name)s - %(levelname)-9s - %(filename)-8s : %(lineno)s line - %(message)s" #日志输出的格式
                    # -8表示占位符，让输出左对齐，输出长度都为8位
                    ,datefmt="%Y-%m-%d %H:%M:%S" #时间输出的格式
                    )






########## 常量 #########################

DEFAULT_CHARSET="ascii"
oracleCharsetCodeMapDict={
    b'\x54\x03':"gbk",
    b'\x69\x03':"utf-8",
    b'\x01\x00':"ascii"
    
}
 
############# 类定义 ######################

 






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
            for d in bdata:
                idata+=d*(100**(high-0xc1))
            
            return str(idata)
        else:
            #data < 0   
            bdata=f.read(size-2)
            signed=f.read(1)
            
            idata=0
            for d in bdata:
                idata+=d*(100**(0x3e-high))
            
            return str(-idata)            
         

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

def readFieldInfo(f: io.BufferedReader)->OracleField:
    fieldTypeBytes=f.read(2);
    field=FIELD_TYPES.get(fieldTypeBytes,OracleField)()
    success=field.readMetaInfo(f);
    if success:
        return field
    else:
        return None
       
def readFieldsData(f:io.BufferedReader,fieldtypes:List[OracleField])->bool:
    
    while True:
        blen=f.read(2)
        #判断是否结束
        if blen==b'\x00\x00':
            blen=f.read(2)
            if blen==b'\xff\xff':
                return True
            else:
                return False;
        else:
            f.seek(f.tell()-2)        
            
        for ft in fieldtypes:
            print(ft.readData(f))
        



#--------------------------------


def main():

    logging.info("开始分析文件")
    options, args = getopt.getopt(sys.argv[1:], "o", longopts=[])
    if len(args)>0:
        fileName=args[0]
    else:
        fileName= "20220216-1105.dmp"
    print("filename="+fileName);
    with open(fileName, "rb") as f:
        fileidx = 0
        try:
            
            a = f.read(1);
            

            if (a[0] != 0x03):
                print("第一个字节不是预期值，文件可能不是dump文件。：", a)
            fileidx = f.tell();

            thecharsetCode=f.read(2)
            currentCharsetName=oracleCharsetCodeMapDict.get(thecharsetCode[::-1]);
            print("dump 文件字符集:",currentCharsetName)

            ret: Result = None
            for num in range(4):
                ret = ByteUtil.readBytes(f, b'\x0a')
                if (ret.code == ResultCode.ERROR):
                    print(ret.msg)
                    return
                print((bytes(ret.data[0:-1]).decode(currentCharsetName, 'ignore')))
            entityOffset=bytes(ret.data).decode(currentCharsetName, 'ignore');
            
            print("获取到dump数据进入点位置:",entityOffset)
            f.seek(int(entityOffset));
            
            #读第一段带长度字节 到 +00：00        
            nextLen=  ByteUtil.littleOrderBytes2UnsignedInt(f.read(2));
            while(nextLen>0):
                f.read(nextLen);
                nextLen=  ByteUtil.littleOrderBytes2UnsignedInt(f.read(2));

            #读第二段带长度字节到 DISABLE:ALL
            nextLen= ByteUtil.littleOrderBytes2UnsignedInt(f.read(2));
            while(nextLen>0):
                f.read(nextLen);
                nextLen=  ByteUtil.littleOrderBytes2UnsignedInt(f.read(2));

            # 接下来是一段找不到长度定义的字节了,直接强行读到insert算了
            
            ret=ByteUtil.readBytes(f, b'\x0a')
            while ret.code==ResultCode.OK:
                insertSql=bytes(ret.data).decode(currentCharsetName, 'ignore');
                #print(insertSql)
                if insertSql.strip().startswith("INSERT INTO"):

                    break
                ret=ByteUtil.readBytes(f, b'\x0a')
            
            if ret.code!=ResultCode.OK:
                print("没找到INSERT 语句，退出") 
            
            print("找到插入语句，准备获取插入数据：",insertSql)
            
            colCount= ByteUtil.littleOrderBytes2UnsignedInt(f.read(2))
            print("字段总数：",colCount)
            tableFields=[]
            for i in range(colCount):
                theFieldInfo=readFieldInfo(f)
                if(theFieldInfo==None):
                    print("field ",i," unknown,exit.")
                    return;
                print("field ",i,"type:", theFieldInfo.type,",len:",theFieldInfo.defineLen,",charset:",theFieldInfo.charset)
                tableFields.append(theFieldInfo);
            #四字节0的分隔符
            spearate=  f.read(4)
            if spearate!=b'\x00\x00\x00\x00':
                print("预期的字段定义与数据值中间的分隔符未出现，程序退出。file offset=",hex(f.tell()))
                return;
            
            readFieldsData(f,tableFields)
        except Exception as e:
            print("catch Exception: prefileIdx=",fileidx,",file offset=",hex(f.tell()))        
            raise e
        
            
        
        
        

        
        
        

    # entity = f.read(4);
    # print(str(entity))




 
 

if __name__ == '__main__':
    main()
    # pass  什么都不干时，用pass，例如只有if语句，后面什么事也没干，这时不加pass就要报错
