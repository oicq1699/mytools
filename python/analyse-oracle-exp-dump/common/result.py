from __future__ import annotations
import enum
from typing import Any


class ResultCode(enum.Enum):
    OK=0
    ERROR=-1

class Result:
    code: ResultCode
    msg:str
    data:Any

    def __init__(self, code,data, msg):
        self.code=code
        self.data=data;
        self.msg=msg;
    
    def isSuccess(self)->bool:
        return self.code==ResultCode.OK
    
    def isError(self)->bool:
        return self.isSuccess()==False
    
    @classmethod
    def successResult(cls,data:Any=None,msg:str="")->Result:
        return cls(ResultCode.OK,data,msg);

    @classmethod
    def errorResult(cls,data:Any=None,msg:str="")->Result:
        return cls(ResultCode.ERROR,data,msg);


def successResult(data:Any=None,msg:str="")->Result:
    return Result(ResultCode.OK,data,msg);

def errorResult(data:Any=None,msg:str="")->Result:
    return Result(ResultCode.ERROR,data,msg);

