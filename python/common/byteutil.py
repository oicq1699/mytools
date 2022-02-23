import io
import operator
import common.result as result
from common.result import  Result

class ByteUtil:
    @classmethod
    def readBytes(cls,f: io.BufferedReader, endFlagBytes: bytes, maxLen: int = 8192) -> Result:
        """读取字节直到指定的中止标记为止

        :param f: 要读取的文件
        :param endFlagBytes: 中止标记，可以是一到多个字节数据
        :param maxLen: 最大读取字节数，超过这个字节数返回错误（result.Result包装）
        :return: 结果，由result.Result包装
        """
        idx = 0
        findList = list(endFlagBytes)
        findLen = len(findList);
        firstread=f.read(findLen);
        targetList = list(firstread)
        if len(targetList) != findLen:
            return result.Result(result.ResultCode.ERROR,
                                "已不能从文件中获取指定长度数据,file offset=" + f.tell() + ",当前仅获取" + len(targetList) + "字节数据")
        if operator.eq(findList, targetList):
            return result.successResult(data=targetList)

        while idx < maxLen:
            nextByte = f.read(1)
            if len(nextByte) == 0:
                return result.errorResult(msg="读取到文件尾部，还未匹配到相应数据,file offset=" + f.tell())

            idx += 1
            targetList.append(nextByte[0])
            if operator.eq(findList, targetList[idx:]):
                return result.successResult(data=targetList)

        return result.errorResult(msg="到达读取最大字节数(" + (maxLen) + ")，还未匹配到相应数据,file offset=" + f.tell())
    @classmethod
    def littleOrderBytes2UnsignedInt(cls,bs:bytes)->int:
        return int.from_bytes(bs,byteorder='little',signed=False);

    @classmethod
    def littleOrderBytes2Int(cls,bs:bytes)->int:
        return int.from_bytes(bs,byteorder='little',signed=True);

    @classmethod    
    def bigOrderBytes2UnsignedInt(cls,bs:bytes)->int:
        return int.from_bytes(bs,byteorder='big',signed=False);

    @classmethod
    def bigOrderBytes2Int(cls,bs:bytes)->int:
        return int.from_bytes(bs,byteorder='big',signed=True);
    

    def findByte(cls,f: io.BufferedReader, start: int, findbytes: bytes, maxLen: int = 8192) -> Result:
        """

        :param f:
        :param start:
        :param findbytes:
        :param maxLen:
        :return:
        """
        f.seek(start)
        idx = 0
        findList = list(findbytes)
        findLen = len(findList);
        targetList = list(f.read(findLen))
        if len(targetList) != findLen:
            return result.Result(result.ResultCode.ERROR,
                                "已不能从文件中获取指定长度数据,file offset=" + (start + idx) + ",当前仅获取" + len(targetList) + "字节数据")
        if operator.eq(findList, targetList):
            return result.successResult(data=idx)

        while (idx < maxLen):
            nextByte = f.read(1)
            if (len(nextByte) == 0):
                return result.errorResult(msg="读取到文件尾部，还未匹配到相应数据,file offset=" + (start + idx))

            idx += 1
            targetList.pop(0)
            targetList.append(nextByte)
            if operator.eq(findList, targetList):
                return result.successResult(data=idx)

        return result.errorResult(msg="到达读取最大字节数(" + (maxLen) + ")，还未匹配到相应数据,file offset=" + (start + idx))