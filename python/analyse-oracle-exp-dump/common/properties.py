class Properties:
    def __init__(self, filename, encoding = 'utf-8'):
        self.filename = filename
        self.fp_read = open(self.filename, 'r', encoding=encoding)
        self.data = self.fp_read.readlines()
        self.fp_read.close()
        self.properties = {}
        for i in self.data:
            if '#' in i:
                continue
            sp = i.split('=')
            key = sp[0].replace(' ','')
            value = sp[-1].strip()
            if value!='':
                self.properties.update({key: value})

    def get(self, key,defValue=None)->str:
        if key in self.properties:
            return self.properties[key]
        else:
            return defValue
        

    def set(self, key, value):
        self.fp_write = open(self.filename, 'w', encoding='utf-8')
        try:
            self.properties[key] = value
        except:
            pass
        data = ""
        for k,v in self.properties.items():
            data += k + " = " + v + "\n"

        self.fp_write.writelines(data[:-1])
        self.fp_write.close()

if __name__ == '__main__':
    # setting.conf
    # a = 1
    # b = 2
    p = Properties('setting.conf','utf-8')
    p.get('a')
    p.set('a','2')
 