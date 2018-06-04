1  #!/usr/bin/python3
2  # -*- coding:utf-8 -*- 
3  #Author: Pang Yapeng


from hashlib import md5
if __name__ == "__main__":
    src = b"EBMPPBGateway10.1.96.3ebmp".hex()
    print(src)
    #src = [0x25,0x25,0x01,0x00,0x15,0x00,0x01,0x03,0x58,0x68,0x80,0x00,0x00,0x01,0x60,0x15,0x01,0x13,0x25,0x03,0x00]
    result = md5(bytearray(src,encoding='UTF-8')).hexdigest()
    #result = bytearray(src,encoding='UTF-8')
    print(result)


