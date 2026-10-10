import blake3, sys
# Inputs: byte i = i % 251 (BLAKE3 spec style), lengths crossing chunk/block boundaries.
lens = [0,1,2,3,4,5,6,7,8,63,64,65,127,128,129,1023,1024,1025,2048,2049,3072,3073,4096,4097,5120,8192,8193,16384,31744,102400,102401,1048576+7]
with open("blake3_vectors.txt","w") as f:
    for n in lens:
        data = bytes(i % 251 for i in range(n))
        f.write(f"{n} {blake3.blake3(data).hexdigest()}\n")
print("ok")
