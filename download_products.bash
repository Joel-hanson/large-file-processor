#!/bin/bash
fileid="11ACp03VCQY5NElctMq7F5zn23jKrqTZI"
filename="products.csv.gz"
curl -c ./cookie -s -L "https://drive.google.com/uc?export=download&id=${fileid}" > /dev/null
curl -Lb ./cookie "https://drive.google.com/uc?export=download&confirm=`awk '/download/ {print $NF}' ./cookie`&id=${fileid}" -o ${filename}
gzip -d $filename