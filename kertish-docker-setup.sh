#!/bin/sh

./kertish-admin -create-cluster 172.20.1.20:9430,172.20.1.21:9430
./kertish-admin -create-cluster 172.20.1.30:9430,172.20.1.31:9430
./kertish-admin -create-cluster 172.20.1.40:9430,172.20.1.41:9430

./kertish-admin -sync-clusters --force