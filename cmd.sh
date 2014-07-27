#!/bin/bash
hadoop jar apJob.jar ap.v2.APCluster -i input -o output -n 10000 -d 41 -c 2 -m 2 -dp 0.6 -n1 8 -n2 4
