#!/bin/sh

version=$1
infinite=1
while [ $infinite -eq 1 ]
do
  python3 /var/opt/microsoft/AzureMonitorForSAPSolutions/sapmon/payload/sapmon.py monitor
  sleep 60s
done

