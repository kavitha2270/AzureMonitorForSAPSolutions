Dockerfile to create configured images containing the data collector scripts to collect telemetry from customers SAP solutions. The scripts can be found in [Azure Monitor For SAP Solutions](https://github.com/Azure/AzureMonitorForSAPSolutions)

# How to run

## Monitor
```bash
docker run -t <image:tag> python3 /var/opt/microsoft/sapmon/v1.6/sapmon.py monitor
```
