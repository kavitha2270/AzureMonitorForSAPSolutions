Dockerfile to create configured images containing the data collector scripts to collect telemetry from customers SAP solutions. The scripts can be found in [Azure Monitor For SAP Solutions](https://github.com/Azure/AzureMonitorForSAPSolutions)

# How to run

## Monitor
```bash
docker run --name sapmon-ver-<version> --detach --restart always --log-opt max-size=50m --log-opt max-file=5 --network host --volume /var/opt/microsoft/sapmon/state:/var/opt/microsoft/sapmon/<version>/sapmon/state --env Version=<version> <image:tag> python3 /var/opt/microsoft/sapmon/<version>/sapmon/payload/sapmon.py monitor
```