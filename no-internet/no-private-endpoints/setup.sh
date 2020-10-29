#!/bin/bash
set -e

az extension add -n sap-hana 2>/dev/null
az extension add -n log-analytics 2>/dev/null

SAPMON_RG=$1
SAPMON_NAME=$2
VERSION_TO_UPDATE=$3

SAPMON=$(az sapmonitor show -g ${SAPMON_RG} -n ${SAPMON_NAME})
if [ $? -ne 0 ]; then
    echo "Unable to find SapMonitor"
    exit 1
fi

echo ${SAPMON} | jq

SAPMON_ID=$(echo ${SAPMON} | jq .managedResourceGroupName -r | cut -d'-' -f3)
COLLECTOR_VERSION=$(echo ${SAPMON} | jq .sapMonitorCollectorVersion -r)
LAWS_ARM_ID=$(echo ${SAPMON} | jq .logAnalyticsWorkspaceArmId -r)
LAWS_SUBSCRIPTION=$(echo ${LAWS_ARM_ID} | cut -d'/' -f3)
LAWS_RG=$(echo ${LAWS_ARM_ID} | cut -d'/' -f5)
LAWS_NAME=$(echo ${LAWS_ARM_ID} | cut -d'/' -f9)

UNSUPPORTED_VERSIONS=("" "v1.5" "v1.6" "v2.0-beta" "2.0" "2.1" "2.2")

if [[ " ${UNSUPPORTED_VERSIONS[@]} " =~ " ${COLLECTOR_VERSION} " ]]; then
    echo "The SapMonitor is of an unsupported version, please recreate the SapMonitor"
    exit 1
fi

while true; do
    read -p "Is this the SapMonitor you want to update? (y/n): " yn
    case $yn in
        [Yy]* ) break;;
        [Nn]* ) exit;;
        * ) echo "Please answer yes or no.";;
    esac
done

# Version to update is set
if [[ ! -z "$VERSION_TO_UPDATE" ]]; then
    while true; do
        read -p "This will also update your SapMonitor version from ${COLLECTOR_VERSION} to ${VERSION_TO_UPDATE}, is that OK? (y/n): " yn
        case $yn in
            [Yy]* ) break;;
            [Nn]* ) exit;;
            * ) echo "Please answer yes or no.";;
        esac
    done
    COLLECTOR_VERSION=${VERSION_TO_UPDATE}
fi

echo "==== Fetching Log-Analytics information ===="
WORKSPACE_ID=$(az monitor log-analytics workspace show \
    --subscription ${LAWS_SUBSCRIPTION} \
    --resource-group ${LAWS_RG} \
    --workspace-name ${LAWS_NAME} \
    --query "customerId" \
    --output tsv)
SHARED_KEY=$(az monitor log-analytics workspace get-shared-keys \
    --subscription ${LAWS_SUBSCRIPTION} \
    --resource-group ${LAWS_RG} \
    --workspace-name ${LAWS_NAME} \
    --query "primarySharedKey" \
    --output tsv)

echo "==== Uploading Storage Account key to KeyVault ===="
USER_PRINCIPAL_NAME=$(az ad signed-in-user show --query "userPrincipalName" --output tsv)
STORAGE_KEY=$(az storage account keys list -n sapmonsto${SAPMON_ID} --query [0].value -o tsv)
az keyvault set-policy \
    --name sapmon-kv-${SAPMON_ID} \
    --resource-group sapmon-rg-${SAPMON_ID} \
    --upn ${USER_PRINCIPAL_NAME} \
    --secret-permissions set \
    --output none
az keyvault secret set \
    --vault-name sapmon-kv-${SAPMON_ID} \
    --name storageAccessKey \
    --value ${STORAGE_KEY} \
    --output none
az keyvault delete-policy \
    --name sapmon-kv-${SAPMON_ID} \
    --resource-group sapmon-rg-${SAPMON_ID} \
    --upn ${USER_PRINCIPAL_NAME} \
    --output none

echo "==== Downloading installation files ===="
wget -O no-internet-install-${COLLECTOR_VERSION}.tar https://github.com/Azure/AzureMonitorForSAPSolutions/releases/download/${COLLECTOR_VERSION}/no-internet-install-${COLLECTOR_VERSION}.tar

echo "==== Uploading installation files to Storage Account ===="
az storage container create \
    --account-name sapmonsto${SAPMON_ID} \
    --name no-internet \
    --public-access blob \
    --output none 2>/dev/null
az storage blob upload \
    --account-name sapmonsto${SAPMON_ID} \
    --container-name no-internet \
    --name no-internet-install-${COLLECTOR_VERSION}.tar \
    --file no-internet-install-${COLLECTOR_VERSION}.tar \
    --output none 2>/dev/null

echo "==== Configuring Collector VM ===="
COMMAND_TO_EXECUTE="wget https://sapmonsto${SAPMON_ID}.blob.core.windows.net/no-internet/no-internet-install-${COLLECTOR_VERSION}.tar && \
tar -xf no-internet-install-${COLLECTOR_VERSION}.tar && \
dpkg -i "'$(tar -tf no-internet-install-'"${COLLECTOR_VERSION}"'.tar | grep containerd.io_)'" && \
dpkg -i "'$(tar -tf no-internet-install-'"${COLLECTOR_VERSION}"'.tar | grep docker-ce-cli_)'" && \
dpkg -i "'$(tar -tf no-internet-install-'"${COLLECTOR_VERSION}"'.tar | grep docker-ce_)'" && \
docker load -i azure-monitor-for-sap-solutions-${COLLECTOR_VERSION}.tar && \
docker rm -f "'$(docker ps -aq)'" 2>/dev/null || true && \
docker run mcr.microsoft.com/oss/azure/azure-monitor-for-sap-solutions:${COLLECTOR_VERSION} python3 /var/opt/microsoft/sapmon/${COLLECTOR_VERSION}/sapmon/payload/sapmon.py onboard --logAnalyticsWorkspaceId ${WORKSPACE_ID} --logAnalyticsSharedKey ${SHARED_KEY} --enableCustomerAnalytics > /tmp/monitor.log.out && \
mkdir -p /var/opt/microsoft/sapmon/state && \
docker run --name sapmon-ver-${COLLECTOR_VERSION} --detach --restart always --network host --volume /var/opt/microsoft/sapmon/state:/var/opt/microsoft/sapmon/${COLLECTOR_VERSION}/sapmon/state --env Version=${COLLECTOR_VERSION} mcr.microsoft.com/oss/azure/azure-monitor-for-sap-solutions:${COLLECTOR_VERSION} sh /var/opt/microsoft/sapmon/${COLLECTOR_VERSION}/monitorapp.sh ${COLLECTOR_VERSION}"

az vm extension set \
    --resource-group sapmon-rg-${SAPMON_ID} \
    --vm-name sapmon-vm-${SAPMON_ID} \
    --name customScript \
    --publisher Microsoft.Azure.Extensions \
    --protected-settings "{\"commandToExecute\": \"${COMMAND_TO_EXECUTE}\"}" \
    --output none

echo "==== Update Complete ===="
