const dotenv = require('dotenv');
dotenv.config();

const { exec } = require('child_process');
const Client = require('azure-iot-device').ModuleClient;
const Message = require('azure-iot-device').Message;
const Transport = require('azure-iot-device-mqtt').Mqtt;
const ProvisioningDeviceClient = require('azure-iot-provisioning-device').ProvisioningDeviceClient;
const SymmetricKeySecurityClient = require('azure-iot-security-symmetric-key').SymmetricKeySecurityClient;
const ProvisioningTransport = require('azure-iot-provisioning-device-mqtt').Mqtt;
const ps = require('ps-node');

// Azure IoT Central settings
const {
  IOT_CENTRAL_SYMMETRIC_KEY,
  IOT_CENTRAL_ID_SCOPE,
  IOT_CENTRAL_REGISTRATION_ID,
  SCHEDULER_COMMAND
} = process.env;

const provisioningHost = 'global.azure-devices-provisioning.net';
const provisioningSecurityClient = new SymmetricKeySecurityClient(IOT_CENTRAL_REGISTRATION_ID, IOT_CENTRAL_SYMMETRIC_KEY);
const provisioningClient = ProvisioningDeviceClient.create(provisioningHost, IOT_CENTRAL_ID_SCOPE, new ProvisioningTransport(), provisioningSecurityClient);

let centralClient;
let previousStatus = null; 

// Function to send messages to IoT Central
function sendMessageToIoTCentral(messageContent) {
  const centralMsg = new Message(messageContent);
  centralClient.sendEvent(centralMsg, (err) => {
    if (err) {
      console.error('Failed to send message to IoT Central:', err.message);
    } else {
      console.log('Message sent to IoT Central:', messageContent);
    }
  });
}

// Initialize IoT Central client
function initializeIoTCentralClient() {
  return new Promise((resolve, reject) => {
    provisioningClient.register((err, result) => {
      if (err) {
        console.error('Error registering device:', err);
        reject(err);
        return;
      }
      console.log('Registration on IoT Central succeeded', result);
      centralClient = Client.fromConnectionString('HostName=' + result.assignedHub + ';DeviceId=' + IOT_CENTRAL_REGISTRATION_ID + ';SharedAccessKey=' + IOT_CENTRAL_SYMMETRIC_KEY, Transport);
      centralClient.open((err) => {
        if (err) {
          console.error('Could not connect to IoT Central:', err.message);
          reject(err);
        } else {
          resolve();
        }
      });
    });
  });
}

// Get private IP address
function getIpAddress() {
  return new Promise((resolve, reject) => {
    exec("hostname -I | awk '{print $1}'", (error, stdout, stderr) => {
      if (error) {
        reject(`exec error: ${error}`);
        return;
      }
      resolve(stdout.trim());
    });
  });
}

// Get Dask scheduler status
function getSchedulerStatus() {
  return new Promise((resolve) => {
    ps.lookup({ command: SCHEDULER_COMMAND }, (err, resultList) => {
      if (err) {
        throw new Error(err);
      }

      if (resultList.length > 0) {
        const proc = resultList[0];
        resolve({
          status: 'online',
          pid: proc.pid,
          cpuPercent: proc.cpu,
          memoryPercent: proc.memory,
          statusDetail: proc.state
        });
      } else {
        resolve({
          status: 'stopped',
          pid: null,
          cpuPercent: 0.0,
          memoryPercent: 0.0,
          statusDetail: 'none'
        });
      }
    });
  });
}

// Main function to initialize clients and monitor Dask scheduler
async function main() {
  try {
    await initializeIoTCentralClient();
    // Initial connection message
    sendMessageToIoTCentral(JSON.stringify({
      [`${SCHEDULER_COMMAND}_state`]: 'connected'
    }));

    setInterval(async () => {
      const schedulerStatus = await getSchedulerStatus();
      const ipAddress = await getIpAddress();
      const statusMessage = {
        ipAddress: ipAddress,
        [`${SCHEDULER_COMMAND}_status`]: schedulerStatus
      };

      if (schedulerStatus.status !== previousStatus) {
        sendMessageToIoTCentral(JSON.stringify(statusMessage));
        previousStatus = schedulerStatus.status;
      }
    }, 60000);

  } catch (err) {
    console.error('Error initializing IoT Central client:', err.message);
  }
}

main();
