package de.db.i4i.esf.aws.iot;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;

import org.eclipse.kura.KuraConnectException;
import org.eclipse.kura.KuraException;
import org.eclipse.kura.KuraNotConnectedException;
import org.eclipse.kura.KuraTimeoutException;
import org.eclipse.kura.KuraTooManyInflightMessagesException;
import org.eclipse.kura.configuration.ConfigurableComponent;
import org.eclipse.kura.data.DataTransportService;
import org.eclipse.kura.data.DataTransportToken;
import org.eclipse.kura.data.transport.listener.DataTransportListener;
import org.eclipse.kura.ssl.SslManagerService;
import org.eclipse.kura.status.CloudConnectionStatusComponent;
import org.eclipse.kura.status.CloudConnectionStatusEnum;
import org.eclipse.kura.status.CloudConnectionStatusService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.iot.client.AWSIotConnectionStatus;
import com.amazonaws.services.iot.client.AWSIotException;
import com.amazonaws.services.iot.client.AWSIotMessage;
import com.amazonaws.services.iot.client.AWSIotMqttClient;
import com.amazonaws.services.iot.client.AWSIotQos;
import com.amazonaws.services.iot.client.AWSIotTimeoutException;

public class AwsIotMqttDataTransport implements DataTransportService, ConfigurableComponent, CloudConnectionStatusComponent {

	private static final Logger logger = LoggerFactory.getLogger(AwsIotMqttDataTransport.class);
	
	private SslManagerService sslManagerService;
	private CloudConnectionStatusService cloudConnectionStatusService;
	
	private AWSIotMqttClient mqttClient;
	
	private String brokerUrl;
	private String clientId;
	private int baseRetryDelay;
	private int connectionTimeout;
	private int keepAliveInterval;
	private int maxConnectionRetries;
	private int maxOfflineQueueSize;
	private int maxRetryDelay;
	private int numOfClientThreads;
	private int serverAckTimeout;
	private String willMessageTopic;
	private String willMessagePayload;
	private String willMessageQos;
	
	public void setSslManagerService(SslManagerService sslManagerService) {
        this.sslManagerService = sslManagerService;
    }

    public void unsetSslManagerService(SslManagerService sslManagerService) {
        this.sslManagerService = null;
    }
	
	public void setCloudConnectionStatusService(CloudConnectionStatusService cloudConnectionStatusService) {
        this.cloudConnectionStatusService = cloudConnectionStatusService;
    }

    public void unsetCloudConnectionStatusService(CloudConnectionStatusService cloudConnectionStatusService) {
        this.cloudConnectionStatusService = null;
    }
	
	@Override
	public synchronized void connect() throws KuraConnectException {
		// ToDo: Notify listeners
		if (isConnected()) {
            logger.error("Already connected");
            throw new IllegalStateException("Already connected");
        }
		
		setupMqttSession();
		
		if (this.mqttClient == null) {
            logger.error("Invalid configuration");
            throw new IllegalStateException("Invalid configuration");
        }
		
		logConnectionProperties();
        
		this.cloudConnectionStatusService.register(this);
		this.cloudConnectionStatusService.updateStatus(this, CloudConnectionStatusEnum.FAST_BLINKING);
		
		try {
			this.mqttClient.connect(this.connectionTimeout);
			logger.info("#  Connected!");
            logger.info("# ------------------------------------------------------------");
            this.cloudConnectionStatusService.updateStatus(this, CloudConnectionStatusEnum.ON);
		} catch (Exception e) {
			logger.warn("xxxxx  Connect failed. Forcing disconnect. xxxxx {}", e);
            closeMqttClient();
            this.cloudConnectionStatusService.updateStatus(this, CloudConnectionStatusEnum.OFF);
            throw new KuraConnectException(e, "Cannot connect");
		} finally {
			this.cloudConnectionStatusService.unregister(this);
        }
	}

	@Override
	public boolean isConnected() {
		if (this.mqttClient != null) {
			return (this.mqttClient.getConnectionStatus() == AWSIotConnectionStatus.CONNECTED);
		}
		return false;
	}

	@Override
	public String getBrokerUrl() {
		return this.brokerUrl;
	}

	@Override
	public String getAccountName() {
		return "";
	}

	@Override
	public String getUsername() {
		return "";
	}

	@Override
	public String getClientId() {
		return this.clientId;
	}

	@Override
	public synchronized void disconnect(long quiesceTimeout) {
		// ToDo: Notify listeners
		if (isConnected()) {
            logger.info("Disconnecting...");
            AWSIotMqttClient disconnectingMqttClient = this.mqttClient;
            this.mqttClient = null;
            if (quiesceTimeout > 0) {
				try {
					Thread.sleep(quiesceTimeout);
				} catch (InterruptedException e) {
					logger.warn("Sleep interrupted");
				}
            }
            try {
				disconnectingMqttClient.disconnect(0, false);
			} catch (Exception e) {}
		} else {
            logger.warn("MQTT client already disconnected");
		}
	}

	@Override
	public void subscribe(String topic, int qos) throws KuraTimeoutException, KuraException, KuraNotConnectedException {
		// TODO Auto-generated method stub

	}

	@Override
	public void unsubscribe(String topic) throws KuraTimeoutException, KuraException, KuraNotConnectedException {
		// TODO Auto-generated method stub

	}

	@Override
	public DataTransportToken publish(String topic, byte[] payload, int qos, boolean retain)
			throws KuraTooManyInflightMessagesException, KuraException, KuraNotConnectedException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void addDataTransportListener(DataTransportListener listener) {
		// TODO Auto-generated method stub

	}

	@Override
	public void removeDataTransportListener(DataTransportListener listener) {
		// TODO Auto-generated method stub

	}
	
	@Override
	public int getNotificationPriority() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public CloudConnectionStatusEnum getNotificationStatus() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setNotificationStatus(CloudConnectionStatusEnum status) {
		// TODO Auto-generated method stub
		
	}
	
	private void setupMqttSession() throws KuraConnectException {
		if (this.brokerUrl == null || this.clientId == null) {
            throw new IllegalStateException("Invalid client configuration");
        }
		
		if (this.mqttClient != null) {
			String sessionBrokerUrl = this.mqttClient.getClientEndpoint();
			String sessionClientId = this.mqttClient.getClientId();
			
			if (!sessionBrokerUrl.equals(this.brokerUrl) || !sessionClientId.equals(this.clientId)) {
				closeMqttClient();
			}
		}
		
		if (this.mqttClient == null) {
			logger.info("Creating a new client instance");
			this.mqttClient = new AWSIotMqttClient(this.brokerUrl, this.clientId, getKeyStore(), getKeyStorePassword());
			this.mqttClient.setBaseRetryDelay(this.baseRetryDelay);
			this.mqttClient.setConnectionTimeout(this.connectionTimeout);
			this.mqttClient.setKeepAliveInterval(this.keepAliveInterval);
			this.mqttClient.setMaxConnectionRetries(this.maxConnectionRetries);
			this.mqttClient.setMaxOfflineQueueSize(this.maxOfflineQueueSize);
			this.mqttClient.setMaxRetryDelay(this.maxRetryDelay);
			this.mqttClient.setNumOfClientThreads(this.numOfClientThreads);
			this.mqttClient.setServerAckTimeout(this.serverAckTimeout);
			this.mqttClient.setWillMessage(new AWSIotMessage(this.willMessageTopic, AWSIotQos.valueOf(this.willMessageQos), this.willMessagePayload));
		}
	}
	
	KeyStore getKeyStore() throws KuraConnectException {
		FileInputStream fis = null;
		KeyStore keyStore = null;
		
		try {
			keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
			fis = new FileInputStream(this.sslManagerService.getConfigurationOptions().getSslKeyStore());
			keyStore.load(fis, getKeyStorePassword().toCharArray());
			return keyStore;
		} catch (CertificateException e) {
			throw new KuraConnectException(e, "One or more certificates in the KeyStore could not be loaded");
		} catch (KeyStoreException e) {
			throw new KuraConnectException(e, "KeyStore type not supported");
		} catch (NoSuchAlgorithmException e) {
			throw new KuraConnectException(e, "Could not load KeyStore");
		} catch (GeneralSecurityException e) {
			throw new KuraConnectException(e, "Could not load KeyStore");
		} catch (FileNotFoundException e) {
			throw new KuraConnectException(e, "KeyStore file not found");
		} catch (IOException e) {
			throw new KuraConnectException(e, "Could not load KeyStore");
		} finally {
			if (fis != null) {
				try {
					fis.close();
				} catch (IOException e) {
					logger.error("Could not close file input stream for KeyStore file");
				}
			}
		}
	}
	
	String getKeyStorePassword() throws KuraConnectException {
		try {
			return this.sslManagerService.getConfigurationOptions().getSslKeystorePassword();
		} catch (GeneralSecurityException e) {
			throw new KuraConnectException(e, "Could not get configuration from SslManager");
		} catch (IOException e) {
			throw new KuraConnectException(e, "Could not get configuration from SslManager");
		}
	}
	
	void logConnectionProperties() {
		logger.info("# ------------------------------------------------------------");
        logger.info("#  Connection Properties");
        logger.info("#  broker               = " + this.brokerUrl);
        logger.info("#  clientId             = " + this.clientId);
        logger.info("#  baseRetryDelay       = " + this.baseRetryDelay);
        logger.info("#  connectionTimeout    = " + this.connectionTimeout);
        logger.info("#  keepAliveInterval    = " + this.keepAliveInterval);
        logger.info("#  maxConnectionRetries = " + this.maxConnectionRetries);
        logger.info("#  maxOfflineQueueSize  = " + this.maxOfflineQueueSize);
        logger.info("#  maxRetryDelay        = " + this.maxRetryDelay);
        logger.info("#  numOfClientThreads   = " + this.numOfClientThreads);
        logger.info("#  serverAckTimeout     = " + this.serverAckTimeout);
        logger.info("#  willMessageTopic     = " + this.willMessageTopic);
        logger.info("#  willMessagePayload   = " + this.willMessagePayload);
        logger.info("#  willMessageQos       = " + this.willMessageQos);
        logger.info("#");
        logger.info("#  Connecting...");
	}
	
	private void closeMqttClient() {
		logger.info("Closing client");
		try {
			this.mqttClient.disconnect(0, false);
		} catch (Exception e) {}
		this.mqttClient = null;
	}
}
