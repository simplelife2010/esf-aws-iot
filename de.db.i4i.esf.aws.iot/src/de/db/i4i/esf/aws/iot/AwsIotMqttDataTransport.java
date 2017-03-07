package de.db.i4i.esf.aws.iot;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.Map;

import org.eclipse.kura.KuraConnectException;
import org.eclipse.kura.KuraErrorCode;
import org.eclipse.kura.KuraException;
import org.eclipse.kura.KuraNotConnectedException;
import org.eclipse.kura.KuraTimeoutException;
import org.eclipse.kura.KuraTooManyInflightMessagesException;
import org.eclipse.kura.configuration.ConfigurableComponent;
import org.eclipse.kura.configuration.ConfigurationService;
import org.eclipse.kura.core.util.ValidationUtil;
import org.eclipse.kura.data.DataTransportService;
import org.eclipse.kura.data.DataTransportToken;
import org.eclipse.kura.data.transport.listener.DataTransportListener;
import org.eclipse.kura.ssl.SslManagerService;
import org.eclipse.kura.ssl.SslServiceListener;
import org.eclipse.kura.status.CloudConnectionStatusComponent;
import org.eclipse.kura.status.CloudConnectionStatusEnum;
import org.eclipse.kura.status.CloudConnectionStatusService;
import org.osgi.service.component.ComponentContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.iot.client.AWSIotConnectionStatus;
import com.amazonaws.services.iot.client.AWSIotException;
import com.amazonaws.services.iot.client.AWSIotMessage;
import com.amazonaws.services.iot.client.AWSIotMqttClient;
import com.amazonaws.services.iot.client.AWSIotQos;
import com.amazonaws.services.iot.client.AWSIotTimeoutException;

public class AwsIotMqttDataTransport implements DataTransportService, ConfigurableComponent, CloudConnectionStatusComponent,SslServiceListener {

	private static final Logger logger = LoggerFactory.getLogger(AwsIotMqttDataTransport.class);
	
	private static final String MQTT_BROKER_URL_PROP_NAME = "broker.url";
	private static final String MQTT_CLIENT_ID_PROP_NAME = "client.id";
	private static final String MQTT_BASE_RETRY_DELAY_PROP_NAME = "base.retry.delay";
	private static final String MQTT_CONNECTION_TIMEOUT_PROP_NAME = "connection.timeout";
	private static final String MQTT_KEEP_ALIVE_INTERVAL_PROP_NAME = "keep.alive.interval";
	private static final String MQTT_MAX_CONNECTION_RETRIES_PROP_NAME = "max.connection.retries";
	private static final String MQTT_MAX_OFFLINE_QUEUE_SIZE_PROP_NAME = "max.offline.queue.size";
	private static final String MQTT_MAX_RETRY_DELAY_PROP_NAME = "max.retry.delay";
	private static final String MQTT_NUM_OF_CLIENT_THREADS_PROP_NAME = "num.of.client.threads";
	private static final String MQTT_SERVER_ACK_TIMEOUT_PROP_NAME = "server.ack.timeout";
	private static final String MQTT_WILL_MESSAGE_TOPIC_PROP_NAME = "will.message.topic";
	private static final String MQTT_WILL_MESSAGE_PAYLOAD_PROP_NAME = "will.message.payload";
	private static final String MQTT_WILL_MESSAGE_QOS_PROP_NAME = "will.message.qos";
	
	private SslManagerService sslManagerService;
	private CloudConnectionStatusService cloudConnectionStatusService;
	private CloudConnectionStatusEnum notificationStatus = CloudConnectionStatusEnum.OFF;
	
	private AWSIotMqttClient mqttClient;
	
	private Map<String, Object> properties;
	
	private String brokerUrl;
	private String clientId;
	private Integer baseRetryDelay;
	private Integer connectionTimeout;
	private Integer keepAliveInterval;
	private Integer maxConnectionRetries;
	private Integer maxOfflineQueueSize;
	private Integer maxRetryDelay;
	private Integer numOfClientThreads;
	private Integer serverAckTimeout;
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
	
    protected void activate(ComponentContext componentContext, Map<String, Object> properties) {
    	logger.info("Activating {}...", properties.get(ConfigurationService.KURA_SERVICE_PID));
    	this.properties.putAll(properties);
    	try {
    		logger.info("Building new configuration...");
            buildConfiguration(properties);
        } catch (RuntimeException e) {
            logger.error("Invalid client configuration. Service will not be able to connect until the configuration is updated", e);
        }
    }
    
    protected void deactivate(ComponentContext componentContext) {
        logger.debug("Deactivating {}...", this.properties.get(ConfigurationService.KURA_SERVICE_PID));
        if (isConnected()) {
            disconnect(0);
        }
    }
    
    public void updated(Map<String, Object> properties) {
        logger.info("Updating {}...", properties.get(ConfigurationService.KURA_SERVICE_PID));
        this.properties.putAll(properties);
        update();
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
		this.notificationStatus = status;
	}
	
	@Override
	public void onConfigurationUpdated() {
		update();
	}
	
	private void buildConfiguration(Map<String, Object> properties) {
		try {
			this.brokerUrl = (String) properties.get(MQTT_BROKER_URL_PROP_NAME);
			ValidationUtil.notEmptyOrNull(this.brokerUrl, MQTT_BROKER_URL_PROP_NAME);
			this.clientId = (String) properties.get(MQTT_CLIENT_ID_PROP_NAME);
			ValidationUtil.notEmptyOrNull(this.clientId, MQTT_CLIENT_ID_PROP_NAME);
			this.baseRetryDelay = (Integer) properties.get(MQTT_BASE_RETRY_DELAY_PROP_NAME);
			this.connectionTimeout = (Integer) properties.get(MQTT_CONNECTION_TIMEOUT_PROP_NAME);
			this.keepAliveInterval = (Integer) properties.get(MQTT_KEEP_ALIVE_INTERVAL_PROP_NAME);
			this.maxConnectionRetries = (Integer) properties.get(MQTT_MAX_CONNECTION_RETRIES_PROP_NAME);
			this.maxOfflineQueueSize = (Integer) properties.get(MQTT_MAX_OFFLINE_QUEUE_SIZE_PROP_NAME);
			this.maxRetryDelay = (Integer) properties.get(MQTT_MAX_RETRY_DELAY_PROP_NAME);
			this.numOfClientThreads = (Integer) properties.get(MQTT_NUM_OF_CLIENT_THREADS_PROP_NAME);
			this.serverAckTimeout = (Integer) properties.get(MQTT_SERVER_ACK_TIMEOUT_PROP_NAME);
			this.willMessageTopic = (String) properties.get(MQTT_WILL_MESSAGE_TOPIC_PROP_NAME);
			this.willMessagePayload = (String) properties.get(MQTT_WILL_MESSAGE_PAYLOAD_PROP_NAME);
			this.willMessageQos = (String) properties.get(MQTT_WILL_MESSAGE_QOS_PROP_NAME);
			if (!this.willMessageTopic.isEmpty()) {
				ValidationUtil.notEmptyOrNull(this.willMessageQos, MQTT_WILL_MESSAGE_QOS_PROP_NAME);
				try {
					AWSIotQos.valueOf(this.willMessageQos);
				} catch (IllegalArgumentException e) {
					throw new KuraException(KuraErrorCode.CONFIGURATION_ATTRIBUTE_INVALID, MQTT_WILL_MESSAGE_QOS_PROP_NAME);
				}
			}
		} catch (KuraException e) {
			logger.error("Invalid configuration");
            throw new IllegalStateException("Invalid MQTT client configuration", e);
		}
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
			if (!this.willMessageTopic.isEmpty()) {
				this.mqttClient.setWillMessage(new AWSIotMessage(this.willMessageTopic, AWSIotQos.valueOf(this.willMessageQos), this.willMessagePayload));
			}
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
	
	private void update() {
    	try {
    		logger.info("Building new configuration...");
            buildConfiguration(properties);
        } catch (RuntimeException e) {
            logger.error("Invalid client configuration. Service will not be able to connect until the configuration is updated", e);
        }
    }
}
