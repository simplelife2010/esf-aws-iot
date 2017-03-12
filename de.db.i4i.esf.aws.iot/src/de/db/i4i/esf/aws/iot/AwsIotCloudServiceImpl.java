package de.db.i4i.esf.aws.iot;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import org.eclipse.kura.KuraErrorCode;
import org.eclipse.kura.KuraException;
import org.eclipse.kura.KuraInvalidMessageException;
import org.eclipse.kura.cloud.CloudClient;
import org.eclipse.kura.cloud.CloudConnectionEstablishedEvent;
import org.eclipse.kura.cloud.CloudConnectionLostEvent;
import org.eclipse.kura.cloud.CloudService;
import org.eclipse.kura.configuration.ConfigurableComponent;
import org.eclipse.kura.configuration.ConfigurationService;
import org.eclipse.kura.data.DataService;
import org.eclipse.kura.data.listener.DataServiceListener;
import org.eclipse.kura.message.KuraPayload;
import org.osgi.service.component.ComponentContext;
import org.osgi.service.event.EventAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AwsIotCloudServiceImpl implements CloudService, DataServiceListener, ConfigurableComponent,
		CloudPayloadJsonEncoder, CloudPayloadJsonDecoder {
	
	private static final Logger logger = LoggerFactory.getLogger(AwsIotCloudServiceImpl.class);
	
	private AwsIotCloudServiceOptions options;
	
	private DataService dataService;
	private EventAdmin eventAdmin;
	
	private final List<AwsIotCloudClientImpl> cloudClients;
	
	public AwsIotCloudServiceImpl() {
        this.cloudClients = new CopyOnWriteArrayList<AwsIotCloudClientImpl>();
    }

	public void setDataService(DataService dataService) {
        this.dataService = dataService;
    }

    public void unsetDataService(DataService dataService) {
        this.dataService = null;
    }
    
    public void setEventAdmin(EventAdmin eventAdmin) {
        this.eventAdmin = eventAdmin;
    }

    public void unsetEventAdmin(EventAdmin eventAdmin) {
        this.eventAdmin = null;
    }
	
	protected void activate(ComponentContext componentContext, Map<String, Object> properties) {
        logger.info("activate {}...", properties.get(ConfigurationService.KURA_SERVICE_PID));
        
        this.options = new AwsIotCloudServiceOptions();
        
        this.dataService.addDataServiceListener(this);
	}
	
	public void updated(Map<String, Object> properties) {
		logger.info("updated {}...: {}", properties.get(ConfigurationService.KURA_SERVICE_PID), properties);
		
		this.options = new AwsIotCloudServiceOptions();
	}
	
	protected void deactivate(ComponentContext componentContext) {
		logger.info("deactivate {}...", componentContext.getProperties().get(ConfigurationService.KURA_SERVICE_PID));
		
		this.dataService.removeDataServiceListener(this);
		this.cloudClients.clear();
		this.unsetDataService(dataService);
		this.unsetEventAdmin(eventAdmin);
	}
	
	@Override
	public KuraPayload buildFromByteArray(byte[] payload) throws KuraException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public byte[] getBytes(KuraPayload kuraPayload) throws KuraException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void onConnectionEstablished() {
		this.eventAdmin.postEvent(new CloudConnectionEstablishedEvent(new HashMap<String, Object>()));
        // notify listeners
        for (AwsIotCloudClientImpl cloudClient : this.cloudClients) {
            cloudClient.onConnectionEstablished();
        }
	}

	@Override
	public void onDisconnecting() {}

	@Override
	public void onDisconnected() {
		this.eventAdmin.postEvent(new CloudConnectionLostEvent(new HashMap<String, Object>()));
	}

	@Override
	public void onConnectionLost(Throwable cause) {
		// raise event
        this.eventAdmin.postEvent(new CloudConnectionLostEvent(new HashMap<String, Object>()));
        // notify listeners
        for (AwsIotCloudClientImpl cloudClient : this.cloudClients) {
            cloudClient.onConnectionLost();
        }
	}

	@Override
	public void onMessageArrived(String topic, byte[] payload, int qos, boolean retained) {
		logger.info("Message arrived on topic: {}", topic);

		String appTopic = new String("");
		String[] topicParts = topic.split(this.options.getTopicSeparator());
		
		if(topicParts.length == 0) {
			logger.warn("Empty topic, ignoring message");
			return;
		}
		
		if (!topicParts[0].equals(this.options.getTopicAccountToken())) {
			logger.warn("Unexpected topic {}, ignoring message", topic);
			return;
		}
		
		if (topicParts.length > 1) {
			appTopic = topic.substring(1 + topic.indexOf(this.options.getTopicSeparator()));
		}
		
		KuraPayload kuraPayload;
		try {
			kuraPayload = new CloudPayloadJsonDecoderImpl(payload).buildFromByteArray();
		} catch (Exception e) {
			logger.warn(
                    "Received message on topic {} that could not be decoded. Wrapping it into a KuraPayload.",
                    topic);
            kuraPayload = new KuraPayload();
            kuraPayload.setBody(payload);
		}
		
		for (AwsIotCloudClientImpl cloudClient : this.cloudClients) {
			cloudClient.onMessageArrived("", appTopic,
                    kuraPayload, qos, retained);
		}
	}

	@Override
	public void onMessagePublished(int messageId, String topic) {
		// TODO Auto-generated method stub

	}

	@Override
	public void onMessageConfirmed(int messageId, String topic) {
		// TODO Auto-generated method stub

	}

	@Override
	public CloudClient newCloudClient(String applicationId) throws KuraException {
        AwsIotCloudClientImpl cloudClient = new AwsIotCloudClientImpl(applicationId, this.dataService, this);
        this.cloudClients.add(cloudClient);
        return cloudClient;
    }

	@Override
	public String[] getCloudApplicationIdentifiers() {
		List<String> appIds = new ArrayList<String>();
        for (AwsIotCloudClientImpl cloudClient : this.cloudClients) {
            appIds.add(cloudClient.getApplicationId());
        }
        return appIds.toArray(new String[0]);
	}

	@Override
	public boolean isConnected() {
		return this.dataService != null && this.dataService.isConnected();
	}

	public AwsIotCloudServiceOptions getCloudServiceOptions() {
        return this.options;
    }
	
	public void removeCloudClient(AwsIotCloudClientImpl cloudClient) {
        this.cloudClients.remove(cloudClient);
    }
	
	byte[] encodePayload(KuraPayload payload) throws KuraException {
        byte[] bytes = new byte[0];
        if (payload == null) {
            return bytes;
        }
        CloudPayloadEncoder encoder = new CloudPayloadJsonEncoderImpl(payload);
        try {
            bytes = encoder.getBytes();
            return bytes;
        } catch (IOException e) {
            throw new KuraException(KuraErrorCode.ENCODE_ERROR, e);
        }
    }
}
