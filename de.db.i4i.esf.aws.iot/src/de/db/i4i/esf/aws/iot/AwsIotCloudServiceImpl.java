package de.db.i4i.esf.aws.iot;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import org.eclipse.kura.KuraErrorCode;
import org.eclipse.kura.KuraException;
import org.eclipse.kura.cloud.CloudClient;
import org.eclipse.kura.cloud.CloudService;
import org.eclipse.kura.configuration.ConfigurableComponent;
import org.eclipse.kura.configuration.ConfigurationService;
import org.eclipse.kura.data.DataService;
import org.eclipse.kura.data.listener.DataServiceListener;
import org.eclipse.kura.message.KuraPayload;
import org.osgi.service.component.ComponentContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AwsIotCloudServiceImpl implements CloudService, DataServiceListener, ConfigurableComponent,
		CloudPayloadJsonEncoder, CloudPayloadJsonDecoder {
	
	private static final Logger logger = LoggerFactory.getLogger(AwsIotCloudServiceImpl.class);
	
	private AwsIotCloudServiceOptions options;
	
	private DataService dataService;
	
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
	
	protected void activate(ComponentContext componentContext, Map<String, Object> properties) {
        logger.info("activate {}...", properties.get(ConfigurationService.KURA_SERVICE_PID));
        
        this.options = new AwsIotCloudServiceOptions();
	}
	
	public void updated(Map<String, Object> properties) {
		logger.info("updated {}...: {}", properties.get(ConfigurationService.KURA_SERVICE_PID), properties);
		
		this.options = new AwsIotCloudServiceOptions();
	}
	
	protected void deactivate(ComponentContext componentContext) {
		logger.info("deactivate {}...", componentContext.getProperties().get(ConfigurationService.KURA_SERVICE_PID));
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
		// TODO Auto-generated method stub

	}

	@Override
	public void onDisconnecting() {
		// TODO Auto-generated method stub

	}

	@Override
	public void onDisconnected() {
		// TODO Auto-generated method stub

	}

	@Override
	public void onConnectionLost(Throwable cause) {
		// TODO Auto-generated method stub

	}

	@Override
	public void onMessageArrived(String topic, byte[] payload, int qos, boolean retained) {
		// TODO Auto-generated method stub

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
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean isConnected() {
		// TODO Auto-generated method stub
		return false;
	}

	public AwsIotCloudServiceOptions getCloudServiceOptions() {
        return this.options;
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
