package de.db.i4i.esf.aws.iot;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.eclipse.kura.KuraErrorCode;
import org.eclipse.kura.KuraException;
import org.eclipse.kura.cloud.CloudClient;
import org.eclipse.kura.cloud.CloudClientListener;
import org.eclipse.kura.data.DataService;
import org.eclipse.kura.message.KuraPayload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AwsIotCloudClientImpl implements CloudClient, CloudClientListener {

	private static final Logger logger = LoggerFactory.getLogger(AwsIotCloudClientImpl.class);
	
    private final String applicationId;
    private final DataService dataService;
    private final AwsIotCloudServiceImpl cloudServiceImpl;
    private final List<CloudClientListenerAdapter> listeners;
    
    private boolean useKuraTopicNamespace = false;

    protected AwsIotCloudClientImpl(String applicationId, DataService dataService, AwsIotCloudServiceImpl cloudServiceImpl) {
        this.applicationId = applicationId;
        this.dataService = dataService;
        this.cloudServiceImpl = cloudServiceImpl;
        this.listeners = new CopyOnWriteArrayList<CloudClientListenerAdapter>();
    }
	
	@Override
	public void onControlMessageArrived(String deviceId, String appTopic, KuraPayload msg, int qos, boolean retain) {
		logger.warn("Receiving control messages is not "
				+ "supported in this CloudClient implementation");
	}

	@Override
	public void onMessageArrived(String deviceId, String appTopic, KuraPayload payload, int qos, boolean retain) {
		for (CloudClientListener listener : this.listeners) {
            listener.onMessageArrived(deviceId, appTopic, payload, qos, retain);
        }
	}

	@Override
	public void onConnectionLost() {
		for (CloudClientListener listener : this.listeners) {
            listener.onConnectionLost();
        }
	}

	@Override
	public void onConnectionEstablished() {
		for (CloudClientListener listener : this.listeners) {
            listener.onConnectionEstablished();
        }
	}

	@Override
	public void onMessageConfirmed(int pubId, String appTopic) {
		for (CloudClientListener listener : this.listeners) {
            listener.onMessageConfirmed(pubId, appTopic);
        }
	}

	@Override
	public void onMessagePublished(int pubId, String appTopic) {
		for (CloudClientListener listener : this.listeners) {
            listener.onMessagePublished(pubId, appTopic);
        }
	}

	@Override
	public String getApplicationId() {
		return this.applicationId;
	}

	@Override
	public void release() {
		this.cloudServiceImpl.removeCloudClient(this);
	}

	@Override
	public boolean isConnected() {
		return this.dataService.isConnected();
	}

	@Override
	public int publish(String appTopic, KuraPayload payload, int qos, boolean retain) throws KuraException {
		return publish(appTopic, payload, qos, retain, 5);
	}

	@Override
	public int publish(String appTopic, KuraPayload payload, int qos, boolean retain, int priority)
			throws KuraException {
		byte[] appPayload = this.cloudServiceImpl.encodePayload(payload);
		return publish(appTopic, appPayload, qos, retain, priority);
	}

	@Override
	public int publish(String appTopic, byte[] payload, int qos, boolean retain, int priority) throws KuraException {
		AwsIotCloudServiceOptions options = this.cloudServiceImpl.getCloudServiceOptions();
		String fullTopic = encodeTopic(options.getTopicClientIdToken(), appTopic);
        return this.dataService.publish(fullTopic, payload, qos, retain, priority);
	}

	@Override
	public int controlPublish(String appTopic, KuraPayload payload, int qos, boolean retain, int priority)
			throws KuraException {
		throw new KuraException(KuraErrorCode.OPERATION_NOT_SUPPORTED, "Publishing control messages is not "
				+ "supported in this CloudClient implementation");
	}

	@Override
	public int controlPublish(String deviceId, String appTopic, KuraPayload payload, int qos, boolean retain,
			int priority) throws KuraException {
		throw new KuraException(KuraErrorCode.OPERATION_NOT_SUPPORTED, "Publishing control messages is not "
				+ "supported in this CloudClient implementation");
	}

	@Override
	public int controlPublish(String deviceId, String appTopic, byte[] payload, int qos, boolean retain, int priority)
			throws KuraException {
		throw new KuraException(KuraErrorCode.OPERATION_NOT_SUPPORTED, "Publishing control messages is not "
				+ "supported in this CloudClient implementation");
	}

	@Override
	public void subscribe(String topic, int qos) throws KuraException {
        String appTopic = encodeTopic(topic);
        this.dataService.subscribe(appTopic, qos);
	}

	@Override
	public void controlSubscribe(String appTopic, int qos) throws KuraException {
		throw new KuraException(KuraErrorCode.OPERATION_NOT_SUPPORTED, "Subscribing to control topics is not "
				+ "supported in this CloudClient implementation");
	}

	@Override
	public void unsubscribe(String topic) throws KuraException {
		String appTopic = encodeTopic(topic);
        this.dataService.unsubscribe(appTopic);
	}

	@Override
	public void controlUnsubscribe(String appTopic) throws KuraException {
		throw new KuraException(KuraErrorCode.OPERATION_NOT_SUPPORTED, "Subscribing to control topics is not "
				+ "supported in this CloudClient implementation");
	}

	@Override
	public void addCloudClientListener(CloudClientListener cloudClientListener) {
		this.listeners.add(new CloudClientListenerAdapter(cloudClientListener));
	}

	@Override
	public void removeCloudClientListener(CloudClientListener cloudClientListener) {
		// create a copy to avoid concurrent modification exceptions
        List<CloudClientListenerAdapter> adapters = new ArrayList<CloudClientListenerAdapter>(this.listeners);
        for (CloudClientListenerAdapter adapter : adapters) {
            if (adapter.getCloudClientListenerAdapted() == cloudClientListener) {
                this.listeners.remove(adapter);
                break;
            }
        }
	}

	@Override
	public List<Integer> getUnpublishedMessageIds() throws KuraException {
		String topicRegex = getAppTopicRegex();
        return this.dataService.getUnpublishedMessageIds(topicRegex);
	}

	@Override
	public List<Integer> getInFlightMessageIds() throws KuraException {
		String topicRegex = getAppTopicRegex();
        return this.dataService.getInFlightMessageIds(topicRegex);
	}

	@Override
	public List<Integer> getDroppedInFlightMessageIds() throws KuraException {
		String topicRegex = getAppTopicRegex();
        return this.dataService.getDroppedInFlightMessageIds(topicRegex);
	}

	private String encodeTopic(String topic) {
        AwsIotCloudServiceOptions options = this.cloudServiceImpl.getCloudServiceOptions();
        return encodeTopic(options.getTopicClientIdToken(), topic);
    }
	
	private String encodeTopic(String deviceId, String appTopic) {
        AwsIotCloudServiceOptions options = this.cloudServiceImpl.getCloudServiceOptions();
        StringBuilder sb = new StringBuilder();
        sb.append(options.getTopicAccountToken());
        if (useKuraTopicNamespace) {
        	sb.append(options.getTopicSeparator()).append(deviceId).append(options.getTopicSeparator()).append(this.applicationId);
        }
        if (appTopic != null && !appTopic.isEmpty()) {
            sb.append(options.getTopicSeparator()).append(appTopic);
        }
        return sb.toString();
    }
	
	private String getAppTopicRegex() {
        AwsIotCloudServiceOptions options = this.cloudServiceImpl.getCloudServiceOptions();
        StringBuilder sb = new StringBuilder();
        
        // String regexExample = "^(\\$EDC/)?eurotech/.+/conf-v1(/.+)?";
        
        // Optional control prefix
        sb.append("^")
                // .append("(").append(options.getTopicControlPrefix())
                //.append("\\$EDC").append(options.getTopicSeparator()).append(")?")
                
        .append(options.getTopicAccountToken());
        if (useKuraTopicNamespace) {
        	sb.append(options.getTopicSeparator()).append(".+") // Any device ID
                .append(options.getTopicSeparator()).append(this.applicationId);
        }
        sb.append("(").append(options.getTopicSeparator()).append(".+)?");
        
        return sb.toString();
    }
}
