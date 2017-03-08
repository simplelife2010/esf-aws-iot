package de.db.i4i.esf.aws.iot;

import java.util.List;

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
    
    private boolean useKuraTopicNamespace = false;

    protected AwsIotCloudClientImpl(String applicationId, DataService dataService, AwsIotCloudServiceImpl cloudServiceImpl) {
        this.applicationId = applicationId;
        this.dataService = dataService;
        this.cloudServiceImpl = cloudServiceImpl;
    }
	
	@Override
	public void onControlMessageArrived(String deviceId, String appTopic, KuraPayload msg, int qos, boolean retain) {
		// TODO Auto-generated method stub

	}

	@Override
	public void onMessageArrived(String deviceId, String appTopic, KuraPayload msg, int qos, boolean retain) {
		// TODO Auto-generated method stub

	}

	@Override
	public void onConnectionLost() {
		// TODO Auto-generated method stub

	}

	@Override
	public void onConnectionEstablished() {
		// TODO Auto-generated method stub

	}

	@Override
	public void onMessageConfirmed(int messageId, String appTopic) {
		// TODO Auto-generated method stub

	}

	@Override
	public void onMessagePublished(int messageId, String appTopic) {
		// TODO Auto-generated method stub

	}

	@Override
	public String getApplicationId() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void release() {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean isConnected() {
		// TODO Auto-generated method stub
		return false;
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
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int controlPublish(String deviceId, String appTopic, KuraPayload payload, int qos, boolean retain,
			int priority) throws KuraException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int controlPublish(String deviceId, String appTopic, byte[] payload, int qos, boolean retain, int priority)
			throws KuraException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void subscribe(String appTopic, int qos) throws KuraException {
		// TODO Auto-generated method stub

	}

	@Override
	public void controlSubscribe(String appTopic, int qos) throws KuraException {
		// TODO Auto-generated method stub

	}

	@Override
	public void unsubscribe(String appTopic) throws KuraException {
		// TODO Auto-generated method stub

	}

	@Override
	public void controlUnsubscribe(String appTopic) throws KuraException {
		// TODO Auto-generated method stub

	}

	@Override
	public void addCloudClientListener(CloudClientListener cloudClientListener) {
		// TODO Auto-generated method stub

	}

	@Override
	public void removeCloudClientListener(CloudClientListener cloudClientListener) {
		// TODO Auto-generated method stub

	}

	@Override
	public List<Integer> getUnpublishedMessageIds() throws KuraException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<Integer> getInFlightMessageIds() throws KuraException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<Integer> getDroppedInFlightMessageIds() throws KuraException {
		// TODO Auto-generated method stub
		return null;
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
}
