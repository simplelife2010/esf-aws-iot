package de.db.i4i.esf.aws.iot;

public class AwsIotCloudServiceOptions {

	private static final String TOPIC_SEPARATOR = "/";
	private static final String TOPIC_ACCOUNT_TOKEN = "#account-name";
	private static final String TOPIC_CLIENT_ID_TOKEN = "#client-id";
	
	private static final int LIFECYCLE_QOS = 0;
	
	public String getTopicSeparator() {
        return TOPIC_SEPARATOR;
    }
	
	public String getTopicAccountToken() {
        return TOPIC_ACCOUNT_TOKEN;
    }
	
	public String getTopicClientIdToken() {
        return TOPIC_CLIENT_ID_TOKEN;
    }
	
	public int getLifeCycleMessageQos() {
        return LIFECYCLE_QOS;
    }
}
