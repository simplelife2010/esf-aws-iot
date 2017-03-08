package de.db.i4i.esf.aws.iot;

public class AwsIotCloudServiceOptions {

	private static final String TOPIC_SEPARATOR = "/";
	private static final String TOPIC_ACCOUNT_TOKEN = "#account-name";
	private static final String TOPIC_CLIENT_ID_TOKEN = "#client-id";
	
	public String getTopicSeparator() {
        return TOPIC_SEPARATOR;
    }
	
	public String getTopicAccountToken() {
        return TOPIC_ACCOUNT_TOKEN;
    }
	
	public String getTopicClientIdToken() {
        return TOPIC_CLIENT_ID_TOKEN;
    }
}
