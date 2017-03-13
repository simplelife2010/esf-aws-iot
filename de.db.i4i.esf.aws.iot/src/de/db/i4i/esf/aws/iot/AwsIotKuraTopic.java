package de.db.i4i.esf.aws.iot;

public class AwsIotKuraTopic {

    private String fullTopic;
    private String[] topicParts;
    private String accountName = "";
    private String applicationTopic = "";
    
    public AwsIotKuraTopic(String fullTopic) {
        this.fullTopic = fullTopic;
        if (fullTopic.compareTo("#") == 0) {
            return;
        }

        this.topicParts = fullTopic.split("/");
        if (this.topicParts.length == 0) {
            return;
        }

        // prefix
        int index = 0;
        int offset = 0; // skip a slash

        // account name
        this.accountName = this.topicParts[index];
        offset += this.accountName.length() + 1;
        index++;

        // applicationTopic
        if (offset < this.fullTopic.length()) {
            this.applicationTopic = this.fullTopic.substring(offset);
        }
    }

    public String getFullTopic() {
        return this.fullTopic;
    }

    public String[] getTopicParts() {
        return this.topicParts;
    }

    public String getAccountName() {
        return this.accountName;
    }

    public String getApplicationTopic() {
        return this.applicationTopic;
    }
}
