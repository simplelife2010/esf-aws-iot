package de.db.i4i.esf.aws.iot;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.eclipse.kura.KuraErrorCode;
import org.eclipse.kura.KuraException;
import org.eclipse.kura.cloud.factory.CloudServiceFactory;
import org.eclipse.kura.configuration.ComponentConfiguration;
import org.eclipse.kura.configuration.ConfigurationService;
import org.osgi.service.component.ComponentConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AwsIotCloudServiceFactory implements CloudServiceFactory {
	
	private static final Logger logger = LoggerFactory.getLogger(AwsIotCloudServiceFactory.class);
	
	private static final String FACTORY_PID = "de.db.i4i.esf.aws.iot.AwsIotCloudServiceFactory";
    private static final String CLOUD_SERVICE_FACTORY_PID = "de.db.i4i.esf.aws.iot.AwsIotCloudService";
    private static final String DATA_SERVICE_FACTORY_PID = "org.eclipse.kura.data.DataService";
    private static final String DATA_TRANSPORT_SERVICE_FACTORY_PID = "org.eclipse.kura.core.data.transport.mqtt.MqttDataTransport";

    private static final String CLOUD_SERVICE_PID = "de.db.i4i.esf.aws.iot.AwsIotCloudService";
    private static final String DATA_SERVICE_PID = "org.eclipse.kura.data.DataService";
    private static final String DATA_TRANSPORT_SERVICE_PID = "org.eclipse.kura.core.data.transport.mqtt.MqttDataTransport";

    private static final String DATA_SERVICE_REFERENCE_NAME = "DataService";
    private static final String DATA_TRANSPORT_SERVICE_REFERENCE_NAME = "DataTransportService";
    
    private static final String REFERENCE_TARGET_VALUE_FORMAT = "(" + ConfigurationService.KURA_SERVICE_PID + "=%s)";

	private ConfigurationService configurationService;

    protected void setConfigurationService(ConfigurationService configurationService) {
        this.configurationService = configurationService;
    }

    protected void unsetConfigurationService(ConfigurationService configurationService) {
        if (configurationService == this.configurationService) {
            this.configurationService = null;
        }
    }
	
	@Override
	public String getFactoryPid() {
		logger.info("getFactoryPid() returns {}", CLOUD_SERVICE_FACTORY_PID);
        return CLOUD_SERVICE_FACTORY_PID;
    }

	@Override
	public void createConfiguration(String pid) throws KuraException {
		logger.info("createConfiguration({})", pid);
        String[] parts = pid.split("-");
        if (parts.length != 0 && CLOUD_SERVICE_PID.equals(parts[0])) {
            String suffix = null;
            if (parts.length > 1) {
                suffix = parts[1];
            }

            String dataServicePid = DATA_SERVICE_PID;
            String dataTransportServicePid = DATA_TRANSPORT_SERVICE_PID;
            if (suffix != null) {
                dataServicePid += "-" + suffix;
                dataTransportServicePid += "-" + suffix;
            }

            // create the CloudService layer and set the selective dependency on the DataService PID
            Map<String, Object> cloudServiceProperties = new HashMap<String, Object>();
            String name = DATA_SERVICE_REFERENCE_NAME + ComponentConstants.REFERENCE_TARGET_SUFFIX;
            cloudServiceProperties.put(name, String.format(REFERENCE_TARGET_VALUE_FORMAT, dataServicePid));
            cloudServiceProperties.put(KURA_CLOUD_SERVICE_FACTORY_PID, FACTORY_PID);

            this.configurationService.createFactoryConfiguration(CLOUD_SERVICE_FACTORY_PID, pid, cloudServiceProperties,
                    false);

            // create the DataService layer and set the selective dependency on the DataTransportService PID
            Map<String, Object> dataServiceProperties = new HashMap<String, Object>();
            name = DATA_TRANSPORT_SERVICE_REFERENCE_NAME + ComponentConstants.REFERENCE_TARGET_SUFFIX;
            dataServiceProperties.put(name, String.format(REFERENCE_TARGET_VALUE_FORMAT, dataTransportServicePid));

            this.configurationService.createFactoryConfiguration(DATA_SERVICE_FACTORY_PID, dataServicePid,
                    dataServiceProperties, false);

            // create the DataTransportService layer and take a snapshot
            this.configurationService.createFactoryConfiguration(DATA_TRANSPORT_SERVICE_FACTORY_PID,
                    dataTransportServicePid, null, true);
        } else {
            throw new KuraException(KuraErrorCode.INVALID_PARAMETER, "Invalid PID '{}'", pid);
        }
    }

	@Override
	public List<String> getStackComponentsPids(String pid) throws KuraException {
        List<String> componentPids = new ArrayList<String>();
        String[] parts = pid.split("-");
        if (parts.length != 0 && CLOUD_SERVICE_PID.equals(parts[0])) {
            String suffix = null;
            if (parts.length > 1) {
                suffix = parts[1];
            }

            String dataServicePid = DATA_SERVICE_PID;
            String dataTransportServicePid = DATA_TRANSPORT_SERVICE_PID;
            if (suffix != null) {
                dataServicePid += "-" + suffix;
                dataTransportServicePid += "-" + suffix;
            }

            componentPids.add(pid);
            componentPids.add(dataServicePid);
            componentPids.add(dataTransportServicePid);
            logger.info("getStackComponentPids({}) returns ...", pid);
            for (String resultPid : componentPids) {
            	logger.info("... {}", resultPid);
            }
            return componentPids;
        } else {
            throw new KuraException(KuraErrorCode.INVALID_PARAMETER, "Invalid PID '{}'", pid);
        }
    }

	@Override
	public void deleteConfiguration(String pid) throws KuraException {
		logger.info("deleteConfiguration({})", pid);
        String[] parts = pid.split("-");
        if (parts.length != 0 && CLOUD_SERVICE_PID.equals(parts[0])) {
            String suffix = null;
            if (parts.length > 1) {
                suffix = parts[1];
            }

            String dataServicePid = DATA_SERVICE_PID;
            String dataTransportServicePid = DATA_TRANSPORT_SERVICE_PID;
            if (suffix != null) {
                dataServicePid += "-" + suffix;
                dataTransportServicePid += "-" + suffix;
            }

            this.configurationService.deleteFactoryConfiguration(pid, false);
            this.configurationService.deleteFactoryConfiguration(dataServicePid, false);
            this.configurationService.deleteFactoryConfiguration(dataTransportServicePid, true);
        }
    }

	@Override
	public Set<String> getManagedCloudServicePids() throws KuraException {
        Set<String> result = new HashSet<String>();

        for (ComponentConfiguration cc : this.configurationService.getComponentConfigurations()) {
            if (CLOUD_SERVICE_PID.equals(cc.getDefinition().getId())) {
                result.add(cc.getPid());
            }
        }

        logger.info("getManagedCloudServicePids() returns...");
        for (String resultPid : result) {
        	logger.info("... {}", resultPid);
        }
        return result;
    }
}
