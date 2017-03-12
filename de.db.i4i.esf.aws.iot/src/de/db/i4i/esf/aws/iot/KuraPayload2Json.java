package de.db.i4i.esf.aws.iot;

import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.eclipse.kura.message.KuraPayload;
import org.eclipse.kura.message.KuraPosition;

public class KuraPayload2Json {
	public Date sentOn;
	public KuraPosition position;
	public Map<String, Object> metrics;
	public byte[] body;
	
	public KuraPayload2Json(KuraPayload payload) {
		this.sentOn = payload.getTimestamp();
		this.position = payload.getPosition();
		this.metrics = new HashMap<String, Object>();
		String key;
		for ( Iterator<String> it = payload.metricsIterator(); it.hasNext(); ) {
			key = it.next();
			this.metrics.put(key, payload.getMetric(key));
		}
		this.body = payload.getBody();
	}
	
	public KuraPayload getKuraPayload() {
		KuraPayload kuraPayload = new KuraPayload();
		kuraPayload.setTimestamp(sentOn);
		kuraPayload.setPosition(position);
		String key;
		for ( Iterator<String> it = metrics.keySet().iterator(); it.hasNext(); ) {
			key = it.next();
			kuraPayload.addMetric(key, metrics.get(key));
		}
		kuraPayload.setBody(body);
		return kuraPayload;
	}
}
