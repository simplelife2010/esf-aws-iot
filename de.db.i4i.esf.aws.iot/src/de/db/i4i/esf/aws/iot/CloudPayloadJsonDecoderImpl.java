package de.db.i4i.esf.aws.iot;

import org.eclipse.kura.KuraInvalidMessageException;
import org.eclipse.kura.message.KuraPayload;

import com.fasterxml.jackson.databind.ObjectMapper;

public class CloudPayloadJsonDecoderImpl {
	
	private byte[] bytes;

    public CloudPayloadJsonDecoderImpl(byte[] bytes) {
        this.bytes = bytes;
    }
    
	public KuraPayload buildFromByteArray() throws KuraInvalidMessageException {
		ObjectMapper mapper = new ObjectMapper();
		KuraPayload kuraPayload;
		try {
			KuraPayload2Json kuraPayload2Json = mapper.readValue(bytes, KuraPayload2Json.class);
			kuraPayload = kuraPayload2Json.convertToKuraPayload();
		} catch (Exception e) {
			throw new KuraInvalidMessageException(e);
		}
		return kuraPayload;
	}
	
}
