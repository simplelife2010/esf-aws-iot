package de.db.i4i.esf.aws.iot;

import java.io.IOException;

import org.eclipse.kura.message.KuraPayload;

import com.fasterxml.jackson.databind.ObjectMapper;

public class CloudPayloadJsonEncoderImpl implements CloudPayloadEncoder {

	private final KuraPayload2Json payload;

    public CloudPayloadJsonEncoderImpl(KuraPayload kuraPayload) {
        this.payload = new KuraPayload2Json(kuraPayload);
    }
	
	@Override
	public byte[] getBytes() throws IOException {
		ObjectMapper mapper = new ObjectMapper();
		return mapper.writeValueAsBytes(payload);
	}

}
