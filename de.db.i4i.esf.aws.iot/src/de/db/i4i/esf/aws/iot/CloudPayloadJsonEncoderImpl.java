package de.db.i4i.esf.aws.iot;

import java.io.IOException;

import org.eclipse.kura.message.KuraPayload;

public class CloudPayloadJsonEncoderImpl implements CloudPayloadEncoder {

	private final KuraPayload kuraPayload;

    public CloudPayloadJsonEncoderImpl(KuraPayload kuraPayload) {
        this.kuraPayload = kuraPayload;
    }
	
	@Override
	public byte[] getBytes() throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

}
