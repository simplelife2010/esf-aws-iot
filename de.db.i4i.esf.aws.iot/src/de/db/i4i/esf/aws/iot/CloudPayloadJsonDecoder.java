package de.db.i4i.esf.aws.iot;

import org.eclipse.kura.KuraException;
import org.eclipse.kura.message.KuraPayload;

public interface CloudPayloadJsonDecoder {
	public KuraPayload buildFromByteArray(byte[] payload) throws KuraException;
}
