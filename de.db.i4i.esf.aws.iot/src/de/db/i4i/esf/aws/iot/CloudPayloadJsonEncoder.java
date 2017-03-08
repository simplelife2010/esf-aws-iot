package de.db.i4i.esf.aws.iot;

import org.eclipse.kura.KuraException;
import org.eclipse.kura.message.KuraPayload;

public interface CloudPayloadJsonEncoder {
	byte[] getBytes(KuraPayload kuraPayload) throws KuraException;
}
