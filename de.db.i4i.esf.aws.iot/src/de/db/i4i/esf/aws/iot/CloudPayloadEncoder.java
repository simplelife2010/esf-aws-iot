package de.db.i4i.esf.aws.iot;

import java.io.IOException;

public interface CloudPayloadEncoder {
	public byte[] getBytes() throws IOException;
}
