package org.opencloudb.net;

import java.io.IOException;

/**
 * �������ݶ�д����
 */
public abstract class SocketWR {
	public abstract void asynRead() throws IOException;
	public abstract void doNextWriteCheck() ;
}
