package org.opencloudb.net;

import java.io.IOException;

/**
 * 网络数据读写处理
 */
public abstract class SocketWR {
	public abstract void asynRead() throws IOException;
	public abstract void doNextWriteCheck() ;
}
