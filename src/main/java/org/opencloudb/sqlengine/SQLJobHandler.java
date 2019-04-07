package org.opencloudb.sqlengine;

import java.util.List;

import org.apache.log4j.Logger;

public interface SQLJobHandler {
	public static final Logger LOGGER = Logger.getLogger(SQLJobHandler.class);

	/**
	 * 写header。
	 *
	 * 见mysql报文协议，主要包含返回的各个fields的name
	 * @param dataNode
	 * @param header
	 * @param fields
	 */
	public void onHeader(String dataNode, byte[] header, List<byte[]> fields);

	/**
	 * 写实际的result set数据
	 * @param dataNode
	 * @param rowData
	 * @return
	 */
	public boolean onRowData(String dataNode, byte[] rowData);

	public void finished(String dataNode, boolean failed);
}
