package org.opencloudb.mpp;

/*
 * Copyright (c) 2013, OpenCloudDB/MyCAT and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software;Designed and Developed mainly by many Chinese 
 * opensource volunteers. you can redistribute it and/or modify it under the 
 * terms of the GNU General Public License version 2 only, as published by the
 * Free Software Foundation.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 * 
 * Any questions about this component can be directed to it's project Web address 
 * https://code.google.com/p/opencloudb/.
 *
 */

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;
import org.opencloudb.MycatServer;
import org.opencloudb.mpp.tmp.RowDataSorter;
import org.opencloudb.mysql.BufferUtil;
import org.opencloudb.mysql.nio.handler.MultiNodeQueryHandler;
import org.opencloudb.net.mysql.EOFPacket;
import org.opencloudb.net.mysql.RowDataPacket;
import org.opencloudb.route.RouteResultset;
import org.opencloudb.server.NonBlockingSession;
import org.opencloudb.server.ServerConnection;
import org.opencloudb.util.StringUtil;

/**
 * Data merge service handle data Min,Max,AVG group 、order by 、limit
 * 
 * @author wuzhih /modify by coder_czp/2015/11/2
 * 
 * Fixbug: mycat sql timeout and hang problem.
 * @author Uncle-pan
 * @since 2016-03-23
 * 
 */
public class DataMergeService implements Runnable {

	// 保存包和节点的关系
	class PackWraper {
		public byte[] data;
		public String node;

	}

	// A flag that represents whether the service running in a business thread,
	// should not a volatile variable!!
	// @author Uncle-pan
	// @since 2016-03-23
	private final AtomicBoolean running = new AtomicBoolean(false);
	// props
	private int fieldCount;
	private RouteResultset rrs;
	private RowDataSorter sorter;
	private RowDataPacketGrouper grouper;
	private MultiNodeQueryHandler multiQueryHandler;
	public PackWraper END_FLAG_PACK = new PackWraper();
	private List<RowDataPacket> result = new Vector<RowDataPacket>();
	private static Logger LOGGER = Logger.getLogger(DataMergeService.class);
	private BlockingQueue<PackWraper> packs = new LinkedBlockingQueue<PackWraper>();
	private ConcurrentHashMap<String, Boolean> canDiscard = new ConcurrentHashMap<String, Boolean>();

	public DataMergeService(MultiNodeQueryHandler handler, RouteResultset rrs) {
		this.rrs = rrs;
		this.multiQueryHandler = handler;
	}

	public RouteResultset getRrs() {
		return this.rrs;
	}

	public void outputMergeResult(NonBlockingSession session, byte[] eof) {
		addPack(END_FLAG_PACK);
	}

	public void onRowMetaData(Map<String, ColMeta> columToIndx, int fieldCount) {
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("field metadata inf:" + columToIndx.entrySet());
		}
		int[] groupColumnIndexs = null;
		this.fieldCount = fieldCount;
		if (rrs.getGroupByCols() != null) {
			groupColumnIndexs = toColumnIndex(rrs.getGroupByCols(), columToIndx);
		}

		if (rrs.getHavingCols() != null) {
			ColMeta colMeta = columToIndx.get(rrs.getHavingCols().getLeft()
					.toUpperCase());
			if (colMeta != null) {
				rrs.getHavingCols().setColMeta(colMeta);
			}
		}

		if (rrs.isHasAggrColumn()) {
			List<MergeCol> mergCols = new LinkedList<MergeCol>();
			Map<String, Integer> mergeColsMap = rrs.getMergeCols();
			if (mergeColsMap != null) {
				for (Map.Entry<String, Integer> mergEntry : mergeColsMap
						.entrySet()) {
					String colName = mergEntry.getKey().toUpperCase();
					int type = mergEntry.getValue();
					if (MergeCol.MERGE_AVG == type) {
						ColMeta sumColMeta = columToIndx.get(colName + "SUM");
						ColMeta countColMeta = columToIndx.get(colName
								+ "COUNT");
						if (sumColMeta != null && countColMeta != null) {
							ColMeta colMeta = new ColMeta(sumColMeta.colIndex,
									countColMeta.colIndex,
									sumColMeta.getColType());
							colMeta.decimals = sumColMeta.decimals; // 保存精度
							mergCols.add(new MergeCol(colMeta, mergEntry
									.getValue()));
						}
					} else {
						ColMeta colMeta = columToIndx.get(colName);
						mergCols.add(new MergeCol(colMeta, mergEntry.getValue()));
					}
				}
			}
			// add no alias merg column
			for (Map.Entry<String, ColMeta> fieldEntry : columToIndx.entrySet()) {
				String colName = fieldEntry.getKey();
				int result = MergeCol.tryParseAggCol(colName);
				if (result != MergeCol.MERGE_UNSUPPORT
						&& result != MergeCol.MERGE_NOMERGE) {
					mergCols.add(new MergeCol(fieldEntry.getValue(), result));
				}
			}
			grouper = new RowDataPacketGrouper(groupColumnIndexs,
					mergCols.toArray(new MergeCol[mergCols.size()]),
					rrs.getHavingCols());
		}
		if (rrs.getOrderByCols() != null) {
			LinkedHashMap<String, Integer> orders = rrs.getOrderByCols();
			OrderCol[] orderCols = new OrderCol[orders.size()];
			int i = 0;
			for (Map.Entry<String, Integer> entry : orders.entrySet()) {
				String key = StringUtil.removeBackquote(entry.getKey()
						.toUpperCase());
				ColMeta colMeta = columToIndx.get(key);
				if (colMeta == null) {
					throw new java.lang.IllegalArgumentException(
							"all columns in order by clause should be in the selected column list!"
									+ entry.getKey());
				}
				orderCols[i++] = new OrderCol(colMeta, entry.getValue());
			}
			// sorter = new RowDataPacketSorter(orderCols);
			RowDataSorter tmp = new RowDataSorter(orderCols);
			tmp.setLimit(rrs.getLimitStart(), rrs.getLimitSize());
			sorter = tmp;
		}
	}

	/**
	 * process new record (mysql binary data),if data can output to client
	 * ,return true
	 * 
	 * @param dataNode
	 *            DN's name (data from this dataNode)
	 * @param rowData
	 *            raw data
	 * @param conn
	 */
	public boolean onNewRecord(String dataNode, byte[] rowData) {
		// 对于需要排序的数据,由于mysql传递过来的数据是有序的,
		// 如果某个节点的当前数据已经不会进入,后续的数据也不会入堆
		if (canDiscard.size() == rrs.getNodes().length) {
			// "END_FLAG" only should be added by MultiNodeHandler.rowEofResponse()
			// @author Uncle-pan
			// @since 2016-03-23
			//LOGGER.info("now we output to client");
			//addPack(END_FLAG_PACK);
			return true;
		}
		if (canDiscard.get(dataNode) != null) {
			// 该node已经处理完毕了，比如order by desc，如果当前node的前一行已经小于现有结果的最小值，那么后续的行都不需要再处理了，会把node加入到canDiscard中
			return true;
		}
		final PackWraper data = new PackWraper();
		data.node = dataNode;
		data.data = rowData;
		addPack(data);
		return false;
	}

	private static int[] toColumnIndex(String[] columns,
			Map<String, ColMeta> toIndexMap) {
		int[] result = new int[columns.length];
		ColMeta curColMeta;
		for (int i = 0; i < columns.length; i++) {
			curColMeta = toIndexMap.get(columns[i].toUpperCase());
			if (curColMeta == null) {
				throw new java.lang.IllegalArgumentException(
						"all columns in group by clause should be in the selected column list.!"
								+ columns[i]);
			}
			result[i] = curColMeta.colIndex;
		}
		return result;
	}

	/**
	 * release resources
	 */
	public void clear() {
		result.clear();
		grouper = null;
		sorter = null;
	}

	@Override
	public void run() {
		// sort-or-group: no need for us to using multi-threads, because
		//both sorter and grouper are synchronized!!
		// @author Uncle-pan
		// @since 2016-03-23
		if(!running.compareAndSet(false, true)){
			return;
		}
		// eof handler has been placed to "if (pack == END_FLAG_PACK){}" in for-statement
		// @author Uncle-pan
		// @since 2016-03-23
		boolean nulpack = false;
		try{
			// loop-on-packs
			for (; ; ) {
				final PackWraper pack = packs.poll();
				// async: handling row pack queue, this business thread should exit when no pack
				// @author Uncle-pan
				// @since 2016-03-23
				if(pack == null){
					nulpack = true;
					break;
				}
				// eof: handling eof pack and exit
				if (pack == END_FLAG_PACK) {// 输出eof结束符
					final int warningCount = 0;
					final EOFPacket eofp   = new EOFPacket();
					final ByteBuffer eof   = ByteBuffer.allocate(9);
					BufferUtil.writeUB3(eof, eofp.calcPacketSize());
					eof.put(eofp.packetId);
					eof.put(eofp.fieldCount);
					BufferUtil.writeUB2(eof, warningCount);
					BufferUtil.writeUB2(eof, eofp.status);
					final ServerConnection source = multiQueryHandler.getSession().getSource();
					final byte[] array = eof.array();
					multiQueryHandler.outputMergeResult(source, array, getResults(array));
					break;
				}
				// merge: sort-or-group, or simple add
				final RowDataPacket row = new RowDataPacket(fieldCount);
				row.read(pack.data);
				if (grouper != null) {// 处理group by
					grouper.addRow(row);
				} else if (sorter != null) {
					// 处理order by：最大堆，堆大小=offset+limit，意味着每个节点都查offset+limit的数据进行堆排序
					// 这种做法能保证准确性，但是性能随着offset在增大而严重下降
					if (!sorter.addRow(row)) {
						// 由于是从各节点查询出来已经有序，如果当前行不能入堆，说明后续所有行都不能入堆
						// 将这个node加入canDiscard列表，说明这个node的结果已经取完，后续还有结果也不会处理了
						canDiscard.put(pack.node, true);
					}
				} else {
					result.add(row);// 直接将结果汇总
				}
			}// rof
		}catch(final Exception e){
			multiQueryHandler.handleDataProcessException(e);
		}finally{
			running.set(false);
		}
		// try to check packs, it's possible that adding a pack after polling a null pack
		//and before this time pointer!!
		// @author Uncle-pan
		// @since 2016-03-23
		if(nulpack && !packs.isEmpty()){
			this.run();
		}
	}
	
	/**
	 * Add a row pack, and may be wake up a business thread to work if not running.
	 * @param pack row pack
	 * @return true wake up a business thread, otherwise false
	 * 
	 * @author Uncle-pan
	 * @since 2016-03-23
	 */
	private final boolean addPack(final PackWraper pack){
		packs.add(pack);
		if(running.get()){
			return false;
		}
		final MycatServer server = MycatServer.getInstance();
		server.getBusinessExecutor().execute(this);// 异步处理
		return true;
	}

	/**
	 * return merged data
	 * 
	 * @return (最多i*(offset+size)行数据)
	 */
	private List<RowDataPacket> getResults(byte[] eof) {
		List<RowDataPacket> tmpResult = result;
		if (this.grouper != null) {
			tmpResult = grouper.getResult();
			grouper = null;
		}
		if (sorter != null) {
			// 处理grouper处理后的数据
			if (tmpResult != null) {
				Iterator<RowDataPacket> itor = tmpResult.iterator();
				while (itor.hasNext()) {
					sorter.addRow(itor.next());
					itor.remove();
				}
			}
			tmpResult = sorter.getSortedResult();
			sorter = null;
		}
		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("prepare mpp merge result for " + rrs.getStatement());
		}

		return tmpResult;
	}
}
