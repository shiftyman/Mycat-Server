package org.opencloudb.mysql.nio.handler;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.opencloudb.backend.BackendConnection;
import org.opencloudb.route.RouteResultsetNode;
import org.opencloudb.server.NonBlockingSession;
import org.opencloudb.sqlcmd.SQLCtrlCommand;

public class MultiNodeCoordinator implements ResponseHandler {
	private static final Logger LOGGER = Logger
			.getLogger(MultiNodeCoordinator.class);
	private final AtomicInteger runningCount = new AtomicInteger(0);
	private final AtomicInteger faileCount = new AtomicInteger(0);
	private volatile int nodeCount;
	private final NonBlockingSession session;
	private SQLCtrlCommand cmdHandler;
	private final AtomicBoolean failed = new AtomicBoolean(false);

	public MultiNodeCoordinator(NonBlockingSession session) {
		this.session = session;
	}

	public void executeBatchNodeCmd(SQLCtrlCommand cmdHandler) {
		this.cmdHandler = cmdHandler;
		final int initCount = session.getTargetCount();
		runningCount.set(initCount);
		nodeCount = initCount;
		failed.set(false);
		faileCount.set(0);
		// 执行
		int started = 0;

		// 逐个发送commit命令
		for (RouteResultsetNode rrn : session.getTargetKeys()) {
			if (rrn == null) {
				LOGGER.error("null is contained in RoutResultsetNodes, source = "
						+ session.getSource());
				continue;
			}
			final BackendConnection conn = session.getTarget(rrn);
			if (conn != null) {
				// 将数据源的响应handler设置为自身
				// 后续有响应时会调用okResponse或errorResponse方法
				conn.setResponseHandler(this);
				cmdHandler.sendCommand(session, conn);//发送commit命令
				++started;
			}
		}

		// 部分node获取链接失败，打印日志并且设置failed=true（但并没有使用）
		// 相当于部分node失败是不处理的（弱XA）
		if (started < nodeCount) {
			runningCount.set(started);
			LOGGER.warn("some connection failed to execut "
					+ (nodeCount - started));
			/**
			 * assumption: only caused by front-end connection close. <br/>
			 * Otherwise, packet must be returned to front-end
			 */
			failed.set(true);
		}
	}

	private boolean finished() {
		int val = runningCount.decrementAndGet();
		return (val == 0);
	}

	@Override
	public void connectionError(Throwable e, BackendConnection conn) {
	}

	@Override
	public void connectionAcquired(BackendConnection conn) {

	}

	@Override
	public void errorResponse(byte[] err, BackendConnection conn) {
		faileCount.incrementAndGet();

		// 释放资源
		if (this.cmdHandler.releaseConOnErr()) {
			session.releaseConnection(conn);
		} else {
			session.releaseConnectionIfSafe(conn, LOGGER.isDebugEnabled(),
					false);
		}
		// finish表示这次是收到的最后一个响应，其他所有的running任务都已经响应过了
		// 此时要做的，就是响应client，commit命令的结果
		if (this.finished()) {
			cmdHandler.errorResponse(session, err, this.nodeCount,
					this.faileCount.get());// 响应client为失败
			if (cmdHandler.isAutoClearSessionCons()) {
				session.clearResources(session.getSource().isTxInterrupted());
			}
		}

	}

	@Override
	public void okResponse(byte[] ok, BackendConnection conn) {
		// 释放后端链接资源
		if (this.cmdHandler.relaseConOnOK()) {
			session.releaseConnection(conn);
		} else {
			session.releaseConnectionIfSafe(conn, LOGGER.isDebugEnabled(),
					false);
		}
		// finish表示这次是收到的最后一个响应，其他所有的running任务都已经响应过了
		// 此时要做的，就是响应client，commit命令的结果
		if (this.finished()) {
			cmdHandler.okResponse(session, ok);// 发送ok响应给client
			if (cmdHandler.isAutoClearSessionCons()) {
				session.clearResources(false);
			}

		}

	}

	@Override
	public void fieldEofResponse(byte[] header, List<byte[]> fields,
			byte[] eof, BackendConnection conn) {

	}

	@Override
	public void rowResponse(byte[] row, BackendConnection conn) {

	}

	@Override
	public void rowEofResponse(byte[] eof, BackendConnection conn) {
	}

	@Override
	public void writeQueueAvailable() {

	}

	@Override
	public void connectionClose(BackendConnection conn, String reason) {

	}

}
