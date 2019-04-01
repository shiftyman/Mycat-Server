package org.opencloudb.net;

/**
 * 连接接收类，监听管理员和应用的连接请求
 */
public interface SocketAcceptor {

	void start();

	String getName();

	int getPort();

}
