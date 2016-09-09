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
package io.mycat.server.handler;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger; import org.slf4j.LoggerFactory;

import io.mycat.backend.mysql.BindValue;
import io.mycat.backend.mysql.ByteUtil;
import io.mycat.backend.mysql.PreparedStatement;
import io.mycat.config.ErrorCode;
import io.mycat.config.Fields;
import io.mycat.net.handler.FrontendPrepareHandler;
import io.mycat.net.mysql.ExecutePacket;
import io.mycat.net.mysql.LongDataPacket;
import io.mycat.net.mysql.OkPacket;
import io.mycat.net.mysql.ResetPacket;
import io.mycat.server.ServerConnection;
import io.mycat.server.response.PreparedStmtResponse;

/**
 * @author mycat, CrazyPig
 */
public class ServerPrepareHandler implements FrontendPrepareHandler {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(ServerPrepareHandler.class);
	
    private ServerConnection source;
    private volatile long pstmtId;
    private Map<String, PreparedStatement> pstmtForSql;
    private Map<Long, PreparedStatement> pstmtForId;

    public ServerPrepareHandler(ServerConnection source) {
        this.source = source;
        this.pstmtId = 0L;
        this.pstmtForSql = new HashMap<String, PreparedStatement>();
        this.pstmtForId = new HashMap<Long, PreparedStatement>();
    }

    @Override
    public void prepare(String sql) {
    	
    	LOGGER.debug("use server prepare, sql: " + sql);
        PreparedStatement pstmt = null;
        if ((pstmt = pstmtForSql.get(sql)) == null) {
        	// 解析获取字段个数和参数个数
        	int columnCount = getColumnCount(sql);
        	int paramCount = getParamCount(sql);
            pstmt = new PreparedStatement(++pstmtId, sql, columnCount, paramCount);
            pstmtForSql.put(pstmt.getStatement(), pstmt);
            pstmtForId.put(pstmt.getId(), pstmt);
        }
        PreparedStmtResponse.response(pstmt, source);
    }
    
    @Override
	public void sendLongData(byte[] data) {
		LongDataPacket packet = new LongDataPacket();
		packet.read(data);
		long pstmtId = packet.getPstmtId();
		PreparedStatement pstmt = pstmtForId.get(pstmtId);
		if(pstmt != null) {
			if(LOGGER.isDebugEnabled()) {
				LOGGER.debug("send long data to prepare sql : " + pstmtForId.get(pstmtId));
			}
			long paramId = packet.getParamId();
			try {
				pstmt.appendLongData(paramId, packet.getLongData());
			} catch (IOException e) {
				source.writeErrMessage(ErrorCode.ERR_FOUND_EXCEPION, e.getMessage());
			}
		}
	}

	@Override
	public void reset(byte[] data) {
		ResetPacket packet = new ResetPacket();
		packet.read(data);
		long pstmtId = packet.getPstmtId();
		PreparedStatement pstmt = pstmtForId.get(pstmtId);
		if(pstmt != null) {
			if(LOGGER.isDebugEnabled()) {
				LOGGER.debug("reset prepare sql : " + pstmtForId.get(pstmtId));
			}
			pstmt.resetLongData();
			source.write(OkPacket.OK);
		} else {
			source.writeErrMessage(ErrorCode.ERR_FOUND_EXCEPION, "can not reset prepare statement : " + pstmtForId.get(pstmtId));
		}
	} 
    
    @Override
    public void execute(byte[] data) {
        long pstmtId = ByteUtil.readUB4(data, 5);
        PreparedStatement pstmt = null;
        if ((pstmt = pstmtForId.get(pstmtId)) == null) {
            source.writeErrMessage(ErrorCode.ER_ERROR_WHEN_EXECUTING_COMMAND, "Unknown pstmtId when executing.");
        } else {
            ExecutePacket packet = new ExecutePacket(pstmt);
            try {
                packet.read(data, source.getCharset());
            } catch (UnsupportedEncodingException e) {
                source.writeErrMessage(ErrorCode.ER_ERROR_WHEN_EXECUTING_COMMAND, e.getMessage());
                return;
            }
            BindValue[] bindValues = packet.values;
            // 还原sql中的动态参数为实际参数值
            String sql = prepareStmtBindValue(pstmt, bindValues);
            // 执行sql
            source.getSession2().setPrepared(true);
            if(LOGGER.isDebugEnabled()) {
            	LOGGER.debug("execute prepare sql: " + sql);
            }
            source.query( sql );
        }
    }
    
    
    @Override
    public void close(byte[] data) {
    	long pstmtId = ByteUtil.readUB4(data, 5); // 获取prepare stmt id
    	if(LOGGER.isDebugEnabled()) {
    		LOGGER.debug("close prepare stmt, stmtId = " + pstmtId);
    	}
    	PreparedStatement pstmt = pstmtForId.remove(pstmtId);
    	if(pstmt != null) {
    		pstmtForSql.remove(pstmt.getStatement());
    	}
    }
    
    @Override
    public void clear() {
    	this.pstmtForId.clear();
    	this.pstmtForSql.clear();
    }
    
    // TODO 获取预处理语句中column的个数
    private int getColumnCount(String sql) {
    	int columnCount = 0;
    	// TODO ...
    	return columnCount;
    }
    
    // 获取预处理sql中预处理参数个数
    private int getParamCount(String sql) {
    	char[] cArr = sql.toCharArray();
    	int count = 0;
    	for(int i = 0; i < cArr.length; i++) {
    		if(cArr[i] == '?') {
    			count++;
    		}
    	}
    	return count;
    }
    
    /**
     * 组装sql语句,替换动态参数为实际参数值
     * @param pstmt
     * @param bindValues
     * @return
     */
    private String prepareStmtBindValue(PreparedStatement pstmt, BindValue[] bindValues) {
    	String sql = pstmt.getStatement();
    	int paramNumber = pstmt.getParametersNumber();
    	int[] paramTypes = pstmt.getParametersType();
    	for(int i = 0; i < paramNumber; i++) {
    		int paramType = paramTypes[i];
    		BindValue bindValue = bindValues[i];
    		if(bindValue.isNull) {
    			sql = sql.replaceFirst("\\?", "NULL");
    			continue;
    		}
    		switch(paramType & 0xff) {
    		case Fields.FIELD_TYPE_TINY:
    			sql = sql.replaceFirst("\\?", String.valueOf(bindValue.byteBinding));
    			break;
    		case Fields.FIELD_TYPE_SHORT:
    			sql = sql.replaceFirst("\\?", String.valueOf(bindValue.shortBinding));
    			break;
    		case Fields.FIELD_TYPE_LONG:
    			sql = sql.replaceFirst("\\?", String.valueOf(bindValue.intBinding));
    			break;
    		case Fields.FIELD_TYPE_LONGLONG:
    			sql = sql.replaceFirst("\\?", String.valueOf(bindValue.longBinding));
    			break;
    		case Fields.FIELD_TYPE_FLOAT:
    			sql = sql.replaceFirst("\\?", String.valueOf(bindValue.floatBinding));
    			break;
    		case Fields.FIELD_TYPE_DOUBLE:
    			sql = sql.replaceFirst("\\?", String.valueOf(bindValue.doubleBinding));
    			break;
    		case Fields.FIELD_TYPE_VAR_STRING:
            case Fields.FIELD_TYPE_STRING:
            case Fields.FIELD_TYPE_VARCHAR:
            case Fields.FIELD_TYPE_BLOB:
            	sql = sql.replaceFirst("\\?", "'" + bindValue.value + "'");
            	break;
            case Fields.FIELD_TYPE_TIME:
            case Fields.FIELD_TYPE_DATE:
            case Fields.FIELD_TYPE_DATETIME:
            case Fields.FIELD_TYPE_TIMESTAMP:
            	sql = sql.replaceFirst("\\?", "'" + bindValue.value + "'");
            	break;
            default:
            	sql = sql.replaceFirst("\\?", bindValue.value.toString());
            	break;
    		}
    	}
    	return sql;
    }

}