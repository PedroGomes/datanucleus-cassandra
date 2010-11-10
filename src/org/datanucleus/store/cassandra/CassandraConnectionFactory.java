/**********************************************************************
Copyright (c) 2010 Pedro Gomes and Universidade do Minho.
(Based on datanucleus-hbase. Copyright (c) 2009 Erik Bengtson and others.)
All rights reserved.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

**********************************************************************/


package org.datanucleus.store.cassandra;

import java.util.Map;

import org.datanucleus.OMFContext;
import org.datanucleus.store.connection.AbstractConnectionFactory;
import org.datanucleus.store.connection.ManagedConnection;



public class CassandraConnectionFactory extends AbstractConnectionFactory{

	CassandraConnectionPool connectionPool;
	CassandraConnectionInfo connectionInfo;
	
	public CassandraConnectionFactory(OMFContext omfContext, String resourceType) {
		super(omfContext, resourceType);
		CassandraStoreManager storeManager = (CassandraStoreManager) omfContext.getStoreManager();
		connectionPool = new CassandraConnectionPool();
		connectionInfo = storeManager.getConnectionInfo();
        connectionPool.setTimeBetweenEvictionRunsMillis(storeManager.getPoolTimeBetweenEvictionRunsMillis());

	}

	@Override
	public ManagedConnection createManagedConnection(Object poolKey, Map arg1) {
			CassandraStoreManager storeManager = (CassandraStoreManager) omfContext.getStoreManager();

	        CassandraManagedConnection managedConnection = connectionPool.getPooledConnection();
	        if (managedConnection == null) 
	        {
	            managedConnection = new CassandraManagedConnection(connectionInfo);
	            managedConnection.setIdleTimeoutMills(storeManager.getPoolMinEvictableIdleTimeMillis());
	            connectionPool.registerConnection(managedConnection);
	        }
	        managedConnection.incrementReferenceCount();
	        return managedConnection;
	}

}
