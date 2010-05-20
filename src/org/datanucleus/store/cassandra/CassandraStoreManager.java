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

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.OMFContext;
import org.datanucleus.PersistenceConfiguration;
import org.datanucleus.metadata.MetaDataListener;
import org.datanucleus.store.AbstractStoreManager;

        
public class CassandraStoreManager extends AbstractStoreManager{

	private boolean autoCreateSchema;
	
	private MetaDataListener metadataListener;
	private CassandraConnectionInfo connectionInfo;
	
    private int poolTimeBetweenEvictionRunsMillis; 
    private int poolMinEvictableIdleTimeMillis;
	
    private String replicaPlacementStrategy;
    private int replicationFactor;
    private String endPointSnitch;
    
	public CassandraStoreManager(ClassLoaderResolver clr,
			OMFContext omfContext) {
		super("cassandra",clr, omfContext);
	
		PersistenceConfiguration conf = omfContext.getPersistenceConfiguration();
		
		if(connectionInfo==null){
			connectionInfo = new CassandraConnectionInfo(conf); 
		}
		
		this.persistenceHandler2 = new CassandraPersistenceHandler(this);
		
		metadataListener = new CassandraMetaDataListener(this);
        omfContext.getMetaDataManager().registerListener(metadataListener);
		
		//Maybe in Cassandra 0.7 this will be dynamic
		autoCreateSchema = conf.getBooleanProperty("datanucleus.autoCreateSchema");
		replicaPlacementStrategy = conf.getStringProperty("datanucleus.cassandra.replicaPlacementStrategy");
		replicationFactor = conf.getIntProperty("datanucleus.cassandra.replicationFactor");
		endPointSnitch = conf.getStringProperty("datanucleus.cassandra.endPointSnitch");
		
        poolTimeBetweenEvictionRunsMillis = conf.getIntProperty("datanucleus.connectionPool.timeBetweenEvictionRunsMillis");
        if (poolTimeBetweenEvictionRunsMillis == 0)
        {
            poolTimeBetweenEvictionRunsMillis = 15 * 1000; // default, 15 secs
        }
         
        // how long may a connection sit idle in the pool before it may be evicted
        poolMinEvictableIdleTimeMillis = conf.getIntProperty("datanucleus.connectionPool.minEvictableIdleTimeMillis");
        if (poolMinEvictableIdleTimeMillis == 0)
        {
            poolMinEvictableIdleTimeMillis = 30 * 1000; // default, 30 secs
        }
		
		logConfiguration();
	}

	@Override
	protected void registerConnectionFactory() {
		super.registerConnectionFactory();
		this.connectionMgr.disableConnectionPool();
	}
	
	public boolean isAutoCreateSchema() {
		return autoCreateSchema;
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		omfContext.getMetaDataManager().deregisterListener(metadataListener);
		super.close();
	}
	
    public CassandraConnectionInfo getConnectionInfo() {
    	if(connectionInfo==null){
    		PersistenceConfiguration conf = omfContext.getPersistenceConfiguration();
    		connectionInfo =  new CassandraConnectionInfo(conf);	
    	}	
		return connectionInfo;
	}

	public int getPoolMinEvictableIdleTimeMillis()
    {
        return poolMinEvictableIdleTimeMillis;
    }
    
    public int getPoolTimeBetweenEvictionRunsMillis()
    {
        return poolTimeBetweenEvictionRunsMillis;
    }

	public String getReplicaPlacementStrategy() {
		return replicaPlacementStrategy;
	}

	public int getReplicationFactor() {
		return replicationFactor;
	}

	public String getEndPointSnitch() {
		return endPointSnitch;
	}
    
	
    
}
