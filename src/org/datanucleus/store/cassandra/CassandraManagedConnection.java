/**********************************************************************
Copyright (c) 2010 Pedro Gomes and Minho University . All rights reserved.
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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.transaction.xa.XAResource;

import org.apache.cassandra.thrift.Cassandra.Client;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.store.connection.AbstractManagedConnection;
import org.datanucleus.store.connection.ManagedConnectionResourceListener;

public class CassandraManagedConnection extends AbstractManagedConnection {

	private CassandraConnectionInfo connectionInfo;

	private List<Client> cassandraClients;

	private int referenceCount = 0;

	private int idleTimeoutMills = 30 * 1000; // 30 secs

	private long expirationTime;

	private int last_client = 0;

	private boolean isDisposed = false;

	public CassandraManagedConnection(CassandraConnectionInfo info) {
		connectionInfo = info;
		disableExpirationTime();
		cassandraClients = new ArrayList<Client>();
	}

	@Override
	public void close() {

		for (int i = 0; i < listeners.size(); i++) {
			((ManagedConnectionResourceListener) listeners.get(i))
					.managedConnectionPreClose();
		}
		try {
//			for (Client client : cassandraClients) {
//				client.getOutputProtocol().getTransport().close();
//			}
		} finally {
			for (int i = 0; i < listeners.size(); i++) {
				((ManagedConnectionResourceListener) listeners.get(i))
						.managedConnectionPostClose();
			}
		}

	}

	@Override
	public Object getConnection() {

		if (cassandraClients.isEmpty()) {
			establishNodeConnection(); 
		}
		Client client = getCassandraClient();
		if (client == null) {
			throw new NucleusDataStoreException("Connection error, no available nodes");
		}
		
		return client;
	}

	public Client getCassandraClient() {

		boolean openClient = false;
		Client cl = null;

		while (!openClient) { // until there is no one open

			if (!cassandraClients.isEmpty()) { // if none, then null...
				cl = cassandraClients.get(last_client);
				if (!cl.getInputProtocol().getTransport().isOpen()) {
					cassandraClients.remove(last_client);
				} else {
					openClient = true;
				}
				last_client++;
				last_client = last_client >= cassandraClients.size() ? 0
						: last_client;

			} else {
				openClient = true;
			}
		}
		return cl;

	}
	
	/**
	 * Establish connections to all nodes.
	 * TODO maybe it would be best if connections were made only when necessary, i.e., established in the round robin section.
	 * Trade-off : N connections vs connection establishment time. 
	 * */
	public void establishNodeConnection() {

		Map<String,Integer> connections = connectionInfo.getConnections();
		for (String host : connections.keySet()) {
		  int port = -1;
          try {
        	  port = connections.get(host);
              TSocket socket = new TSocket(host, port);
              TProtocol prot = new TBinaryProtocol(socket);
              Client c = new Client(prot);
              socket.open();
              cassandraClients.add(c);
          } catch (TTransportException ex) {
        	  throw new NucleusDataStoreException("Error when connecting to client: "+
  					 host+":"+port);
          }
      }
		
	}

	@Override
	public XAResource getXAResource() {
		// TODO Auto-generated method stub
		return null;
	}

	void incrementReferenceCount() {
		++referenceCount;
		disableExpirationTime();
	}

	public void release() {
		--referenceCount;

		if (referenceCount == 0) {
			close();
			enableExpirationTime();
		} else if (referenceCount < 0) {
			throw new NucleusDataStoreException("Too many calls on release(): "
					+ this);
		}
	}

	private void enableExpirationTime() {
		this.expirationTime = System.currentTimeMillis() + idleTimeoutMills;
	}

	private void disableExpirationTime() {
		this.expirationTime = -1;
	}

	public void setIdleTimeoutMills(int mills) {
		this.idleTimeoutMills = mills;
	}

	public boolean isExpired() {
		return expirationTime > 0
				&& expirationTime > System.currentTimeMillis();
	}

	public void dispose() {
		isDisposed = true;
		for (Client client : cassandraClients) {
			client.getOutputProtocol().getTransport().close();
		}
	}

	public boolean isDisposed() {
		return isDisposed;
	}
	

}
