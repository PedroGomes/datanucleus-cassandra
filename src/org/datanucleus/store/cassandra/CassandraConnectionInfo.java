/**********************************************************************
Copyright (c) 2010 Pedro Gomes and Universidade do Minho. All rights reserved.
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

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.cassandra.thrift.TokenRange;
import org.apache.cassandra.thrift.Cassandra.Client;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.datanucleus.PersistenceConfiguration;
import org.datanucleus.exceptions.NucleusDataStoreException;

public class CassandraConnectionInfo {

	private static Map<String, Integer> ring_connections;
	private static String keyspace;

	private static boolean intialized = false;
	
	public CassandraConnectionInfo(PersistenceConfiguration conf) {

		if(intialized){
			return;
		}
		
		ring_connections = new TreeMap<String, Integer>();

		String url = conf.getStringProperty("datanucleus.ConnectionURL");
		String[] url_info = url.split("://");

		if (url_info.length < 2) {
			throw new NucleusDataStoreException(
					"Malformed URL : Example -> "
							+ "cassandra:KeySpace://connectionURL:port,connectionURL:port...");
		}
		
		String[] dataStore_info = url_info[0].split(":"); // datastore:Keyspace
		String[] connection_info = url_info[1].split(","); // host:port,host:port...
		
		if (dataStore_info.length < 2) {
			throw new NucleusDataStoreException(
					"Malformed URL : No defined Keyspace -> "
							+ "cassandra:KeySpace://connectionURL:port,connectionURL:port...");
		}
		
		

		// get Keyspace
		if (dataStore_info[1] != null && !dataStore_info[1].isEmpty()) {
			keyspace = dataStore_info[1];
		} else {
			throw new NucleusDataStoreException(
					"Malformed URL : No defined Keyspace -> "
							+ "cassandra:KeySpace://connectionURL:port,connectionURL:port...");
		}

		// get first connection
		String main_connection;

		if (connection_info[0] != null && !connection_info[0].isEmpty()) {
			main_connection = connection_info[0];
		} else {
			throw new NucleusDataStoreException(
					"Malformed URL : No main connection -> "
							+ "cassandra:KeySpace://connectionURL:port,connectionURL:port...");
		}
		String[] main_connection_info = main_connection.split(":");
		String main_host;
		String main_port;
		if (main_connection_info[0] != null
				&& !main_connection_info[0].isEmpty()) {
			main_host = main_connection_info[0];
		} else {
			throw new NucleusDataStoreException(
					"Malformed URL : Malformed main  connection  -> "
							+ "cassandra:KeySpace://connectionURL:port,connectionURL:port...");
		}
		if (main_connection_info[1] != null
				&& !main_connection_info[1].isEmpty()) {
			main_port = main_connection_info[1];
		} else {
			throw new NucleusDataStoreException(
					"Malformed URL : Malformed  main  connection  -> "
							+ "cassandra:KeySpace://connectionURL:port,connectionURL:port...");
		}
		int port;
		try {
			port = Integer.parseInt(main_port);
		} catch (Exception e) {
			throw new NucleusDataStoreException(
					"Malformed URL : Error when extracting connection port  -> "
							+ "cassandra:KeySpace://connectionURL:port,connectionURL:port...");
		}
		ring_connections.put(main_host, port);

		// get other connections, the term ring activates ring lookup.
		int num_endpoints = connection_info.length - 1;

		boolean other_endpoints = num_endpoints > 0;
		boolean get_ring = false;
		int endpoint_it = 1;

		String endpoint_host;
		int endpoint_port = 0;

		while (other_endpoints && endpoint_it <= num_endpoints) {

			if (connection_info[endpoint_it] != null
					&& !connection_info[endpoint_it].isEmpty()) {
				String enpoint_info[] = connection_info[endpoint_it].split(":");
				if (enpoint_info[0] != null && !enpoint_info[0].isEmpty()) {
					endpoint_host = enpoint_info[0];
				} else {
					throw new NucleusDataStoreException(
							"Malformed URL : Error when extracting connection port  -> "
									+ "cassandra:KeySpace://connectionURL:port,connectionURL:port...");
				}

				String end_port;
				if (enpoint_info[1] != null && !enpoint_info[1].isEmpty()) {
					end_port = enpoint_info[1];
					try {
						endpoint_port = Integer.parseInt(end_port);
					} catch (Exception e) {
						throw new NucleusDataStoreException(
								"Malformed URL : Error when extracting connection port  -> "
										+ "cassandra:KeySpace://connectionURL:port,connectionURL:port...");
					}

				} else {
					throw new NucleusDataStoreException(
							"Malformed URL : Error when extracting connection port  -> "
									+ "cassandra:KeySpace://connectionURL:port,connectionURL:port...");
				}

				if (endpoint_host.equalsIgnoreCase("ring")) {
					get_ring = true;
					other_endpoints = false;
				} else {
					ring_connections.put(endpoint_host, endpoint_port);
				}
				endpoint_it++;
			}

		}

		if (get_ring) {
			try {
				TSocket socket = new TSocket(main_host, port);
				TProtocol protocol = new TBinaryProtocol(socket);
				Client cassandraClient = new Client(protocol);
				socket.open();

				List<TokenRange> ring = cassandraClient
						.describe_ring(keyspace);
				for (TokenRange tr : ring) {
					List<String> endpoints = tr.endpoints;
					for (String endpoint : endpoints) {
						if(!ring_connections.containsKey(endpoint)){
							//System.out.println("New node connection->"+endpoint+":"+endpoint_port);
							ring_connections.put(endpoint, endpoint_port);
						}
					}
				}
			} catch (TException e) {
				throw new NucleusDataStoreException(e.getMessage(), e);
			}

		}
		intialized = true;
	}

	public Map<String, Integer> getConnections() {
		return ring_connections;
	}

	public String getKeyspace() {
		return keyspace;
	}

}
