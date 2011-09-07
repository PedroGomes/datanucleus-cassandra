/**********************************************************************
 Copyright (c) 2010 Pedro Gomes and Universidade do Minho. All rights reserved.
 (Based on datanucleus-hbase. Copyright (c) 2009 Tatsuya Kawano and others.)
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

import org.apache.cassandra.thrift.Cassandra.Client;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.store.connection.AbstractManagedConnection;
import org.datanucleus.store.connection.ManagedConnectionResourceListener;

import javax.transaction.xa.XAResource;
import java.util.*;

public class CassandraManagedConnection extends AbstractManagedConnection {

    private CassandraConnectionInfo connectionInfo;

    private List<CassandraConnection> cassandraClients;

    private int referenceCount = 0;

    private int idleTimeoutMills = 30 * 1000; // 30 secs

    private long expirationTime;

    private int last_client = 0;

    private boolean isDisposed = false;

    public CassandraManagedConnection(CassandraConnectionInfo info) {

        connectionInfo = info;
        disableExpirationTime();
        cassandraClients = new LinkedList<CassandraConnection>();


    }


    @Override
    public void close() {

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

    private Client getCassandraClient() {

        boolean openClient = false;

        Client cl = null;

        while (!openClient) {

            CassandraConnection connection = cassandraClients.get(last_client);

            if (connection.isOpen()) {
                cl = connection.getConnection();
                openClient = true;
            } else {
                cassandraClients.remove(last_client);
            }
            last_client++;
            last_client = last_client >= cassandraClients.size() ? 0
                    : last_client;

        }

        //try one more time

        if (!openClient) {
            establishNodeConnection();
        }

        CassandraConnection connection = cassandraClients.get(last_client);

        if (connection.isOpen()) {
            cl = connection.getConnection();
        } else {
            throw new NucleusDataStoreException("Connection error, no available nodes");
        }


        if (referenceCount == 0) {
            throw new NucleusDataStoreException("Fetching client with no reference incrementation: "
                    + this);
        }

        return cl;
    }


    /**
     * Establish connections to all nodes.
     * Trade-off : N connections vs connection establishment time.
     */
    public void establishNodeConnection() {

        Random random = new Random();

        Map<String, Integer> pre_connections = connectionInfo.getConnections();
        Map<String, Integer> connections = new TreeMap<String, Integer>();

        int selected_connection = random.nextInt(pre_connections.size());
        int iterator = 0;

        for (String host : pre_connections.keySet()) {
            if (iterator == selected_connection) {
                connections.put(host, pre_connections.get(host));
                break;
            }
            iterator++;
        }


        for (String host : connections.keySet()) {


            int port = connections.get(host);
            CassandraConnection cassandraConnection = new CassandraConnection(host, port);
            cassandraClients.add(cassandraConnection);


            for (int i = 0; i < cassandraClients.size(); i++) {
                int pos = random.nextInt(cassandraClients.size());
                CassandraConnection temp = cassandraClients.get(pos);
                cassandraClients.set(pos, cassandraClients.get(i));
                cassandraClients.set(i, temp);
            }
        }

    }

    @Override
    public XAResource getXAResource() {
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
            throw new NucleusDataStoreException("Too many invocations on release(): "
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
                && expirationTime < System.currentTimeMillis();
    }

    public void dispose() {

        isDisposed = true;

        if (referenceCount > 0) {
            throw new NucleusDataStoreException("Invoking close on a used connection "
                    + this);
        }

        for (CassandraConnection client : cassandraClients) {
            client.close();
            client = null;

        }
    }

    public boolean isDisposed() {
        return isDisposed;
    }

    @Override
    public void flush() {

    }

    @Override
    public void addListener(ManagedConnectionResourceListener listener)
    {

    }


}

class CassandraConnection {

    private final TTransport transport;
    private final TSocket socket;
    private final TProtocol protocol;
    private final Client client;

    CassandraConnection(String host, int port) {
        try {
            socket = new TSocket(host, port);
            protocol = new TBinaryProtocol(socket);
            transport = socket;
            client = new Client(protocol);
            socket.open();
        } catch (TTransportException ex) {
            throw new NucleusDataStoreException("Error when connecting to client: " +
                    host + ":" + port);
        }
    }

    public boolean isOpen() {
        return transport.isOpen();
    }

    public void close() {
        try {

            client.getOutputProtocol().getTransport().flush();
            client.getOutputProtocol().getTransport().close();

        } catch (Exception e) {
            throw new NucleusDataStoreException("Error when closing client", e);
        }
    }

    public Client getConnection() {
        return client;
    }

}



