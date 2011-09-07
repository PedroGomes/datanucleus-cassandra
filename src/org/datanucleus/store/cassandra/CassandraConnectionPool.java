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

import java.lang.ref.WeakReference;
import java.util.LinkedList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.ReentrantLock;

public class CassandraConnectionPool {

    private ReentrantLock connection_lock = new ReentrantLock();

    private final List<CassandraManagedConnection> connections;

    private final ThreadLocal<WeakReference<CassandraManagedConnection>> connectionForCurrentThread;

    private final Timer evictorThread;

    //private int timeBetweenEvictionRunsMillis = 15 * 1000; // default, 15 secs
    private int timeBetweenEvictionRunsMillis = 15 * 1000; // default, 15 secs

    public CassandraConnectionPool() {

        connectionForCurrentThread = new ThreadLocal<WeakReference<CassandraManagedConnection>>();
        connections = new CopyOnWriteArrayList<CassandraManagedConnection>();

        evictorThread = new Timer("Cassandra Connection Eviction Thread", true);
        startConnectionEvictorThread(evictorThread);

    }

    public void registerConnection(CassandraManagedConnection managedConnection) {

        connections.add(managedConnection);
        connectionForCurrentThread.set(new WeakReference<CassandraManagedConnection>(managedConnection));

    }

    public CassandraManagedConnection getPooledConnection() {
        WeakReference<CassandraManagedConnection> ref = connectionForCurrentThread.get();

        if (ref == null) {
            return null;
        } else {

            CassandraManagedConnection managedConnection = ref.get();

            connection_lock.lock();

            try {
                if (managedConnection != null && !managedConnection.isDisposed()) {
                    return managedConnection;
                } else {
                    return null;
                }

            } finally {
                connection_lock.unlock();
            }


        }
    }

    public void setTimeBetweenEvictionRunsMillis(int timeBetweenEvictionRunsMillis) {
        this.timeBetweenEvictionRunsMillis = timeBetweenEvictionRunsMillis;
    }

    private void disposeTimedOutConnections() {

        List<CassandraManagedConnection> timedOutConnections = new LinkedList<CassandraManagedConnection>();

        connection_lock.lock();


        for (CassandraManagedConnection managedConnection : connections) {
            if (managedConnection.isExpired()) {
                timedOutConnections.add(managedConnection);
                managedConnection.dispose();
            }
        }

        connection_lock.unlock();

        for (CassandraManagedConnection managedConnection : timedOutConnections) {
            connections.remove(managedConnection);
        }

    }

    private void startConnectionEvictorThread(Timer connectionTimeoutThread) {
        TimerTask timeoutTask = new TimerTask() {

            public void run() {
                disposeTimedOutConnections();
            }
        };

        evictorThread.schedule(timeoutTask, timeBetweenEvictionRunsMillis, timeBetweenEvictionRunsMillis);
    }

}
