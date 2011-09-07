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

import org.apache.cassandra.thrift.Cassandra.Client;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.ColumnMetaData;

import java.io.*;
import java.util.*;

public class CassandraUtils {

    private static boolean alterSchema = false;
    private static boolean containsKeySpace = false;

    private static StringBuilder schemaInfo = null;
    private static String keyspaceInfo = "";

    private static File cassandra_Schema_File = null;

    public static String getQualifierName(AbstractClassMetaData acmd,
                                          int absoluteFieldNumber) {
        AbstractMemberMetaData ammd = acmd
                .getMetaDataForManagedMemberAtAbsolutePosition(absoluteFieldNumber);
        String columnName = null;

        // Try the first column if specified
        ColumnMetaData[] colmds = ammd.getColumnMetaData();
        if (colmds != null && colmds.length > 0) {
            columnName = colmds[0].getName();
        }
        if (columnName == null) {
            // Fallback to the field/property name
            columnName = ammd.getName();
        }
        if (columnName.indexOf(":") > -1) {
            columnName = columnName.substring(columnName.indexOf(":") + 1);
        }
        return columnName;
    }

    public static String getQualifierName(AbstractMemberMetaData ammd) {
        String columnName = null;

        // Try the first column if specified
        ColumnMetaData[] colmds = ammd.getColumnMetaData();
        if (colmds != null && colmds.length > 0) {
            columnName = colmds[0].getName();
        }
        if (columnName == null) {
            // Fallback to the field/property name
            columnName = ammd.getName();
        }
        if (columnName.indexOf(":") > -1) {
            columnName = columnName.substring(columnName.indexOf(":") + 1);
        }
        return columnName;
    }

    public static String getFamilyName(AbstractClassMetaData acmd) {
        if (acmd.getTable() != null) {
            return acmd.getTable();
        }
        return acmd.getName();
    }

    public static byte[] getKeyColumn(AbstractClassMetaData acm) {

        int[] pks = acm.getPKMemberPositions();

        if (pks.length != 1) {
            throw new NucleusDataStoreException(
                    "Only single field identity objects are allowed");
        }

        AbstractMemberMetaData memberMetaData = acm
                .getMetaDataForManagedMemberAtAbsolutePosition(pks[0]);

        ColumnMetaData[] cmd = memberMetaData.getColumnMetaData();
        if (cmd != null && cmd.length > 0) {
            return cmd[0].getName().getBytes();
        }
        return memberMetaData.getName().getBytes();
    }

    /**
     * Generates the elements in fault within the schema, when running the application.
     * In version 0.7 of Cassandra will be responsible for the runtime edition of the schema.
     */
    public static void createSchema(AbstractClassMetaData classMetaData,
                                    CassandraStoreManager storeManager) throws IOException {

        Client cassandraClient = null;
        CassandraConnectionInfo connectionInfo = storeManager
                .getConnectionInfo();
        cassandraClient = getConnection(connectionInfo);
        String keyspace = connectionInfo.getKeyspace();

        if (cassandraClient == null) {
            throw new NucleusDataStoreException(
                    "No connection to the data store");
        }

        if (schemaInfo == null) { // when starting the schema, check if the
            // keyspace is already defined.
            schemaInfo = new StringBuilder();

            try {
                containsKeySpace = cassandraClient.describe_keyspaces()
                        .contains(keyspace);
            } catch (TException e) {
                throw new NucleusDataStoreException(e.getMessage(), e);
            }

            if (!containsKeySpace) {
                alterSchema = true;
                schemaInfo.append("<Keyspace Name=\"" + keyspace + "\">\n");
                keyspaceInfo =
                        "<ReplicaPlacementStrategy>"
                                + storeManager.getReplicaPlacementStrategy()
                                + "</ReplicaPlacementStrategy>\n"
                                + "<ReplicationFactor>"
                                + storeManager.getReplicationFactor()
                                + "</ReplicationFactor>\n"
                                + "<EndPointSnitch>"
                                + storeManager.getEndPointSnitch()
                                + "</EndPointSnitch>\n" + "</Keyspace>\n";
            }
        }

        boolean containsColumn = false;
        String columnName = getFamilyName(classMetaData);
        if (containsKeySpace) {
            try {
                containsColumn = cassandraClient.describe_keyspace(keyspace)
                        .containsKey(columnName);
            } catch (NotFoundException e) {
                throw new NucleusDataStoreException(e.getMessage(), e);
            } catch (TException e) {
                throw new NucleusDataStoreException(e.getMessage(), e);
            }
        }
        if (!containsColumn) {
            alterSchema = true;
            schemaInfo.append("<ColumnFamily Name=\"" + columnName
                    + "\" CompareWith=\"BytesType\"/>\n");
        }

        if (alterSchema) {

            if (cassandra_Schema_File == null) {
                cassandra_Schema_File = new File("datanucleus.schema");

                if (cassandra_Schema_File.exists()) {// delete
                    cassandra_Schema_File.delete();
                }
                cassandra_Schema_File.createNewFile();// create new
            }

            if (cassandra_Schema_File != null) {
                System.out.println("FILE:"
                        + cassandra_Schema_File.getAbsolutePath());
                DataOutputStream out = null;
                try {
                    FileOutputStream file = new FileOutputStream(
                            cassandra_Schema_File, false);
                    out = new DataOutputStream(file);

                    out.write((schemaInfo.toString() + keyspaceInfo)
                            .getBytes());

                    out.flush();
                    out.close();
                } catch (IOException e) {
                    out.close();
                    throw new NucleusException(e.getMessage(), e);
                }
            } else {
                throw new NucleusException("Schema file was not created.");
            }
        }
    }

    public static Client getConnection(CassandraConnectionInfo connectionInfo) {
        Map<String, Integer> connections = connectionInfo.getConnections();
        Iterator<String> connections_iterator = connections.keySet().iterator();

        boolean connection = false;
        Client cassandraClient = null;

        while (connections_iterator.hasNext() && !connection) {
            String host = (String) connections_iterator.next();
            connection = true;
            TSocket socket = new TSocket(host, connections.get(host));
            TProtocol protocol = new TBinaryProtocol(socket);
            cassandraClient = new Client(protocol);
            try {
                socket.open();
            } catch (TTransportException e) {
                System.out.println("Dead client: " + host + ":"
                        + connections.get(host));
                connection = false;
            }
        }

        return cassandraClient;
    }


    public static List<byte[]> getFetchColumnList(
            AbstractClassMetaData metaData, int[] fieldNumbers) {

        List<byte[]> fieldNames = new ArrayList<byte[]>(fieldNumbers.length);

        for (int i = 0; i < fieldNumbers.length; i++) {
            try {
                String column_name = getQualifierName(metaData, fieldNumbers[i]);
                fieldNames.add(column_name.getBytes("UTF-8"));
            } catch (UnsupportedEncodingException e) {
                throw new NucleusDataStoreException(e.getMessage(), e);
            }
        }

        return fieldNames;
    }


    public static void addMutation(Mutation mutation, String key, String family, Map<String, Map<String, List<Mutation>>> mutationMap) {

        if (mutationMap.containsKey(key)) {
            Map<String, List<Mutation>> mutations_key = mutationMap.get(key);
            if (mutations_key.containsKey(family)) {
                List<Mutation> mutation_list = mutations_key.get(family);
                if (mutation_list != null) {
                    mutation_list.add(mutation);
                } else {
                    mutation_list = new ArrayList<Mutation>(1);
                    mutation_list.add(mutation);
                    mutations_key.put(family, mutation_list);
                }
            } else {
                List<Mutation> mutation_list = new ArrayList<Mutation>(1);
                mutation_list.add(mutation);
                mutations_key.put(family, mutation_list);
            }
        } else {
            Map<String, List<Mutation>> mutations_key = new TreeMap<String, List<Mutation>>();
            List<Mutation> mutation_list = new ArrayList<Mutation>(1);
            mutation_list.add(mutation);
            mutations_key.put(family, mutation_list);
            mutationMap.put(key, mutations_key);
        }


    }
}
