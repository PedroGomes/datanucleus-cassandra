/**********************************************************************
Copyright (c) 2010 Pedro Gomes and Universidade do Minho. All rights reserved.
(Based on datanucleus-hbase. Copyright (c) 2009 Erik Bengtson and others.)
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

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.Deletion;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.cassandra.thrift.Cassandra.Client;
import org.apache.thrift.TException;
import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.api.ApiAdapter;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusObjectNotFoundException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.Relation;
import org.datanucleus.store.AbstractPersistenceHandler;
import org.datanucleus.store.ExecutionContext;
import org.datanucleus.store.ObjectProvider;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.cassandra.index.IndexHandler;
import org.datanucleus.util.Localiser;

public class CassandraPersistenceHandler extends AbstractPersistenceHandler {

	/** Localiser for messages. */
	Localiser LOCALISER;

	protected final CassandraStoreManager storeManager;
	private static String keyspace;

	public CassandraPersistenceHandler(StoreManager stm) {

		LOCALISER = Localiser.getInstance(
				"org.datanucleus.store.cassandra.Localisation",
				CassandraPersistenceHandler.class.getClassLoader());
		storeManager = (CassandraStoreManager) stm;
		keyspace = storeManager.getConnectionInfo().getKeyspace();
		IndexHandler.setPersistenceHandler(this);
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
	}

	@Override
	public void deleteObject(ObjectProvider objp) {

		// Check if read-only so update not permitted

		CassandraManagedConnection managedConnection = (CassandraManagedConnection) storeManager
				.getConnection(objp.getExecutionContext());

		storeManager.assertReadOnlyForUpdateOfObject(objp);

		AbstractClassMetaData acmd = objp.getClassMetaData();
		ExecutionContext ec = objp.getExecutionContext();

		Map<String, Map<String, List<Mutation>>> mutation_map = new TreeMap<String, Map<String, List<Mutation>>>();

		Object pkValue = objp.provideField(acmd.getPKMemberPositions()[0]);
		try {

			String key = CassandraUtils.ObjectToString(pkValue);
			String column_family = CassandraUtils.getFamilyName(acmd);

			int[] fields = acmd.getAllMemberPositions();

			for (int field_number : fields) {
				AbstractMemberMetaData fieldMetaData = acmd
						.getMetaDataForManagedMemberAtAbsolutePosition(field_number);
				// here we have the field value

				Object value = objp.provideField(field_number);

				ClassLoaderResolver clr = ec.getClassLoaderResolver();

				int relationType = fieldMetaData.getRelationType(clr);

				if (relationType == Relation.ONE_TO_ONE_BI
						|| relationType == Relation.ONE_TO_ONE_UNI
						|| relationType == Relation.MANY_TO_ONE_BI) {
					if (value == null) {
						objp.loadField(field_number);
						value = objp.provideField(field_number);
					}
					if (value == null) { //if still null, ignore
						continue;
					}
					Object valueId = ec.getApiAdapter().getIdForObject(value);
					IndexHandler.deleteIndex(fieldMetaData, field_number, objp,
							key, valueId, mutation_map);
				} else {
					if (value == null) {
						continue;
					}
					IndexHandler.deleteIndex(fieldMetaData, field_number, objp,
							key, value, mutation_map);
				}

				if (fieldMetaData.isDependent()
						|| (fieldMetaData.getCollection() != null && fieldMetaData
								.getCollection().isDependentElement())) {

					// check if this is a relationship
					if (relationType == Relation.ONE_TO_ONE_BI
							|| relationType == Relation.ONE_TO_ONE_UNI
							|| relationType == Relation.MANY_TO_ONE_BI) {

						ec.deleteObjectInternal(value);
					}

					else if (relationType == Relation.MANY_TO_MANY_BI
							|| relationType == Relation.ONE_TO_MANY_BI
							|| relationType == Relation.ONE_TO_MANY_UNI) {

						if (fieldMetaData.hasCollection()) {

							for (Object element : (Collection<?>) value) {
								ec.deleteObjectInternal(element);
							}

						} else if (fieldMetaData.hasMap()) {
							ApiAdapter adapter = ec.getApiAdapter();

							Map<?, ?> map = ((Map<?, ?>) value);
							Object mapValue;

							// get each element and persist it.
							for (Object mapKey : map.keySet()) {

								mapValue = map.get(mapKey);

								if (adapter.isPersistable(mapKey)) {
									ec.deleteObjectInternal(mapKey);

								}
								if (adapter.isPersistable(mapValue)) {
									ec.deleteObjectInternal(mapValue);
								}

							}

						} else if (fieldMetaData.hasArray()) {
							Object persisted = null;

							for (int i = 0; i < Array.getLength(value); i++) {
								persisted = Array.get(value, i);
								ec.deleteObjectInternal(persisted);
							}
						}

					}

				}

			}

			String family = CassandraUtils.getFamilyName(acmd);

			Deletion deletion = new Deletion(System.currentTimeMillis());
			Mutation mutation = new Mutation();
			mutation.setDeletion(deletion);
			CassandraUtils.addMutation(mutation, key, family, mutation_map);

			try {
				// ((Client) managedConnection.getConnection()).remove(keyspace,
				// key, path, System.currentTimeMillis(),
				// ConsistencyLevel.QUORUM);
				((Client) managedConnection.getConnection()).batch_mutate(
						keyspace, mutation_map, ConsistencyLevel.QUORUM);

			} catch (InvalidRequestException e) {
				throw new NucleusDataStoreException(e.getMessage(), e);
			} catch (UnavailableException e) {
				throw new NucleusDataStoreException(e.getMessage(), e);
			} catch (TimedOutException e) {
				throw new NucleusDataStoreException(e.getMessage(), e);
			} catch (TException e) {
				throw new NucleusDataStoreException(e.getMessage(), e);
			}
		} catch (IOException e) {
			throw new NucleusDataStoreException(e.getMessage(), e);
		} finally {
			managedConnection.release();
		}

	}

	@Override
	public void fetchObject(ObjectProvider objp, int[] fieldNumbers) {

		CassandraManagedConnection managedConnection = (CassandraManagedConnection) storeManager
				.getConnection(objp.getExecutionContext());

		try {
			AbstractClassMetaData acmd = objp.getClassMetaData();

			Object pkValue = objp.provideField(acmd.getPKMemberPositions()[0]);
			String key = CassandraUtils.ObjectToString(pkValue);

			ColumnParent parent = new ColumnParent(CassandraUtils
					.getFamilyName(acmd));

			Client dbClient = ((Client) managedConnection.getConnection());

			SliceRange range = new SliceRange();
			range.setStart(new byte[] {});
			range.setFinish(new byte[] {});
			range.setCount(dbClient.get_count(keyspace, key, parent,
					ConsistencyLevel.QUORUM));
			SlicePredicate predicate = new SlicePredicate();

			predicate.setSlice_range(range);

			List<ColumnOrSuperColumn> result = dbClient.get_slice(keyspace,
					key, parent, predicate, ConsistencyLevel.QUORUM);

			if (result == null) {
				throw new NucleusObjectNotFoundException();
			}
			CassandraFetchFieldManager fm = new CassandraFetchFieldManager(
					acmd, objp, result);
			objp.replaceFields(acmd.getAllMemberPositions(), fm);

		} catch (IOException e) {
			throw new NucleusDataStoreException(e.getMessage(), e);
		} catch (InvalidRequestException e) {
			throw new NucleusDataStoreException(e.getMessage(), e);
		} catch (UnavailableException e) {
			throw new NucleusDataStoreException(e.getMessage(), e);
		} catch (TimedOutException e) {
			throw new NucleusDataStoreException(e.getMessage(), e);
		} catch (TException e) {
			throw new NucleusDataStoreException(e.getMessage(), e);
		} finally {
			managedConnection.release();
		}

	}

	@Override
	public Object findObject(ExecutionContext arg0, Object arg1) {

		return null;
	}

	@Override
	public void insertObject(ObjectProvider objp) {
		// Check if read-only so update not permitted
		storeManager.assertReadOnlyForUpdateOfObject(objp);

		if (!storeManager.managesClass(objp.getClassMetaData()
				.getFullClassName())) {
			storeManager.addClass(objp.getClassMetaData().getFullClassName(),
					objp.getExecutionContext().getClassLoaderResolver());
		}

		try {
			locateObject(objp);

			throw new NucleusUserException(LOCALISER.msg(
					"Cassandra.Insert.ObjectWithIdAlreadyExists", objp
							.getClassMetaData().getEntityName(), objp
							.getClassMetaData().getPrimaryKeyMemberNames()[0]));

		} catch (NucleusObjectNotFoundException onfe) {
			// Do nothing since object with this id doesn't exist
		}

		CassandraManagedConnection managedconnection = (CassandraManagedConnection) storeManager
				.getConnection(objp.getExecutionContext());
		try {
			AbstractClassMetaData acmd = objp.getClassMetaData();

			Object pkValue = objp.provideField(acmd.getPKMemberPositions()[0]);
			String key = CassandraUtils.ObjectToString(pkValue);
			String column_family = CassandraUtils.getFamilyName(acmd);

			CassandraInsertFieldManager fm = new CassandraInsertFieldManager(
					acmd, objp, key, column_family);
			objp.provideFields(acmd.getAllMemberPositions(), fm);
			Client client = (Client) managedconnection.getConnection();
			client.batch_mutate(keyspace, fm.getMutation(),
					ConsistencyLevel.QUORUM);

		} catch (IOException e) {
			e.printStackTrace();
			throw new NucleusDataStoreException(e.getMessage(), e);
		} catch (InvalidRequestException e) {
			e.printStackTrace();

			throw new NucleusDataStoreException(e.getMessage(), e);
		} catch (UnavailableException e) {
			e.printStackTrace();

			throw new NucleusDataStoreException(e.getMessage(), e);
		} catch (TimedOutException e) {
			e.printStackTrace();

			throw new NucleusDataStoreException(e.getMessage(), e);
		} catch (TException e) {
			e.printStackTrace();

			throw new NucleusDataStoreException(e.getMessage(), e);
		} finally {
			managedconnection.release();
		}
	}

	@Override
	public void locateObject(ObjectProvider objp) {
		CassandraManagedConnection managedconnection = (CassandraManagedConnection) storeManager
				.getConnection(objp.getExecutionContext());
		try {
			AbstractClassMetaData acmd = objp.getClassMetaData();
			Object pkValue = objp.provideField(acmd.getPKMemberPositions()[0]);
			String key = CassandraUtils.ObjectToString(pkValue);

			SlicePredicate predicate = new SlicePredicate();
			SliceRange range = new SliceRange("".getBytes(), "".getBytes(),
					false, 1);
			predicate.setSlice_range(range);
			ColumnParent parent = new ColumnParent(CassandraUtils
					.getFamilyName(acmd));

			List<ColumnOrSuperColumn> columns = ((Client) managedconnection
					.getConnection()).get_slice(keyspace, key, parent,
					predicate, ConsistencyLevel.QUORUM);

			if (columns.isEmpty()) {
				throw new NucleusObjectNotFoundException();
			}
		} catch (IOException e) {
			throw new NucleusDataStoreException(e.getMessage(), e);
		} catch (InvalidRequestException e) {
			throw new NucleusDataStoreException(e.getMessage(), e);
		} catch (UnavailableException e) {
			throw new NucleusDataStoreException(e.getMessage(), e);
		} catch (TimedOutException e) {
			throw new NucleusDataStoreException(e.getMessage(), e);
		} catch (TException e) {
			throw new NucleusDataStoreException(e.getMessage(), e);
		} finally {
			managedconnection.release();
		}
	}

	@Override
	public void updateObject(ObjectProvider objp, int[] arg1) {
		// Check if read-only so update not permitted
		storeManager.assertReadOnlyForUpdateOfObject(objp);
		CassandraManagedConnection managedconnection = (CassandraManagedConnection) storeManager
				.getConnection(objp.getExecutionContext());
		try {
			AbstractClassMetaData acmd = objp.getClassMetaData();

			Object pkValue = objp.provideField(acmd.getPKMemberPositions()[0]);
			String key = CassandraUtils.ObjectToString(pkValue);
			String column_family = CassandraUtils.getFamilyName(acmd);

			CassandraInsertFieldManager fm = new CassandraInsertFieldManager(
					acmd, objp, key, column_family);
			objp.provideFields(acmd.getAllMemberPositions(), fm);
			((Client) managedconnection.getConnection()).batch_mutate(keyspace,
					fm.getMutation(), ConsistencyLevel.QUORUM);

		} catch (IOException e) {
			throw new NucleusDataStoreException(e.getMessage(), e);
		} catch (InvalidRequestException e) {
			throw new NucleusDataStoreException(e.getMessage(), e);
		} catch (UnavailableException e) {
			throw new NucleusDataStoreException(e.getMessage(), e);
		} catch (TimedOutException e) {
			throw new NucleusDataStoreException(e.getMessage(), e);
		} catch (TException e) {
			throw new NucleusDataStoreException(e.getMessage(), e);
		} finally {
			managedconnection.release();
		}
	}

	public Object getOldFieldValue(ObjectProvider objp, int field_number) {

		storeManager.assertReadOnlyForUpdateOfObject(objp);
		CassandraManagedConnection managedconnection = (CassandraManagedConnection) storeManager
				.getConnection(objp.getExecutionContext());

		Object o = null;

		try {
			AbstractClassMetaData acmd = objp.getClassMetaData();

			Object pkValue = objp.provideField(acmd.getPKMemberPositions()[0]);

			String key = CassandraUtils.ObjectToString(pkValue);

			String column_family = CassandraUtils.getFamilyName(acmd);
			AbstractMemberMetaData fieldMetaData = acmd
					.getMetaDataForManagedMemberAtAbsolutePosition(field_number);

			String column_name = CassandraUtils.getQualifierName(fieldMetaData);

			ColumnPath path = new ColumnPath(column_family);
			path.setColumn(column_name.getBytes());

			ColumnOrSuperColumn columnOrSuperColumn = ((Client) managedconnection
					.getConnection()).get(keyspace, key, path,
					ConsistencyLevel.QUORUM);
			List<ColumnOrSuperColumn> result = new ArrayList<ColumnOrSuperColumn>();
			result.add(columnOrSuperColumn);
			Column c = columnOrSuperColumn.getColumn();

			CassandraFetchFieldManager cassandraFetchFieldManager = new CassandraFetchFieldManager(
					acmd, objp, result);

			o = cassandraFetchFieldManager.fetchField(field_number,
					fieldMetaData.getType());

		} catch (IOException e) {
			throw new NucleusDataStoreException(e.getMessage(), e);
		} catch (InvalidRequestException e) {
			throw new NucleusDataStoreException(e.getMessage(), e);
		} catch (NotFoundException e) {
			return null;
		} catch (UnavailableException e) {
			throw new NucleusDataStoreException(e.getMessage(), e);
		} catch (TimedOutException e) {
			throw new NucleusDataStoreException(e.getMessage(), e);
		} catch (TException e) {
			throw new NucleusDataStoreException(e.getMessage(), e);
		}

		return o;
	}
}
