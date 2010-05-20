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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.Deletion;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.thrift.SlicePredicate;
import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.StateManager;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.Relation;
import org.datanucleus.store.ExecutionContext;
import org.datanucleus.store.ObjectProvider;
import org.datanucleus.store.fieldmanager.AbstractFieldManager;

//TODO isolate cassandra operations...
public class CassandraInsertFieldManager extends AbstractFieldManager {

	private AbstractClassMetaData acmd;
	private ObjectProvider objectProvider;

	private List<Mutation> mutations;
	private Deletion deletion;
	private String column_family;
	private String row_key;

	public CassandraInsertFieldManager(AbstractClassMetaData acmd,
			ObjectProvider objp, String key, String ColumnFamily) {
		this.acmd = acmd;
		this.objectProvider = objp;

		this.mutations = new ArrayList<Mutation>();
		this.column_family = ColumnFamily;
		this.row_key = key;
	}

	public void storeBooleanField(int fieldNumber, boolean value) {
		String columnName = CassandraUtils.getQualifierName(acmd, fieldNumber);
		try {
			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			ObjectOutputStream oos = new ObjectOutputStream(bos);
			oos.writeBoolean(value);
			oos.flush();
			Mutation mutation = new Mutation();
			ColumnOrSuperColumn columnOrSuperColumn = new ColumnOrSuperColumn();
			Column column = new Column(columnName.getBytes(),
					bos.toByteArray(), System.currentTimeMillis());
			columnOrSuperColumn.setColumn(column);
			mutation.setColumn_or_supercolumn(columnOrSuperColumn);
			mutations.add(mutation);
			oos.close();
			bos.close();
		} catch (IOException e) {
			throw new NucleusException(e.getMessage(), e);
		}
	}

	public void storeByteField(int fieldNumber, byte value) {
		String columnName = CassandraUtils.getQualifierName(acmd, fieldNumber);

		Mutation mutation = new Mutation();
		ColumnOrSuperColumn columnOrSuperColumn = new ColumnOrSuperColumn();
		Column column = new Column(columnName.getBytes(), new byte[] { value },
				System.currentTimeMillis());
		columnOrSuperColumn.setColumn(column);
		mutation.setColumn_or_supercolumn(columnOrSuperColumn);
		mutations.add(mutation);

	}

	public void storeCharField(int fieldNumber, char value) {
		String columnName = CassandraUtils.getQualifierName(acmd, fieldNumber);

		try {
			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			ObjectOutputStream oos = new ObjectOutputStream(bos);
			oos.writeChar(value);
			oos.flush();

			Mutation mutation = new Mutation();
			ColumnOrSuperColumn columnOrSuperColumn = new ColumnOrSuperColumn();
			Column column = new Column(columnName.getBytes(),
					bos.toByteArray(), System.currentTimeMillis());
			columnOrSuperColumn.setColumn(column);
			mutation.setColumn_or_supercolumn(columnOrSuperColumn);
			mutations.add(mutation);

			oos.close();
			bos.close();
		} catch (IOException e) {
			throw new NucleusException(e.getMessage(), e);
		}
	}

	public void storeDoubleField(int fieldNumber, double value) {
		String columnName = CassandraUtils.getQualifierName(acmd, fieldNumber);

		try {
			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			ObjectOutputStream oos = new ObjectOutputStream(bos);
			oos.writeDouble(value);
			oos.flush();

			Mutation mutation = new Mutation();
			ColumnOrSuperColumn columnOrSuperColumn = new ColumnOrSuperColumn();
			Column column = new Column(columnName.getBytes(),
					bos.toByteArray(), System.currentTimeMillis());
			columnOrSuperColumn.setColumn(column);
			mutation.setColumn_or_supercolumn(columnOrSuperColumn);
			mutations.add(mutation);

			oos.close();
			bos.close();
		} catch (IOException e) {
			throw new NucleusException(e.getMessage(), e);
		}
	}

	public void storeFloatField(int fieldNumber, float value) {
		String columnName = CassandraUtils.getQualifierName(acmd, fieldNumber);

		try {
			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			ObjectOutputStream oos = new ObjectOutputStream(bos);
			oos.writeFloat(value);
			oos.flush();

			Mutation mutation = new Mutation();
			ColumnOrSuperColumn columnOrSuperColumn = new ColumnOrSuperColumn();
			Column column = new Column(columnName.getBytes(),
					bos.toByteArray(), System.currentTimeMillis());
			columnOrSuperColumn.setColumn(column);
			mutation.setColumn_or_supercolumn(columnOrSuperColumn);
			mutations.add(mutation);

			oos.close();
			bos.close();
		} catch (IOException e) {
			throw new NucleusException(e.getMessage(), e);
		}
	}

	public void storeIntField(int fieldNumber, int value) {
		String columnName = CassandraUtils.getQualifierName(acmd, fieldNumber);

		try {
			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			ObjectOutputStream oos = new ObjectOutputStream(bos);
			oos.writeInt(value);
			oos.flush();

			Mutation mutation = new Mutation();
			ColumnOrSuperColumn columnOrSuperColumn = new ColumnOrSuperColumn();
			Column column = new Column(columnName.getBytes(),
					bos.toByteArray(), System.currentTimeMillis());
			columnOrSuperColumn.setColumn(column);
			mutation.setColumn_or_supercolumn(columnOrSuperColumn);
			mutations.add(mutation);

			oos.close();
			bos.close();
		} catch (IOException e) {
			throw new NucleusException(e.getMessage(), e);
		}
	}

	public void storeLongField(int fieldNumber, long value) {
		String columnName = CassandraUtils.getQualifierName(acmd, fieldNumber);

		try {
			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			ObjectOutputStream oos = new ObjectOutputStream(bos);
			oos.writeLong(value);
			oos.flush();

			Mutation mutation = new Mutation();
			ColumnOrSuperColumn columnOrSuperColumn = new ColumnOrSuperColumn();
			Column column = new Column(columnName.getBytes(),
					bos.toByteArray(), System.currentTimeMillis());
			columnOrSuperColumn.setColumn(column);
			mutation.setColumn_or_supercolumn(columnOrSuperColumn);
			mutations.add(mutation);

			oos.close();
			bos.close();
		} catch (IOException e) {
			throw new NucleusException(e.getMessage(), e);
		}
	}

	public void storeShortField(int fieldNumber, short value) {
		String columnName = CassandraUtils.getQualifierName(acmd, fieldNumber);

		try {
			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			ObjectOutputStream oos = new ObjectOutputStream(bos);
			oos.writeShort(value);
			oos.flush();

			Mutation mutation = new Mutation();
			ColumnOrSuperColumn columnOrSuperColumn = new ColumnOrSuperColumn();
			Column column = new Column(columnName.getBytes(),
					bos.toByteArray(), System.currentTimeMillis());
			columnOrSuperColumn.setColumn(column);
			mutation.setColumn_or_supercolumn(columnOrSuperColumn);
			mutations.add(mutation);

			oos.close();
			bos.close();
		} catch (IOException e) {
			throw new NucleusException(e.getMessage(), e);
		}
	}

	public void storeStringField(int fieldNumber, String value) {
		String columnName = CassandraUtils.getQualifierName(acmd, fieldNumber);

		if (value == null) {

			if (deletion == null) { // No deletes yet, create a new Deletion.
				deletion = new Deletion();
				SlicePredicate predicate = new SlicePredicate();
				List<byte[]> column_names = new ArrayList<byte[]>();
				column_names.add(columnName.getBytes());
				predicate.setColumn_names(column_names);
				deletion.setPredicate(predicate);
			} else {// add the column to the ones to be deleted
				deletion.getPredicate().getColumn_names().add(
						columnName.getBytes());
			}
		} else {
			try {
				ByteArrayOutputStream bos = new ByteArrayOutputStream();
				ObjectOutputStream oos = new ObjectOutputStream(bos);
				oos.writeObject(value);

				Mutation mutation = new Mutation();
				ColumnOrSuperColumn columnOrSuperColumn = new ColumnOrSuperColumn();
				Column column = new Column(columnName.getBytes(), bos
						.toByteArray(), System.currentTimeMillis());
				columnOrSuperColumn.setColumn(column);
				mutation.setColumn_or_supercolumn(columnOrSuperColumn);
				mutations.add(mutation);

				oos.close();
				bos.close();
			} catch (IOException e) {
				throw new NucleusException(e.getMessage(), e);
			}
		}
	}

	public void storeObjectField(int fieldNumber, Object value) {
		String columnName = CassandraUtils.getQualifierName(acmd, fieldNumber);

		if (value == null) {

			if (deletion == null) { // No deletes yet, create a new Deletion.
				deletion = new Deletion();
				SlicePredicate predicate = new SlicePredicate();
				List<byte[]> column_names = new ArrayList<byte[]>();
				column_names.add(columnName.getBytes());
				predicate.setColumn_names(column_names);
				deletion.setPredicate(predicate);
			} else {// add the column to the ones to be deleted
				deletion.getPredicate().getColumn_names().add(
						columnName.getBytes());
			}
		} else {

			ExecutionContext context = objectProvider.getExecutionContext();
			ClassLoaderResolver clr = context.getClassLoaderResolver();
			AbstractMemberMetaData fieldMetaData = acmd
					.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber);
			int relationType = fieldMetaData.getRelationType(clr);

			if (relationType == Relation.ONE_TO_ONE_BI
					|| relationType == Relation.ONE_TO_ONE_UNI
					|| relationType == Relation.MANY_TO_ONE_BI) {

				Object persisted = context.persistObjectInternal(value,
						objectProvider, -1, StateManager.PC);

				Object valueId = context.getApiAdapter().getIdForObject(
						persisted);

				try {

					ByteArrayOutputStream bos = new ByteArrayOutputStream();
					ObjectOutputStream oos = new ObjectOutputStream(bos);
					oos.writeObject(valueId);

					Mutation mutation = new Mutation();
					ColumnOrSuperColumn columnOrSuperColumn = new ColumnOrSuperColumn();
					Column column = new Column(columnName.getBytes(), bos
							.toByteArray(), System.currentTimeMillis());
					columnOrSuperColumn.setColumn(column);
					mutation.setColumn_or_supercolumn(columnOrSuperColumn);
					mutations.add(mutation);

					oos.close();
					bos.close();
				} catch (IOException e) {
					throw new NucleusException(e.getMessage(), e);
				}

				return;

			} else if (relationType == Relation.MANY_TO_MANY_BI
					|| relationType == Relation.ONE_TO_MANY_BI
					|| relationType == Relation.ONE_TO_MANY_UNI) {

				if (value instanceof Collection) {

					List<Object> mapping = new ArrayList<Object>();

					for (Object c : (Collection) value) {
						Object persisted = context.persistObjectInternal(c,
								objectProvider, -1, StateManager.PC);
						Object valueId = context.getApiAdapter()
								.getIdForObject(persisted);
						mapping.add(valueId);
					}

					try {

						ByteArrayOutputStream bos = new ByteArrayOutputStream();
						ObjectOutputStream oos = new ObjectOutputStream(bos);
						oos.writeObject(mapping);

						Mutation mutation = new Mutation();
						ColumnOrSuperColumn columnOrSuperColumn = new ColumnOrSuperColumn();
						Column column = new Column(columnName.getBytes(), bos
								.toByteArray(), System.currentTimeMillis());
						columnOrSuperColumn.setColumn(column);
						mutation.setColumn_or_supercolumn(columnOrSuperColumn);
						mutations.add(mutation);

						oos.close();
						bos.close();
					} catch (IOException e) {
						throw new NucleusException(e.getMessage(), e);
					}
				}
				return;
			}

			try {
				ByteArrayOutputStream bos = new ByteArrayOutputStream();
				ObjectOutputStream oos = new ObjectOutputStream(bos);
				oos.writeObject(value);

				Mutation mutation = new Mutation();
				ColumnOrSuperColumn columnOrSuperColumn = new ColumnOrSuperColumn();
				Column column = new Column(columnName.getBytes(), bos
						.toByteArray(), System.currentTimeMillis());
				columnOrSuperColumn.setColumn(column);
				mutation.setColumn_or_supercolumn(columnOrSuperColumn);
				mutations.add(mutation);

				oos.close();
				bos.close();
			} catch (IOException e) {
				throw new NucleusException(e.getMessage(), e);
			}
		}

	}

	public Map<String, Map<String, List<Mutation>>> getMutation() {

		if (deletion != null) {
			Mutation mutation = new Mutation();
			mutation.setDeletion(deletion);
			mutations.add(mutation);
		}

		Map<String, List<Mutation>> columnFamily_mutations = new TreeMap<String, List<Mutation>>();
		columnFamily_mutations.put(column_family, mutations);
		Map<String, Map<String, List<Mutation>>> mutation_map = new TreeMap<String, Map<String, List<Mutation>>>();
		mutation_map.put(row_key, columnFamily_mutations);

		return mutation_map;

	}

}
