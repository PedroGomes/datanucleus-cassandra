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

import static org.datanucleus.store.cassandra.CassandraUtils.addMutation;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.Deletion;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.thrift.SlicePredicate;
import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.StateManager;
import org.datanucleus.api.ApiAdapter;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.MapMetaData;
import org.datanucleus.metadata.MetaDataManager;
import org.datanucleus.metadata.Relation;
import org.datanucleus.store.ExecutionContext;
import org.datanucleus.store.ObjectProvider;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.cassandra.index.IndexHandler;
import org.datanucleus.store.fieldmanager.AbstractFieldManager;
import org.datanucleus.store.scostore.Store;


//TODO isolate cassandra operations...
public class CassandraInsertFieldManager extends AbstractFieldManager {

	private AbstractClassMetaData acmd;
	private ObjectProvider objectProvider;
	
	private List<Mutation> mutations;
	private Deletion deletion;
	private String column_family;
	private String row_key;

	
	Map<String, List<Mutation>> columnFamily_mutations;
	Map<String, Map<String, List<Mutation>>> mutation_map;

    //Needs low level operation isolation
	public CassandraInsertFieldManager(AbstractClassMetaData acmd,
			ObjectProvider objp ,String key, String ColumnFamily) {
		
		this.acmd = acmd;
		this.objectProvider = objp;

		this.mutations = new ArrayList<Mutation>();
		this.column_family = ColumnFamily;
		this.row_key = key;
		
		columnFamily_mutations = new TreeMap<String, List<Mutation>>();
		mutation_map = new TreeMap<String, Map<String, List<Mutation>>>();
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
		
			IndexHandler.writeIndex(acmd.getMetaDataForManagedMemberAtPosition(fieldNumber),fieldNumber,objectProvider,row_key, value , mutation_map);

			
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

		
		IndexHandler.writeIndex(acmd.getMetaDataForManagedMemberAtPosition(fieldNumber),fieldNumber,objectProvider,row_key, value , mutation_map);

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
			
			IndexHandler.writeIndex(acmd.getMetaDataForManagedMemberAtPosition(fieldNumber),fieldNumber,objectProvider,row_key, value , mutation_map);


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
			
			IndexHandler.writeIndex(acmd.getMetaDataForManagedMemberAtPosition(fieldNumber),fieldNumber,objectProvider,row_key, value , mutation_map);


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

			IndexHandler.writeIndex(acmd.getMetaDataForManagedMemberAtPosition(fieldNumber),fieldNumber,objectProvider,row_key, value , mutation_map);

			
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

			IndexHandler.writeIndex(acmd.getMetaDataForManagedMemberAtPosition(fieldNumber),fieldNumber,objectProvider,row_key, value , mutation_map);

			
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

			IndexHandler.writeIndex(acmd.getMetaDataForManagedMemberAtPosition(fieldNumber),fieldNumber,objectProvider,row_key, value , mutation_map);

			
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

			IndexHandler.writeIndex(acmd.getMetaDataForManagedMemberAtPosition(fieldNumber),fieldNumber,objectProvider,row_key, value , mutation_map);

			
			oos.close();
			bos.close();
		} catch (IOException e) {
			throw new NucleusException(e.getMessage(), e);
		}
	}

	public void storeStringField(int fieldNumber, String value) {
		String columnName = CassandraUtils.getQualifierName(acmd, fieldNumber);

		if (value == null) {

			IndexHandler.deleteIndex(acmd.getMetaDataForManagedMemberAtPosition(fieldNumber),fieldNumber,objectProvider,row_key, value , mutation_map);
			
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

				IndexHandler.writeIndex(acmd.getMetaDataForManagedMemberAtPosition(fieldNumber),fieldNumber,objectProvider,row_key, value , mutation_map);
				
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
			ExecutionContext context = objectProvider.getExecutionContext();
			ClassLoaderResolver clr = context.getClassLoaderResolver();
			AbstractMemberMetaData fieldMetaData = acmd
					.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber);
			int relationType = fieldMetaData.getRelationType(clr);

			if (relationType == Relation.ONE_TO_ONE_BI
					|| relationType == Relation.ONE_TO_ONE_UNI
					|| relationType == Relation.MANY_TO_ONE_BI) {
				
				Object old_value = objectProvider.provideField(fieldNumber);
				IndexHandler.deleteIndex(fieldMetaData, fieldNumber, objectProvider, row_key,fieldNumber, mutation_map);
				
			}			
			
			
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
				
				IndexHandler.writeIndex(acmd.getMetaDataForManagedMemberAtPosition(fieldNumber),fieldNumber,objectProvider,row_key, valueId , mutation_map);

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

				ApiAdapter api = context.getApiAdapter();
				
				if (value instanceof Collection) {

					List<Object> mapping = new ArrayList<Object>();

					for (Object elem : (Collection) value) {	
						
						if(api.isPersistable(elem)){
							Object persisted = context.persistObjectInternal(elem,
									objectProvider, -1, StateManager.PC);
							Object valueId = context.getApiAdapter()
									.getIdForObject(persisted);
							mapping.add(valueId);
						}else{
							
							mapping.add(elem);
						}
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
				 else if (value instanceof Map)
                 {
                     // Process all keys, values of the Map that are PC
					 
					 Map<Object,Object> mapping = new TreeMap<Object,Object>();
					 
                     Map map = (Map)value;
                     
                     Set keys = map.keySet();
                     Iterator iter = keys.iterator();
                     while (iter.hasNext())
                     {
                         Object mapKey = iter.next();
                         Object key = null;
                         
                         if (api.isPersistable(mapKey))
                         {                      		
     						Object persisted = context.persistObjectInternal(mapKey,
     								objectProvider, -1, StateManager.PC);
     						key = context.getApiAdapter().getIdForObject(persisted);
                         }
                         else{                        	 
                        	key =  mapKey;
                         }
                         
                         Object mapValue =  map.get(key);
                         Object key_value = null;
                         
                         if (api.isPersistable(mapValue))
                         {
                           
                        	 Object persisted = context.persistObjectInternal(mapValue,
      								objectProvider, -1, StateManager.PC);
                        	 key_value = context.getApiAdapter().getIdForObject(persisted);              
                         }
                         else{                      	 
                        	 key_value = mapValue;
                         }
                         
                          mapping.put(key, key_value);                         
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

			//normal object;			
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
		for(Mutation mutation : mutations){
			addMutation(mutation, row_key, column_family, mutation_map);	
		}
		return mutation_map;
	}

}
