package org.datanucleus.store.cassandra.index;



import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.jdo.identity.ByteIdentity;
import javax.jdo.identity.CharIdentity;
import javax.jdo.identity.IntIdentity;
import javax.jdo.identity.LongIdentity;
import javax.jdo.identity.StringIdentity;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.Deletion;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.thrift.SlicePredicate;
import org.datanucleus.ObjectManager;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.IndexMetaData;
import org.datanucleus.store.AbstractPersistenceHandler;
import org.datanucleus.store.ExecutionContext;
import org.datanucleus.store.ObjectProvider;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.cassandra.CassandraInsertFieldManager;
import org.datanucleus.store.cassandra.CassandraPersistenceHandler;
import org.datanucleus.store.cassandra.CassandraUtils;
import org.datanucleus.store.mapped.expression.FloatingPointLiteral;

public class IndexHandler {

	static AbstractPersistenceHandler persistenceHandler;
	
	public static void writeIndex(AbstractMemberMetaData metada,
			int fieldNumber, ObjectProvider objp, String row_key, Object field,
			Map<String, Map<String, List<Mutation>>> mutation_map) {

		IndexMetaData indexMetadata = metada.getIndexMetaData();
		if (indexMetadata == null || field ==null) {
			return;
		}

		// If it should maintain the index consistent, - it should be false only
		// for fields that don't change
		boolean unique = indexMetadata.isUnique();
		String index_column_family = indexMetadata.getTable();
		String index_key = getStringRepresentation(field);

		// find old value
		if (unique) {
			Object old_value = ((CassandraPersistenceHandler)persistenceHandler).getOldFieldValue(objp, fieldNumber);
						
			if (old_value != null) {

				if (getStringRepresentation(old_value).equals(index_key)) {
					return; // already indexed
				}
				// id not equal so delete old value from index

				String columnName = row_key;

				Deletion deletion = new Deletion(System.currentTimeMillis());
				SlicePredicate predicate = new SlicePredicate();
				List<byte[]> column_names = new ArrayList<byte[]>(1);
				try {
					column_names.add(columnName.getBytes("UTF8"));
				} catch (UnsupportedEncodingException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				predicate.setColumn_names(column_names);
				deletion.setPredicate(predicate);
				Mutation mutation = new Mutation();
				mutation.setDeletion(deletion);

				String old_key = getStringRepresentation(old_value);
				CassandraUtils.addMutation(mutation, old_key,
						index_column_family, mutation_map);

			}

		}

		Mutation mutation = new Mutation();
		
		byte[] row_bytes = null;
		try {
			 row_bytes = row_key.getBytes("UTF8");
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			return;
		}
		
		Column column = new Column(row_bytes, new byte[] {}, System
				.currentTimeMillis());
		ColumnOrSuperColumn column_or_supercolumn = new ColumnOrSuperColumn();
		column_or_supercolumn.setColumn(column);
		mutation.setColumn_or_supercolumn(column_or_supercolumn);
		CassandraUtils.addMutation(mutation, index_key,
				index_column_family, mutation_map);

	}

	public static void deleteIndex(AbstractMemberMetaData metada,
			int fieldNumber, ObjectProvider objp, String row_key, Object field,
			Map<String, Map<String, List<Mutation>>> mutation_map) {

		IndexMetaData indexMetadata = metada.getIndexMetaData();
		if (indexMetadata == null) {
			return;
		}

		// If it should maintain the index consistent, - it should be false only
		// for fields that don't change
		boolean unique = indexMetadata.isUnique();
		String index_column_family = indexMetadata.getTable();
		String index_key = getStringRepresentation(field);

		
		Object old_value = field;	

		if (old_value != null) {

			String columnName = row_key;	
			Deletion deletion = new Deletion(System.currentTimeMillis());
			SlicePredicate predicate = new SlicePredicate();
			List<byte[]> column_names = new ArrayList<byte[]>(1);
			try {
				column_names.add(columnName.getBytes("UTF8"));
			} catch (UnsupportedEncodingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			predicate.setColumn_names(column_names);
			deletion.setPredicate(predicate);
			Mutation mutation = new Mutation();
			mutation.setDeletion(deletion);

			String old_key = getStringRepresentation(old_value);
			

			
			CassandraUtils.addMutation(mutation, old_key,
					index_column_family, mutation_map);

		}

	}

	public static String getStringRepresentation(Object o) {

		if (o instanceof Long || o instanceof Integer || o instanceof Short
				|| o instanceof Float || o instanceof Double
				|| o instanceof String) {
			return o + "";
		}
		if(o instanceof StringIdentity){
			StringIdentity id = (StringIdentity) o;
			return id.getKey();
			
		}
		if(o instanceof LongIdentity){
			LongIdentity id = (LongIdentity) o;
			return id.getKey()+"";
			
		}
		if(o instanceof IntIdentity){
			IntIdentity id = (IntIdentity) o;
			return id.getKey()+"";
			
		}
		if(o instanceof CharIdentity){
			CharIdentity id = (CharIdentity) o;
			return id.getKey()+"";
			
		}
		if(o instanceof ByteIdentity){
			ByteIdentity id = (ByteIdentity) o;
			return id.getKey()+"";
			
		}
		
		
		return null;
	}

	public static void setPersistenceHandler(
			AbstractPersistenceHandler persistenceHandler) {
		IndexHandler.persistenceHandler = persistenceHandler;
	}
	
	

}
