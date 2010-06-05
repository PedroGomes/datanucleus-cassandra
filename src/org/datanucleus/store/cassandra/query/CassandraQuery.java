package org.datanucleus.store.cassandra.query;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KeySlice;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.cassandra.thrift.Cassandra.Client;
import org.apache.thrift.TException;
import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.FetchPlan;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.store.ExecutionContext;
import org.datanucleus.store.ObjectProvider;
import org.datanucleus.store.Type;
import org.datanucleus.store.FieldValues2;
import org.datanucleus.store.cassandra.CassandraFetchFieldManager;
import org.datanucleus.store.cassandra.CassandraManagedConnection;
import org.datanucleus.store.cassandra.CassandraStoreManager;
import org.datanucleus.store.cassandra.CassandraUtils;

public class CassandraQuery {

	static List getObjectsOfCandidateType(final ExecutionContext ec,
			final CassandraManagedConnection mconn, Class candidateClass,
			boolean subclasses, boolean ignoreCache) {
		List results = new ArrayList();

		try {
			String keyspace = ((CassandraStoreManager) ec.getStoreManager())
					.getConnectionInfo().getKeyspace();
			final ClassLoaderResolver clr = ec.getClassLoaderResolver();
			final AbstractClassMetaData acmd = ec.getMetaDataManager()
					.getMetaDataForClass(candidateClass, clr);

			Client c = (Client) mconn.getConnection();
			String columnFamily = CassandraUtils.getFamilyName(acmd);
			ColumnParent parent = new ColumnParent();
			parent.setColumn_family(columnFamily);

			SlicePredicate slice_predicate = new SlicePredicate();
			int[] fieldNumbers = acmd.getAllMemberPositions();
			List<byte[]> column_names = new ArrayList<byte[]>();
			for (int i = 0; i < fieldNumbers.length; i++) {
				byte[] columnName = CassandraUtils.getQualifierName(acmd,
						fieldNumbers[i]).getBytes();
				column_names.add(columnName);
			}
			slice_predicate.setColumn_names(column_names);

			List<KeySlice> result = c.get_range_slice(keyspace, parent,
					slice_predicate, "", "", 3000, ConsistencyLevel.QUORUM);

			Iterator<KeySlice> iterator = result.iterator();

			while (iterator.hasNext()) {
				final KeySlice keySlice = (KeySlice) iterator.next();
				if (!keySlice.getColumns().isEmpty()) {

					results.add(ec.findObjectUsingAID(new Type(clr
							.classForName(acmd.getFullClassName())),
							new FieldValues2() {

								@Override
								public FetchPlan getFetchPlanForLoading() {
									return null;
								}

								@Override
								public void fetchNonLoadedFields(
										ObjectProvider sm) {
									sm.replaceNonLoadedFields(acmd
											.getAllMemberPositions(),
											new CassandraFetchFieldManager(
													acmd, sm, keySlice
															.getColumns()));

								}

								@Override
								public void fetchFields(ObjectProvider sm) {
									sm.replaceFields(acmd
											.getPKMemberPositions(),
											new CassandraFetchFieldManager(
													acmd, sm, keySlice
															.getColumns()));
									sm.replaceFields(acmd
											.getBasicMemberPositions(clr, ec
													.getMetaDataManager()),
											new CassandraFetchFieldManager(
													acmd, sm, keySlice
															.getColumns()));

								}
							}, ignoreCache, true));

				}
			}
		} catch (InvalidRequestException e) {
			throw new NucleusDataStoreException(e.getMessage(), e.getCause());
		} catch (UnavailableException e) {
			throw new NucleusDataStoreException(e.getMessage(), e.getCause());
		} catch (TimedOutException e) {
			throw new NucleusDataStoreException(e.getMessage(), e.getCause());
		} catch (TException e) {
			throw new NucleusDataStoreException(e.getMessage(), e.getCause());
		}
		return results;
	}

}
