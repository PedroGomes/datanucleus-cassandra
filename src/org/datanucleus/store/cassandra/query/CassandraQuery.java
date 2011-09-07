package org.datanucleus.store.cassandra.query;

import org.apache.cassandra.thrift.Cassandra.Client;
import org.apache.cassandra.thrift.*;
import org.apache.thrift.TException;
import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.FetchPlan;
import org.datanucleus.FetchPlanForClass;
import org.datanucleus.StateManager;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.Relation;
import org.datanucleus.state.StateManagerFactory;
import org.datanucleus.store.*;
import org.datanucleus.store.cassandra.*;

import javax.jdo.identity.SingleFieldIdentity;
import java.io.UnsupportedEncodingException;
import java.util.*;
import java.util.Map.Entry;

public class CassandraQuery {

	public static int search_slice_ratio = 1500; // should come from the

	// properties


	private static void fetchChildren(final ExecutionContext ec,
									  final CassandraManagedConnection mconn, final AbstractClassMetaData acmd,
									  List<Child_Info> child_info, boolean ignoreCache, FetchPlan fetchPlan) {


		try {
			String keyspace = ((CassandraStoreManager) ec.getStoreManager())
					.getConnectionInfo().getKeyspace();
			final ClassLoaderResolver clr = ec.getClassLoaderResolver();

			Client c = (Client) mconn.getConnection();
			String columnFamily = CassandraUtils.getFamilyName(acmd);

			//column parent
			ColumnParent parent = new ColumnParent();
			parent.setColumn_family(columnFamily);

			//all the columns to retrieve
			SlicePredicate slice_predicate = new SlicePredicate();
			int[] fieldNumbers = acmd.getAllMemberPositions();
			List<byte[]> column_names = new ArrayList<byte[]>(fieldNumbers.length);
			for (int i = 0; i < fieldNumbers.length; i++) {
				byte[] columnName = CassandraUtils.getQualifierName(acmd,
						fieldNumbers[i]).getBytes();
				column_names.add(columnName);
			}
			slice_predicate.setColumn_names(column_names);

			Map<String, List<Child_Info>> collected_entities_mapping = new TreeMap<String, List<Child_Info>>();

			int last_key = 0;
			boolean terminated = false;

			Map<String, List<ColumnOrSuperColumn>> result_set = new TreeMap<String, List<ColumnOrSuperColumn>>();

			while (!terminated) {

				Set<String> key_set = new HashSet<String>();

				int total_searched = 0;

				for (int index = last_key; index < child_info.size() && total_searched < search_slice_ratio; index++) {

					Object key_value = ConversionUtils.convertBytes(Object.class, child_info.get(index).getRaw_object());

					if (key_value instanceof javax.jdo.identity.SingleFieldIdentity) {

						javax.jdo.identity.SingleFieldIdentity identity = (SingleFieldIdentity) key_value;

						String key = ConversionUtils.ObjectToString(identity.getKeyAsObject());
						if (collected_entities_mapping.get(key) == null) {
							collected_entities_mapping.put(key, new LinkedList<Child_Info>());
						}
						collected_entities_mapping.get(key).add(child_info.get(index));
						key_set.add(key);
						last_key = index;
						total_searched++;


					} else {
						System.out.println("UPS!!");
						//nothing I can do for now
						return;
					}


				}

				List<String> keys = new ArrayList<String>();
				for (String key : key_set) {
					keys.add(key);
				}

				Map<String, List<ColumnOrSuperColumn>> slice_result_set = c.multiget_slice(keyspace, keys, parent, slice_predicate, ConsistencyLevel.QUORUM);


				for (String key : slice_result_set.keySet()) {
					result_set.put(key, slice_result_set.get(key));
				}

				if (total_searched < search_slice_ratio) {
					terminated = true;
				}
			}


			FetchPlanForClass fetchPlanForClass = fetchPlan.getFetchPlanForClass(acmd);

			//if null create
			if (fetchPlanForClass == null) {
				fetchPlanForClass = fetchPlan.manageFetchPlanForClass(acmd);
			}

			int[] fields_to_fetch = new int[]{};
			//if still null, an empty list avoids the load of the field
			if (fetchPlanForClass != null) {
				fields_to_fetch = fetchPlanForClass.getMemberNumbers();
			}

			//retrieve all the non basic fields that are in the fetch plan
			int[] all_fields = acmd.getAllMemberPositions();
			int[] basic_fields = acmd.getBasicMemberPositions(clr, ec.getMetaDataManager());
			LinkedList<Integer> non_basic_fields_list = new LinkedList<Integer>();
			for (int field : all_fields) {
				boolean found = false;
				for (int basic_field : basic_fields) {
					if (basic_field == field) {
						found = true;
						break;
					}
				}
				if (!found) {
					if (fields_to_fetch.length > 0) {
						for (int field_to_fetch : fields_to_fetch) {
							if (field_to_fetch == field) {
								found = true;
								break;
							}
						}
						if (found) {
							non_basic_fields_list.add(field);
						}
					}
				}
			}

			int[] non_basic_fields = new int[non_basic_fields_list.size()];


			for (int i = 0; i < non_basic_fields_list.size(); i++) {
				non_basic_fields[i] = non_basic_fields_list.get(i);
			}

			//Map to collect all keys
			Map<String, List<Child_Info>> collected_keys = new TreeMap<String, List<Child_Info>>();
			for (int non_basic_field : non_basic_fields) {
				String column_name = CassandraUtils.getQualifierName(acmd, non_basic_field);
				collected_keys.put(column_name, new ArrayList<Child_Info>(result_set.size()));
			}


			int number = 0;
			for (String key : collected_entities_mapping.keySet()) {
				number += collected_entities_mapping.get(key).size();
			}


			for (Entry<String, List<ColumnOrSuperColumn>> resultEntry : result_set.entrySet()) {

				final List<ColumnOrSuperColumn> columns = resultEntry.getValue();

				if (!columns.isEmpty()) {

					final Child_Info info = new Child_Info();

					for (int index = 0; index < columns.size(); index++) {
						ColumnOrSuperColumn columnOrSuperColumn = columns.get(index);
						String name = new String(columnOrSuperColumn.getColumn().name);
						//if non basic field
						if (collected_keys.containsKey(name)) {
							info.setRaw_object(columnOrSuperColumn.getColumn().getValue());
							collected_keys.get(name).add(info);
						}

					}


					Object returned = ec.findObjectUsingAID(new Type(clr
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
													acmd, sm, columns));
								}

								@Override
								public void fetchFields(ObjectProvider sm) {
									info.setObjectProvider(sm);
									sm.replaceFields(acmd
											.getPKMemberPositions(),
											new CassandraFetchFieldManager(
													acmd, sm, columns));
									sm.replaceFields(acmd
											.getBasicMemberPositions(clr, ec
													.getMetaDataManager()),
											new CassandraFetchFieldManager(
													acmd, sm, columns));


								}
							}, ignoreCache, true);

					List<Child_Info> child_infos = collected_entities_mapping.get(resultEntry.getKey());
					for (Child_Info entity_info : child_infos) {
						entity_info.setObject(returned);
					}
				}
			}

			for (int non_basic_field : non_basic_fields) {
				String column_name = CassandraUtils.getQualifierName(acmd, non_basic_field);
				AbstractMemberMetaData fieldMetaData = acmd
						.getMetaDataForManagedMemberAtAbsolutePosition(non_basic_field);

				int relationType = fieldMetaData.getRelationType(clr);

				if (relationType == Relation.ONE_TO_ONE_BI
						|| relationType == Relation.ONE_TO_ONE_UNI
						|| relationType == Relation.MANY_TO_ONE_BI) {

					AbstractClassMetaData child_metaData = ec.getMetaDataManager().getMetaDataForClass(fieldMetaData.getType(), clr);
					fetchChildren(ec, mconn, child_metaData, collected_keys.get(column_name), ignoreCache, fetchPlan);
					for (Child_Info info : collected_keys.get(column_name)) {
						info.applyToField(non_basic_field);
					}

				} else if (relationType == Relation.MANY_TO_MANY_BI
						|| relationType == Relation.ONE_TO_MANY_BI
						|| relationType == Relation.ONE_TO_MANY_UNI) {


					String elementClassName = fieldMetaData.getCollection()
							.getElementType();

					if (fieldMetaData.hasCollection()) {

						AbstractClassMetaData child_metaData = ec.getMetaDataManager().getMetaDataForClass(elementClassName, clr);
						fetchChildrenList(ec, mconn, child_metaData, collected_keys.get(column_name), ignoreCache, fetchPlan);
						for (Child_Info info : collected_keys.get(column_name)) {
							info.applyToField(non_basic_field);
						}
					}

//				 	if (fieldMetaData.hasMap()) {
//
//						AbstractClassMetaData child_metaData = ec.getMetaDataManager().getMetaDataForClass(elementClassName, clr);
//						fetchChildrenList(ec, mconn, child_metaData, collected_keys.get(column_name), ignoreCache);
//						for (Child_Info child_info : collected_keys.get(column_name)) {
//							child_info.applyToField(non_basic_field);
//						}
//					}
				}
			}


		} catch (InvalidRequestException
				e) {
			throw new NucleusDataStoreException(e.getMessage(), e.getCause());
		} catch (UnavailableException
				e) {
			throw new NucleusDataStoreException(e.getMessage(), e.getCause());
		} catch (TimedOutException
				e) {
			throw new NucleusDataStoreException(e.getMessage(), e.getCause());
		} catch (TException
				e) {
			throw new NucleusDataStoreException(e.getMessage(), e.getCause());
		} catch (Exception e) {
			throw new NucleusDataStoreException(e.getMessage(), e.getCause());
		}


	}


	private static void fetchChildrenList(final ExecutionContext ec,
										  final CassandraManagedConnection mconn, final AbstractClassMetaData acmd,
										  List<Child_Info> child_info, boolean ignoreCache, FetchPlan fetchPlan) {


		try {

			long t1 = System.currentTimeMillis();

			String keyspace = ((CassandraStoreManager) ec.getStoreManager())
					.getConnectionInfo().getKeyspace();
			final ClassLoaderResolver clr = ec.getClassLoaderResolver();

			Client c = (Client) mconn.getConnection();
			String columnFamily = CassandraUtils.getFamilyName(acmd);

			//column parent
			ColumnParent parent = new ColumnParent();
			parent.setColumn_family(columnFamily);

			//all the columns to retrieve
			SlicePredicate slice_predicate = new SlicePredicate();
			int[] fieldNumbers = acmd.getAllMemberPositions();
			List<byte[]> column_names = new ArrayList<byte[]>(fieldNumbers.length);
			for (int i = 0; i < fieldNumbers.length; i++) {
				byte[] columnName = CassandraUtils.getQualifierName(acmd,
						fieldNumbers[i]).getBytes();
				column_names.add(columnName);
			}
			slice_predicate.setColumn_names(column_names);

			Map<String, List<Child_Info>> collected_key_mapping = new TreeMap<String, List<Child_Info>>();
			Map<Child_Info, List<Object>> collected_entities_mapping = new TreeMap<Child_Info, List<Object>>();

			int last_key = 0;
			boolean terminated = false;

			Map<String, List<ColumnOrSuperColumn>> result_set = new TreeMap<String, List<ColumnOrSuperColumn>>();

			long t2 = System.currentTimeMillis();


			while (!terminated) {

				Set<String> key_set = new HashSet<String>();

				int total_searched = 0;

				for (int index = last_key; index < child_info.size() && total_searched < search_slice_ratio; index++) {

					Object key_values = ConversionUtils.convertBytes(Object.class, child_info.get(index).getRaw_object());

					List<Object> mapping = (List<Object>) key_values;

					for (Object key_value : mapping) {

						if (key_value instanceof javax.jdo.identity.SingleFieldIdentity) {

							javax.jdo.identity.SingleFieldIdentity identity = (SingleFieldIdentity) key_value;

							String key = ConversionUtils.ObjectToString(identity.getKeyAsObject());
							if (collected_key_mapping.get(key) == null) {
								collected_key_mapping.put(key, new LinkedList<Child_Info>());
							}
							collected_key_mapping.get(key).add(child_info.get(index));
							key_set.add(key);
							last_key = index;
							total_searched++;


						} else {
							System.out.println("UPS!!");
							//nothing I can do for now
							return;
						}

					}


				}

				List<String> keys = new ArrayList<String>();
				for (String key : key_set) {
					keys.add(key);
				}

				Map<String, List<ColumnOrSuperColumn>> slice_result_set = c.multiget_slice(keyspace, keys, parent, slice_predicate, ConsistencyLevel.QUORUM);


				for (String key : slice_result_set.keySet()) {
					result_set.put(key, slice_result_set.get(key));
				}

				if (total_searched < search_slice_ratio) {
					terminated = true;
				}
			}

			long t3 = System.currentTimeMillis();


			FetchPlanForClass fetchPlanForClass = fetchPlan.getFetchPlanForClass(acmd);

			//if null create
			if (fetchPlanForClass == null) {
				fetchPlanForClass = fetchPlan.manageFetchPlanForClass(acmd);
			}

			int[] fields_to_fetch = new int[]{};
			//if still null, an empty list avoids the load of the field
			if (fetchPlanForClass != null) {
				fields_to_fetch = fetchPlanForClass.getMemberNumbers();
			}

			//retrieve all the non basic fields that are in the fetch plan
			int[] all_fields = acmd.getAllMemberPositions();
			int[] basic_fields = acmd.getBasicMemberPositions(clr, ec.getMetaDataManager());
			LinkedList<Integer> non_basic_fields_list = new LinkedList<Integer>();
			for (int field : all_fields) {
				boolean found = false;
				for (int basic_field : basic_fields) {
					if (basic_field == field) {
						found = true;
						break;
					}
				}
				if (!found) {
					if (fields_to_fetch.length > 0) {
						for (int field_to_fetch : fields_to_fetch) {
							if (field_to_fetch == field) {
								found = true;
								break;
							}
						}
						if (found) {
							non_basic_fields_list.add(field);
						}
					}
				}
			}

			int[] non_basic_fields = new int[non_basic_fields_list.size()];


			for (int i = 0; i < non_basic_fields_list.size(); i++) {
				non_basic_fields[i] = non_basic_fields_list.get(i);
			}

			//Map to collect all keys
			Map<String, List<Child_Info>> collected_keys = new TreeMap<String, List<Child_Info>>();
			for (int non_basic_field : non_basic_fields) {
				String column_name = CassandraUtils.getQualifierName(acmd, non_basic_field);
				collected_keys.put(column_name, new ArrayList<Child_Info>(result_set.size()));
			}

			long t4 = System.currentTimeMillis();


			for (Entry<String, List<ColumnOrSuperColumn>> resultEntry : result_set.entrySet()) {

				final List<ColumnOrSuperColumn> columns = resultEntry.getValue();

				if (!columns.isEmpty()) {


					final Child_Info info = new Child_Info();

					for (int index = 0; index < columns.size(); index++) {
						ColumnOrSuperColumn columnOrSuperColumn = columns.get(index);
						String name = new String(columnOrSuperColumn.getColumn().name);
						//if non basic field
						if (collected_keys.containsKey(name)) {
							info.setRaw_object(columnOrSuperColumn.getColumn().getValue());
							collected_keys.get(name).add(info);
						}

					}


					Object returned = ec.findObjectUsingAID(new Type(clr
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
													acmd, sm, columns));
								}

								@Override
								public void fetchFields(ObjectProvider sm) {
									info.setObjectProvider(sm);
									sm.replaceFields(acmd
											.getPKMemberPositions(),
											new CassandraFetchFieldManager(
													acmd, sm, columns));
									sm.replaceFields(acmd
											.getBasicMemberPositions(clr, ec
													.getMetaDataManager()),
											new CassandraFetchFieldManager(
													acmd, sm, columns));


								}
							}, ignoreCache, false);

					List<Child_Info> child_infos = collected_key_mapping.get(resultEntry.getKey());
					for (Child_Info entity_info : child_infos) {
						if (!collected_entities_mapping.containsKey(entity_info)) {
							collected_entities_mapping.put(entity_info, new LinkedList<Object>());
						}
						collected_entities_mapping.get(entity_info).add(returned);
					}
				}
			}
			long t5 = System.currentTimeMillis();

	//		System.out.println("ON CHILDREN LIST: " + (t2 - t1) + "||" + (t3 - t2) + "||" + (t4 - t3) + " || " + (t5 - t4));

			for (int non_basic_field : non_basic_fields) {
				String column_name = CassandraUtils.getQualifierName(acmd, non_basic_field);
				AbstractMemberMetaData fieldMetaData = acmd
						.getMetaDataForManagedMemberAtAbsolutePosition(non_basic_field);

				int relationType = fieldMetaData.getRelationType(clr);

				if (relationType == Relation.ONE_TO_ONE_BI
						|| relationType == Relation.ONE_TO_ONE_UNI
						|| relationType == Relation.MANY_TO_ONE_BI) {

					AbstractClassMetaData child_metaData = ec.getMetaDataManager().getMetaDataForClass(fieldMetaData.getType(), clr);
					fetchChildren(ec, mconn, child_metaData, collected_keys.get(column_name), ignoreCache, fetchPlan);
					for (Child_Info info : collected_keys.get(column_name)) {
						info.applyToField(non_basic_field);
					}

				} else if (relationType == Relation.MANY_TO_MANY_BI
						|| relationType == Relation.ONE_TO_MANY_BI
						|| relationType == Relation.ONE_TO_MANY_UNI) {


					String elementClassName = fieldMetaData.getCollection()
							.getElementType();

					if (fieldMetaData.hasCollection()) {

						AbstractClassMetaData child_metaData = ec.getMetaDataManager().getMetaDataForClass(elementClassName, clr);
						fetchChildrenList(ec, mconn, child_metaData, collected_keys.get(column_name), ignoreCache, fetchPlan);
						for (Child_Info info : collected_keys.get(column_name)) {
							info.applyToField(non_basic_field);
						}
					}

//				 	if (fieldMetaData.hasMap()) {
//
//						AbstractClassMetaData child_metaData = ec.getMetaDataManager().getMetaDataForClass(elementClassName, clr);
//						fetchChildrenList(ec, mconn, child_metaData, collected_keys.get(column_name), ignoreCache);
//						for (Child_Info child_info : collected_keys.get(column_name)) {
//							child_info.applyToField(non_basic_field);
//						}
//					}
				}
			}


			for (Entry<Child_Info, List<Object>> child_infoListEntry : collected_entities_mapping.entrySet()) {
				child_infoListEntry.getKey().setObject(child_infoListEntry.getValue());
			}

		} catch (InvalidRequestException
				e) {
			throw new NucleusDataStoreException(e.getMessage(), e.getCause());
		} catch (UnavailableException
				e) {
			throw new NucleusDataStoreException(e.getMessage(), e.getCause());
		} catch (TimedOutException
				e) {
			throw new NucleusDataStoreException(e.getMessage(), e.getCause());
		} catch (TException
				e) {
			throw new NucleusDataStoreException(e.getMessage(), e.getCause());
		} catch (Exception e) {
			throw new NucleusDataStoreException(e.getMessage(), e.getCause());
		}


	}


	//get all or range
	static List getObjectsOfCandidateType(final ExecutionContext ec,
										  final CassandraManagedConnection mconn, Class candidateClass,
										  FetchPlan fetchPlan, boolean subclasses, boolean ignoreCache, long fromInclNo,
										  long toExclNo) {


		List results = new ArrayList();

		try {
			String keyspace = ((CassandraStoreManager) ec.getStoreManager())
					.getConnectionInfo().getKeyspace();
			final ClassLoaderResolver clr = ec.getClassLoaderResolver();
			final AbstractClassMetaData acmd = ec.getMetaDataManager()
					.getMetaDataForClass(candidateClass, clr);

			Client c = (Client) mconn.getConnection();
			String columnFamily = CassandraUtils.getFamilyName(acmd);

			//column parent
			ColumnParent parent = new ColumnParent();
			parent.setColumn_family(columnFamily);

			//all the columns to retrieve
			SlicePredicate slice_predicate = new SlicePredicate();
			int[] fieldNumbers = acmd.getAllMemberPositions();
			List<byte[]> column_names = new ArrayList<byte[]>(fieldNumbers.length);
			for (int i = 0; i < fieldNumbers.length; i++) {
				byte[] columnName = CassandraUtils.getQualifierName(acmd,
						fieldNumbers[i]).getBytes();
				column_names.add(columnName);
			}
			slice_predicate.setColumn_names(column_names);

			KeyRange range = new KeyRange();
			range.setCount(search_slice_ratio);
			range.setStart_key("");
			range.setEnd_key("");

			String last_key = "";
			int number_keys = 0;
			boolean terminated = false;

			long limit = toExclNo;// (toExclNo<0) ? -1 : (toExclNo-1);
			List<KeySlice> result = new LinkedList<KeySlice>();
			long t1 = System.currentTimeMillis();


			while (!terminated) {
				List<KeySlice> keys = c.get_range_slices(keyspace, parent,
						slice_predicate, range,
						ConsistencyLevel.QUORUM);

				if (!keys.isEmpty()) {
					last_key = keys.get(keys.size() - 1).key;
					range.setStart_key(last_key);
				}

				for (KeySlice key : keys) {
					if (!key.getColumns().isEmpty()) {
						number_keys++;
						if (number_keys > fromInclNo) {
							result.add(key);
						}

					}
					if (number_keys >= limit) {
						terminated = true;
						break;
					}

				}
				if (keys.size() < search_slice_ratio) {
					terminated = true;
				}
			}
			long t2 = System.currentTimeMillis();

			int id = (int) (Math.random() * 1000);

//			if (acmd.getEntityName().equals("Order") && result.size() > 4000) {
//				System.out.println(">" + id + ">: " + result.size() + " ORDERS LOADED, time: " + (t2 - t1) + " limite: " + limit);
//			}


			FetchPlanForClass fetchPlanForClass = fetchPlan.getFetchPlanForClass(acmd);

			//if null create
			if (fetchPlanForClass == null) {
				fetchPlanForClass = fetchPlan.manageFetchPlanForClass(acmd);
			}

			int[] fields_to_fetch = new int[]{};
			//if still null, an empty list avoids the load of the field
			if (fetchPlanForClass != null) {
				fields_to_fetch = fetchPlanForClass.getMemberNumbers();
			}

			//retrieve all the non basic fields that are in the fetch plan
			int[] all_fields = acmd.getAllMemberPositions();
			int[] basic_fields = acmd.getBasicMemberPositions(clr, ec.getMetaDataManager());
			LinkedList<Integer> non_basic_fields_list = new LinkedList<Integer>();
			for (int field : all_fields) {
				boolean found = false;
				for (int basic_field : basic_fields) {
					if (basic_field == field) {
						found = true;
						break;
					}
				}
				if (!found) {
					if (fields_to_fetch.length > 0) {
						for (int field_to_fetch : fields_to_fetch) {
							if (field_to_fetch == field) {
								found = true;
								break;
							}
						}
						if (found) {
							non_basic_fields_list.add(field);
						}
					}
				}
			}

			int[] non_basic_fields = new int[non_basic_fields_list.size()];


			for (int i = 0; i < non_basic_fields_list.size(); i++) {
				non_basic_fields[i] = non_basic_fields_list.get(i);
			}

			//Map to collect all keys
			Map<String, List<Child_Info>> collected_keys = new TreeMap<String, List<Child_Info>>();
			for (int non_basic_field : non_basic_fields) {
				String column_name = CassandraUtils.getQualifierName(acmd, non_basic_field);
				collected_keys.put(column_name, new ArrayList<Child_Info>(result.size()));
			}


			Iterator<KeySlice> iterator = result.iterator();

			while (iterator.hasNext()) {
				final KeySlice keySlice = iterator.next();


				if (!keySlice.getColumns().isEmpty()) {

					List<ColumnOrSuperColumn> columns = keySlice.getColumns();

					final Child_Info info = new Child_Info();

					for (int index = 0; index < columns.size(); index++) {
						ColumnOrSuperColumn columnOrSuperColumn = columns.get(index);
						String name = new String(columnOrSuperColumn.getColumn().name);
						//if non basic field
						if (collected_keys.containsKey(name)) {
							info.setRaw_object(columnOrSuperColumn.getColumn().getValue());
							collected_keys.get(name).add(info);
						}

					}


//					FieldValues fv = new FieldValues() {
//
//						@Override
//						public void fetchFields(StateManager sm) {
//							sm.getObjectProvider().replaceNonLoadedFields(acmd
//									.getAllMemberPositions(),
//									new CassandraFetchFieldManager(
//											acmd, sm.getObjectProvider()
//											, keySlice
//											.getColumns()));
//						}
//
//						@Override
//						public void fetchNonLoadedFields(StateManager sm) {
//							info.setObjectProvider(sm.getObjectProvider());
//							sm.getObjectProvider().replaceFields(acmd
//									.getPKMemberPositions(),
//									new CassandraFetchFieldManager(
//											acmd, sm.getObjectProvider(), keySlice
//											.getColumns()));
//							sm.getObjectProvider().replaceFields(acmd
//									.getBasicMemberPositions(clr, ec
//											.getMetaDataManager()),
//									new CassandraFetchFieldManager(
//											acmd, sm.getObjectProvider(), keySlice
//											.getColumns()));
//						}
//
//						@Override
//						public FetchPlan getFetchPlanForLoading() {
//							return null;
//						}
//
//
//					};
//
//					StateManager sm = StateManagerFactory.newStateManagerForHollowPopulatedAppId(ec, new Type(clr
//							.classForName(acmd.getFullClassName())).getType(), fv);

//					results.add(sm.getObject());


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
									info.setObjectProvider(sm);
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


			for (int non_basic_field : non_basic_fields) {
				String column_name = CassandraUtils.getQualifierName(acmd, non_basic_field);
				AbstractMemberMetaData fieldMetaData = acmd
						.getMetaDataForManagedMemberAtAbsolutePosition(non_basic_field);

				int relationType = fieldMetaData.getRelationType(clr);

				if (relationType == Relation.ONE_TO_ONE_BI
						|| relationType == Relation.ONE_TO_ONE_UNI
						|| relationType == Relation.MANY_TO_ONE_BI) {

					AbstractClassMetaData child_metaData = ec.getMetaDataManager().getMetaDataForClass(fieldMetaData.getType(), clr);

					fetchChildren(ec, mconn, child_metaData, collected_keys.get(column_name), ignoreCache, fetchPlan);
					for (Child_Info child_info : collected_keys.get(column_name)) {
						child_info.applyToField(non_basic_field);
					}

				} else if (relationType == Relation.MANY_TO_MANY_BI
						|| relationType == Relation.ONE_TO_MANY_BI
						|| relationType == Relation.ONE_TO_MANY_UNI) {


					String elementClassName = fieldMetaData.getCollection()
							.getElementType();

					if (fieldMetaData.hasCollection()) {
						long t3 = System.currentTimeMillis();

						AbstractClassMetaData child_metaData = ec.getMetaDataManager().getMetaDataForClass(elementClassName, clr);
						fetchChildrenList(ec, mconn, child_metaData, collected_keys.get(column_name), ignoreCache, fetchPlan);

						long t4 = System.currentTimeMillis();


						for (Child_Info child_info : collected_keys.get(column_name)) {
							child_info.applyToField(non_basic_field);
						}

						long t5 = System.currentTimeMillis();

//						System.out.println("Fetch children : " + (t4 - t3) + "||" + (t5 - t4));

					}

//				 	if (fieldMetaData.hasMap()) {
//
//						AbstractClassMetaData child_metaData = ec.getMetaDataManager().getMetaDataForClass(elementClassName, clr);
//						fetchChildrenList(ec, mconn, child_metaData, collected_keys.get(column_name), ignoreCache);
//						for (Child_Info child_info : collected_keys.get(column_name)) {
//							child_info.applyToField(non_basic_field);
//						}
//					}
				}
			}


			if (acmd.getEntityName().contains("Order")) {
				System.out.println();
			}

			result = null;

		} catch (InvalidRequestException
				e)

		{
			throw new NucleusDataStoreException(e.getMessage(), e.getCause());
		} catch (UnavailableException
				e)

		{
			throw new NucleusDataStoreException(e.getMessage(), e.getCause());
		} catch (TimedOutException
				e)

		{
			throw new NucleusDataStoreException(e.getMessage(), e.getCause());
		} catch (TException
				e)

		{
			throw new NucleusDataStoreException(e.getMessage(), e.getCause());
		}

		return results;
	}


	//greater than -> greater = true | lower than -> greater = false

	static List getObjectsOfCandidateTypeGreaterOrLowerThan(final ExecutionContext ec,
															final CassandraManagedConnection mconn, Class candidateClass,
															boolean subclasses, boolean ignoreCache, long toExclNo, boolean greater, String base) {
		List results = new ArrayList();

		long initial_time = System.currentTimeMillis();

		try {
			String keyspace = ((CassandraStoreManager) ec.getStoreManager())
					.getConnectionInfo().getKeyspace();
			final ClassLoaderResolver clr = ec.getClassLoaderResolver();
			final AbstractClassMetaData acmd = ec.getMetaDataManager()
					.getMetaDataForClass(candidateClass, clr);

			Client c = (Client) mconn.getConnection();
			String columnFamily = CassandraUtils.getFamilyName(acmd);

			//column parent
			ColumnParent parent = new ColumnParent();
			parent.setColumn_family(columnFamily);

			//all the columns to retrieve
			SlicePredicate slice_predicate = new SlicePredicate();
			int[] fieldNumbers = acmd.getAllMemberPositions();
			List<byte[]> column_names = new ArrayList<byte[]>(fieldNumbers.length);
			for (int i = 0; i < fieldNumbers.length; i++) {
				byte[] columnName = CassandraUtils.getQualifierName(acmd,
						fieldNumbers[i]).getBytes();
				column_names.add(columnName);
			}
			slice_predicate.setColumn_names(column_names);


			KeyRange range = new KeyRange();
			range.setCount(search_slice_ratio);

			if (greater) {
				range.setStart_key(base);
				range.setEnd_key("");
			} else {
				range.setStart_key("");
				range.setEnd_key(base);
			}

			String last_key = "";
			int number_keys = 0;
			boolean terminated = false;

			long limit = toExclNo;// (toExclNo<0) ? -1 : (toExclNo-1);
			List<KeySlice> result = new LinkedList<KeySlice>();
			while (!terminated) {
				List<KeySlice> keys = c.get_range_slices(keyspace, parent,
						slice_predicate, range,
						ConsistencyLevel.QUORUM);

				if (!keys.isEmpty()) {
					last_key = keys.get(keys.size() - 1).key;
					range.setStart_key(last_key);
				}

				for (KeySlice key : keys) {
					if (!key.getColumns().isEmpty()) {
						number_keys++;
						result.add(key);
					}
					if (number_keys >= limit) {
						terminated = true;
						break;
					}

				}
				if (keys.size() < search_slice_ratio) {
					terminated = true;
				}
			}


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
			long timeout = System.currentTimeMillis();
			System.out.println(">>Timeout on range query: " + (timeout - initial_time));
			throw new NucleusDataStoreException(e.getMessage(), e.getCause());
		} catch (TException e) {
			throw new NucleusDataStoreException(e.getMessage(), e.getCause());
		}
		return results;
	}

	//not used (range between two defined keys)
	static List getObjectsOfCandidateType(final ExecutionContext ec,
										  final CassandraManagedConnection mconn, Class candidateClass,
										  boolean subclasses, boolean ignoreCache, String fromInclNo,
										  String toExclNo) {
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
			List<byte[]> column_names = new ArrayList<byte[]>(fieldNumbers.length);
			for (int i = 0; i < fieldNumbers.length; i++) {
				byte[] columnName = CassandraUtils.getQualifierName(acmd,
						fieldNumbers[i]).getBytes();
				column_names.add(columnName);
			}
			slice_predicate.setColumn_names(column_names);

			String last_key = "";
			int number_keys = 0;
			boolean terminated = false;

			String limit = toExclNo;// (toExclNo<0) ? -1 : (toExclNo-1);
			List<KeySlice> result = new LinkedList<KeySlice>();

			KeyRange range = new KeyRange();
			range.setStart_key(fromInclNo);
			range.setEnd_key(toExclNo);
			range.setCount(search_slice_ratio);

			while (!terminated) {
				List<KeySlice> keys = c.get_range_slices(keyspace, parent,
						slice_predicate, range, ConsistencyLevel.QUORUM);

				if (!keys.isEmpty()) {
					range.setStart_key(keys.get(keys.size() - 1).key);
				}

				for (KeySlice key : keys) {
					if (!key.getColumns().isEmpty()) {
						number_keys++;
						result.add(key);
					}
				}
				if (keys.size() < search_slice_ratio) {
					terminated = true;
				}
			}

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

	//get from index
	static List getObjectsOfCandidateType(final ExecutionContext ec,
										  final CassandraManagedConnection mconn, Class candidateClass,
										  boolean subclasses, boolean ignoreCache, long fromInclNo,
										  long toExclNo, String index_table, String value) {
		List results = new ArrayList();
//        if(toExclNo==1){
//          Exception exception = new Exception("");
//          exception.printStackTrace();
//        }


		try {
			String keyspace = ((CassandraStoreManager) ec.getStoreManager())
					.getConnectionInfo().getKeyspace();
			final ClassLoaderResolver clr = ec.getClassLoaderResolver();
			final AbstractClassMetaData acmd = ec.getMetaDataManager()
					.getMetaDataForClass(candidateClass, clr);

			Client c = (Client) mconn.getConnection();

			ColumnParent parent = new ColumnParent();
			parent.setColumn_family(index_table);

			SlicePredicate slice_predicate = new SlicePredicate();

			if (fromInclNo + toExclNo > Integer.MAX_VALUE) {
				toExclNo = Integer.MAX_VALUE - fromInclNo - 1;
			}

			SliceRange range = new SliceRange("".getBytes(), "".getBytes(),
					false, ((int) (fromInclNo + toExclNo)));
			slice_predicate.setSlice_range(range);

			List<ColumnOrSuperColumn> key_columns = c.get_slice(keyspace,
					value, parent, slice_predicate, ConsistencyLevel.QUORUM);
			List<String> keys = new LinkedList<String>();

			int number_keys = 0;
			for (ColumnOrSuperColumn index_column : key_columns) {
				number_keys++;
				if (number_keys > fromInclNo) {
					byte[] column_name = index_column.getColumn().name;
					String key;
					try {
						key = new String(column_name, "UTF8");
						keys.add(key);
					} catch (UnsupportedEncodingException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
						continue;
					}

				}
			}

			slice_predicate = new SlicePredicate();
			int[] fieldNumbers = acmd.getAllMemberPositions();
			List<byte[]> column_names = new ArrayList<byte[]>(fieldNumbers.length);
			for (int i = 0; i < fieldNumbers.length; i++) {
				byte[] columnName = CassandraUtils.getQualifierName(acmd,
						fieldNumbers[i]).getBytes();
				column_names.add(columnName);
			}
			slice_predicate.setColumn_names(column_names);

			String columnFamily = CassandraUtils.getFamilyName(acmd);
			parent = new ColumnParent();
			parent.setColumn_family(columnFamily);

			Map<String, List<ColumnOrSuperColumn>> result = new TreeMap<String, List<ColumnOrSuperColumn>>();

			Map<String, List<ColumnOrSuperColumn>> entities = c.multiget_slice(
					keyspace, keys, parent, slice_predicate,
					ConsistencyLevel.QUORUM);


			// List<KeySlice> entities = c.get_range_slice(keyspace, parent,
			// slice_predicate, last_key, "", search_slice_ratio,
			// ConsistencyLevel.QUORUM);

			for (Entry<String, List<ColumnOrSuperColumn>> element : entities
					.entrySet()) {

				String key = element.getKey();
				List<ColumnOrSuperColumn> columns = element.getValue();

				if (!columns.isEmpty()) {
					result.put(key, columns);
				}
			}


			for (Entry<String, List<ColumnOrSuperColumn>> element : result
					.entrySet()) {

				final List<ColumnOrSuperColumn> columns = element.getValue();

				results.add(ec.findObjectUsingAID(new Type(clr
						.classForName(acmd.getFullClassName())),
						new FieldValues2() {

							@Override
							public FetchPlan getFetchPlanForLoading() {
								return null;
							}

							@Override
							public void fetchNonLoadedFields(ObjectProvider sm) {
								sm.replaceNonLoadedFields(acmd
										.getAllMemberPositions(),
										new CassandraFetchFieldManager(acmd,
												sm, columns));
							}

							@Override
							public void fetchFields(ObjectProvider sm) {
								sm.replaceFields(acmd.getPKMemberPositions(),
										new CassandraFetchFieldManager(acmd,
												sm, columns));
								sm.replaceFields(acmd.getBasicMemberPositions(
										clr, ec.getMetaDataManager()),
										new CassandraFetchFieldManager(acmd,
												sm, columns));

							}
						}, ignoreCache, true));

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


	//get all or range
	static List<byte[]> getKeysOfCandidateType(final ExecutionContext ec,
											   final CassandraManagedConnection mconn, Class candidateClass,
											   boolean subclasses, boolean ignoreCache, long fromInclNo,
											   long toExclNo) {


		List<byte[]> results = new LinkedList<byte[]>();

		try {
			String keyspace = ((CassandraStoreManager) ec.getStoreManager())
					.getConnectionInfo().getKeyspace();
			final ClassLoaderResolver clr = ec.getClassLoaderResolver();
			final AbstractClassMetaData acmd = ec.getMetaDataManager()
					.getMetaDataForClass(candidateClass, clr);

			Client c = (Client) mconn.getConnection();
			String columnFamily = CassandraUtils.getFamilyName(acmd);

			//column parent
			ColumnParent parent = new ColumnParent();
			parent.setColumn_family(columnFamily);

			//all the columns to retrieve
			SlicePredicate slice_predicate = new SlicePredicate();
			List<byte[]> column_names = new ArrayList<byte[]>(1);
			column_names.add(CassandraUtils.getKeyColumn(acmd));
//            for (int i = 0; i < fieldNumbers.length; i++) {
//				byte[] columnName = CassandraUtils.getQualifierName(acmd,
//						fieldNumbers[i]).getBytes();
//				column_names.add(columnName);
//			}
			slice_predicate.setColumn_names(column_names);

			KeyRange range = new KeyRange();
			range.setCount(search_slice_ratio);
			range.setStart_key("");
			range.setEnd_key("");

			String last_key = "";
			int number_keys = 0;
			boolean terminated = false;

			long limit = toExclNo;// (toExclNo<0) ? -1 : (toExclNo-1);
			List<KeySlice> result = new LinkedList<KeySlice>();

			while (!terminated) {
				List<KeySlice> keys = c.get_range_slices(keyspace, parent,
						slice_predicate, range,
						ConsistencyLevel.QUORUM);

				if (!keys.isEmpty()) {
					last_key = keys.get(keys.size() - 1).key;
					range.setStart_key(last_key);
				}

				for (KeySlice key : keys) {
					if (!key.getColumns().isEmpty()) {
						number_keys++;
						if (number_keys > fromInclNo) {
							result.add(key);
						}

					}
					if (number_keys >= limit) {
						terminated = true;
						break;
					}

				}
				if (keys.size() < search_slice_ratio) {
					terminated = true;
				}
			}

			if (result.size() > 11000) {
				System.out.println("LInked list to big (" + result.size() + ") on: " + parent.getColumn_family() + " : " + fromInclNo + " | " + toExclNo);
				throw new NucleusDataStoreException("LInked list to big (" + result.size() + ") on: " + parent.getColumn_family() + " : " + fromInclNo + " | " + toExclNo);
			}

			Iterator<KeySlice> iterator = result.iterator();


			while (iterator.hasNext()) {
				final KeySlice keySlice = iterator.next();
				if (!keySlice.getColumns().isEmpty()) {
					results.add(keySlice.getColumns().get(0).getColumn().getValue());
				}
			}
			result = null;

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


	//greater than -> greater = true | lower than -> greater = false
	static List<byte[]> getKeysOfCandidateTypeGreaterOrLowerThan(final ExecutionContext ec,
																 final CassandraManagedConnection mconn, Class candidateClass,
																 boolean subclasses, boolean ignoreCache, long toExclNo, boolean greater, String base) {
		List<byte[]> results = new LinkedList<byte[]>();

		long initial_time = System.currentTimeMillis();

		try {
			String keyspace = ((CassandraStoreManager) ec.getStoreManager())
					.getConnectionInfo().getKeyspace();
			final ClassLoaderResolver clr = ec.getClassLoaderResolver();
			final AbstractClassMetaData acmd = ec.getMetaDataManager()
					.getMetaDataForClass(candidateClass, clr);

			Client c = (Client) mconn.getConnection();
			String columnFamily = CassandraUtils.getFamilyName(acmd);

			//column parent
			ColumnParent parent = new ColumnParent();
			parent.setColumn_family(columnFamily);

			//all the columns to retrieve
			SlicePredicate slice_predicate = new SlicePredicate();
			List<byte[]> column_names = new ArrayList<byte[]>(1);
			column_names.add(CassandraUtils.getKeyColumn(acmd));
//            for (int i = 0; i < fieldNumbers.length; i++) {
//				byte[] columnName = CassandraUtils.getQualifierName(acmd,
//						fieldNumbers[i]).getBytes();
//				column_names.add(columnName);
//			}
			slice_predicate.setColumn_names(column_names);


			KeyRange range = new KeyRange();
			range.setCount(search_slice_ratio);

			if (greater) {
				range.setStart_key(base);
				range.setEnd_key("");
			} else {
				range.setStart_key("");
				range.setEnd_key(base);
			}

			String last_key = "";
			int number_keys = 0;
			boolean terminated = false;

			long limit = toExclNo;// (toExclNo<0) ? -1 : (toExclNo-1);
			List<KeySlice> result = new LinkedList<KeySlice>();
			while (!terminated) {
				List<KeySlice> keys = c.get_range_slices(keyspace, parent,
						slice_predicate, range,
						ConsistencyLevel.QUORUM);

				if (!keys.isEmpty()) {
					last_key = keys.get(keys.size() - 1).key;
					range.setStart_key(last_key);
				}

				for (KeySlice key : keys) {
					if (!key.getColumns().isEmpty()) {
						number_keys++;
						result.add(key);
					}
					if (number_keys >= limit) {
						terminated = true;
						break;
					}

				}
				if (keys.size() < search_slice_ratio) {
					terminated = true;
				}
			}

			Iterator<KeySlice> iterator = result.iterator();

			while (iterator.hasNext()) {
				final KeySlice keySlice = (KeySlice) iterator.next();
				if (!keySlice.getColumns().isEmpty()) {
					results.add(keySlice.getColumns().get(0).getColumn().getValue());
				}
			}
		} catch (InvalidRequestException e) {
			throw new NucleusDataStoreException(e.getMessage(), e.getCause());
		} catch (UnavailableException e) {
			throw new NucleusDataStoreException(e.getMessage(), e.getCause());
		} catch (TimedOutException e) {
			long timeout = System.currentTimeMillis();
			System.out.println(">>Timeout on range query: " + (timeout - initial_time));
			throw new NucleusDataStoreException(e.getMessage(), e.getCause());
		} catch (TException e) {
			throw new NucleusDataStoreException(e.getMessage(), e.getCause());
		}
		return results;
	}


	//get from index
	static List<byte[]> getKeysOfCandidateTypeFromIndex(final ExecutionContext ec,
														final CassandraManagedConnection mconn, Class candidateClass,
														boolean subclasses, boolean ignoreCache, long fromInclNo,
														long toExclNo, String index_table, String value) {

		List<byte[]> results = new LinkedList<byte[]>();
//        if(toExclNo==1){
//          Exception exception = new Exception("");
//          exception.printStackTrace();
//        }


		try {
			String keyspace = ((CassandraStoreManager) ec.getStoreManager())
					.getConnectionInfo().getKeyspace();
			final ClassLoaderResolver clr = ec.getClassLoaderResolver();
			final AbstractClassMetaData acmd = ec.getMetaDataManager()
					.getMetaDataForClass(candidateClass, clr);

			Client c = (Client) mconn.getConnection();

			ColumnParent parent = new ColumnParent();
			parent.setColumn_family(index_table);

			SlicePredicate slice_predicate = new SlicePredicate();

			if (fromInclNo + toExclNo > Integer.MAX_VALUE) {
				toExclNo = Integer.MAX_VALUE - fromInclNo - 1;
			}

			SliceRange range = new SliceRange("".getBytes(), "".getBytes(),
					false, ((int) (fromInclNo + toExclNo)));
			slice_predicate.setSlice_range(range);

			List<ColumnOrSuperColumn> key_columns = c.get_slice(keyspace,
					value, parent, slice_predicate, ConsistencyLevel.QUORUM);
			List<String> keys = new LinkedList<String>();

			int number_keys = 0;
			for (ColumnOrSuperColumn index_column : key_columns) {
				number_keys++;
				if (number_keys > fromInclNo) {
					byte[] column_name = index_column.getColumn().name;
					String key;
					try {
						key = new String(column_name, "UTF8");
						keys.add(key);
					} catch (UnsupportedEncodingException e) {
						e.printStackTrace();
						continue;
					}

				}
			}


			slice_predicate = new SlicePredicate();
			List<byte[]> column_names = new ArrayList<byte[]>(1);
			column_names.add(CassandraUtils.getKeyColumn(acmd));
//            for (int i = 0; i < fieldNumbers.length; i++) {
//				byte[] columnName = CassandraUtils.getQualifierName(acmd,
//						fieldNumbers[i]).getBytes();
//				column_names.add(columnName);
//			}
			slice_predicate.setColumn_names(column_names);

			String columnFamily = CassandraUtils.getFamilyName(acmd);
			parent = new ColumnParent();
			parent.setColumn_family(columnFamily);

			Map<String, List<ColumnOrSuperColumn>> result = new TreeMap<String, List<ColumnOrSuperColumn>>();

			Map<String, List<ColumnOrSuperColumn>> entities = c.multiget_slice(
					keyspace, keys, parent, slice_predicate,
					ConsistencyLevel.QUORUM);


			// List<KeySlice> entities = c.get_range_slice(keyspace, parent,
			// slice_predicate, last_key, "", search_slice_ratio,
			// ConsistencyLevel.QUORUM);

			for (Entry<String, List<ColumnOrSuperColumn>> element : entities
					.entrySet()) {

				String key = element.getKey();
				List<ColumnOrSuperColumn> columns = element.getValue();

				if (!columns.isEmpty()) {
					results.add(columns.get(0).getColumn().getValue());
				}
			}

//			for (Entry<String, List<ColumnOrSuperColumn>> element : result
//					.entrySet()) {
//
//				final List<ColumnOrSuperColumn> columns = element.getValue();
//
//				results.add(ec.findObjectUsingAID(new Type(clr
//						.classForName(acmd.getFullClassName())),
//						new FieldValues2() {
//
//							@Override
//							public FetchPlan getFetchPlanForLoading() {
//								return null;
//							}
//
//							@Override
//							public void fetchNonLoadedFields(ObjectProvider sm) {
//								sm.replaceNonLoadedFields(acmd
//										.getAllMemberPositions(),
//										new CassandraFetchFieldManager(acmd,
//												sm, columns));
//							}
//
//							@Override
//							public void fetchFields(ObjectProvider sm) {
//								sm.replaceFields(acmd.getPKMemberPositions(),
//										new CassandraFetchFieldManager(acmd,
//												sm, columns));
//								sm.replaceFields(acmd.getBasicMemberPositions(
//										clr, ec.getMetaDataManager()),
//										new CassandraFetchFieldManager(acmd,
//												sm, columns));
//
//							}
//						}, ignoreCache, true));
//
//			}
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


class Child_Info implements Comparable {

	ObjectProvider objectProvider;
	byte[] raw_object;
	Object object;

	public ObjectProvider getObjectProvider() {
		return objectProvider;
	}

	public void setObjectProvider(ObjectProvider objectProvider) {
		this.objectProvider = objectProvider;
	}

	public byte[] getRaw_object() {
		return raw_object;
	}

	public void setRaw_object(byte[] raw_object) {
		this.raw_object = raw_object;
	}

	public Object getObject() {
		return object;
	}

	public void setObject(Object object) {
		this.object = object;
	}

	public void applyToField(int field) {

		objectProvider.replaceField(field, object);
	}

	@Override
	public int compareTo(Object o) {
		if (this.equals(o)) {
			return 0;
		} else {
			//dont care
			return -1;
		}
	}
}


