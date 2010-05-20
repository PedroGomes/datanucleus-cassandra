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

import java.io.IOException;
import java.util.List;

import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.cassandra.thrift.Cassandra.Client;
import org.apache.thrift.TException;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusObjectNotFoundException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.store.AbstractPersistenceHandler;
import org.datanucleus.store.ExecutionContext;
import org.datanucleus.store.ObjectProvider;
import org.datanucleus.store.StoreManager;
import org.datanucleus.util.Localiser;


public class CassandraPersistenceHandler extends  AbstractPersistenceHandler{

	  /** Localiser for messages. */
     Localiser LOCALISER;
	
	protected final CassandraStoreManager storeManager;
	private String keyspace;
	
	public CassandraPersistenceHandler(StoreManager stm) {
		
		LOCALISER = Localiser.getInstance(
		        "org.datanucleus.store.cassandra.Localisation", CassandraPersistenceHandler.class.getClassLoader());
		storeManager =  (CassandraStoreManager) stm;
		keyspace = storeManager.getConnectionInfo().getKeyspace(); 
	}
	
	@Override
	public void close() {
		// TODO Auto-generated method stub	
	}

	@Override
	public void deleteObject(ObjectProvider objp) {
		
        // Check if read-only so update not permitted
		storeManager.assertReadOnlyForUpdateOfObject(objp);
		
        CassandraManagedConnection managedConnection = (CassandraManagedConnection) storeManager.getConnection(objp.getExecutionContext());
        try
        {
            AbstractClassMetaData acmd = objp.getClassMetaData();
             
            Object pkValue = objp.provideField(acmd.getPKMemberPositions()[0]);
            String key = CassandraUtils.ObjectToString(pkValue);
            
            ColumnPath path = new ColumnPath(CassandraUtils.getFamilyName(acmd));
                     
            try {
            	((Client)managedConnection.getConnection()).remove(keyspace, key, path, System.currentTimeMillis(), ConsistencyLevel.QUORUM);
			} catch (InvalidRequestException e) {
				 throw new NucleusDataStoreException(e.getMessage(),e);
			} catch (UnavailableException e) {
				 throw new NucleusDataStoreException(e.getMessage(),e);
			} catch (TimedOutException e) {
				 throw new NucleusDataStoreException(e.getMessage(),e);
			} catch (TException e) {
				 throw new NucleusDataStoreException(e.getMessage(),e);
			} 
        }
        catch (IOException e)
        {
            throw new NucleusDataStoreException(e.getMessage(),e);
        }
        finally
        {
            managedConnection.release();
        }    
					
	}

	@Override
	public void fetchObject(ObjectProvider objp, int[] fieldNumbers) {
		
		 CassandraManagedConnection managedConnection = (CassandraManagedConnection) storeManager.getConnection(objp.getExecutionContext());	        
		 
		 try
	        {
	            AbstractClassMetaData acmd = objp.getClassMetaData();
	         
	            Object pkValue = objp.provideField(acmd.getPKMemberPositions()[0]);
	            String key = CassandraUtils.ObjectToString(pkValue);
	            
	            ColumnParent parent = new ColumnParent(CassandraUtils.getFamilyName(acmd));
	            
	            Client dbClient = ((Client)managedConnection.getConnection());
	            
	            SliceRange range = new SliceRange();
	            range.setStart(new byte[]{});
	            range.setFinish(new byte[]{});
	            range.setCount( dbClient.get_count(keyspace, key,parent , ConsistencyLevel.QUORUM));
	            SlicePredicate predicate = new SlicePredicate();
	            
	            predicate.setSlice_range(range);

	            List<ColumnOrSuperColumn> result = dbClient.get_slice(keyspace,key, parent, predicate, ConsistencyLevel.QUORUM);
	           
	            
	            if(result==null)
	            {
	                throw new NucleusObjectNotFoundException();
	            }
	            CassandraFetchFieldManager fm = new CassandraFetchFieldManager(acmd,objp, result);
	            objp.replaceFields(acmd.getAllMemberPositions(), fm);
	       
	        }
	        catch (IOException e)
	        {
	            throw new NucleusDataStoreException(e.getMessage(),e);
	        } catch (InvalidRequestException e) {
	        	 throw new NucleusDataStoreException(e.getMessage(),e);
			} catch (UnavailableException e) {
				 throw new NucleusDataStoreException(e.getMessage(),e);
			} catch (TimedOutException e) {
				 throw new NucleusDataStoreException(e.getMessage(),e);
			} catch (TException e) {
				 throw new NucleusDataStoreException(e.getMessage(),e);
			}
	        finally
	        {
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

        if (!storeManager.managesClass(objp.getClassMetaData().getFullClassName()))
        {
        	storeManager.addClass(objp.getClassMetaData().getFullClassName(),objp.getExecutionContext().getClassLoaderResolver());
        }

        try
        {
            locateObject(objp);
            
            throw new NucleusUserException(LOCALISER.msg("Cassandra.Insert.ObjectWithIdAlreadyExists"));

        }
        catch (NucleusObjectNotFoundException onfe)
        {
            // Do nothing since object with this id doesn't exist
        }
        
        CassandraManagedConnection managedconnection = (CassandraManagedConnection) storeManager.getConnection(objp.getExecutionContext());
        try
        {
            AbstractClassMetaData acmd = objp.getClassMetaData();
            
            Object pkValue = objp.provideField(acmd.getPKMemberPositions()[0]);
            String key = CassandraUtils.ObjectToString(pkValue);  
            String column_family = CassandraUtils.getFamilyName(acmd);
            
            CassandraInsertFieldManager fm = new CassandraInsertFieldManager(acmd,objp,key, column_family);
            objp.provideFields(acmd.getAllMemberPositions(), fm);
            Client client = (Client)managedconnection.getConnection();
            client.batch_mutate(keyspace,fm.getMutation(),ConsistencyLevel.QUORUM);

        }
        catch (IOException e)
        {
            throw new NucleusDataStoreException(e.getMessage(),e);
        } catch (InvalidRequestException e) {
        	throw new NucleusDataStoreException(e.getMessage(),e);
		} catch (UnavailableException e) {
			throw new NucleusDataStoreException(e.getMessage(),e);
		} catch (TimedOutException e) {
			throw new NucleusDataStoreException(e.getMessage(),e);
		} catch (TException e) {
			throw new NucleusDataStoreException(e.getMessage(),e);
		}
        finally
        {
        	managedconnection.release();
        }	
	}

	@Override
	public void locateObject(ObjectProvider objp) {
        CassandraManagedConnection managedconnection = (CassandraManagedConnection) storeManager.getConnection(objp.getExecutionContext());
	        try
	        {
	            AbstractClassMetaData acmd = objp.getClassMetaData();
	            Object pkValue = objp.provideField(acmd.getPKMemberPositions()[0]);
	            String key = CassandraUtils.ObjectToString(pkValue);  
	            
	            SlicePredicate predicate = new SlicePredicate();
	            SliceRange range = new SliceRange("".getBytes(),"".getBytes(),false,1);
	            predicate.setSlice_range(range);
	            ColumnParent parent = new ColumnParent(CassandraUtils.getFamilyName(acmd));
	            
	            List<ColumnOrSuperColumn> columns = ((Client)managedconnection.getConnection()).get_slice(keyspace,key,parent, predicate, ConsistencyLevel.QUORUM);
	            
	            if(columns.isEmpty())
	            {
	                throw new NucleusObjectNotFoundException();
	            }
	        }
	        catch (IOException e)
	        {
	            throw new NucleusDataStoreException(e.getMessage(),e);
	        } catch (InvalidRequestException e) {
	        	throw new NucleusDataStoreException(e.getMessage(),e);
			} catch (UnavailableException e) {
				throw new NucleusDataStoreException(e.getMessage(),e);
			} catch (TimedOutException e) {
				throw new NucleusDataStoreException(e.getMessage(),e);
			} catch (TException e) {
				throw new NucleusDataStoreException(e.getMessage(),e);
			}
	        finally
	        {
	            managedconnection.release();
	        }    
	}

	@Override
	public void updateObject(ObjectProvider objp, int[] arg1) {
		  // Check if read-only so update not permitted
        storeManager.assertReadOnlyForUpdateOfObject(objp);
        CassandraManagedConnection managedconnection = (CassandraManagedConnection) storeManager.getConnection(objp.getExecutionContext());
        try
        {
            AbstractClassMetaData acmd = objp.getClassMetaData();
            
            Object pkValue = objp.provideField(acmd.getPKMemberPositions()[0]);
            String key = CassandraUtils.ObjectToString(pkValue);  
            String column_family = CassandraUtils.getFamilyName(acmd);
            
            CassandraInsertFieldManager fm = new CassandraInsertFieldManager(acmd,objp ,key, column_family);
            objp.provideFields(acmd.getAllMemberPositions(), fm);
            ((Client)managedconnection.getConnection()).batch_mutate(keyspace,fm.getMutation(),ConsistencyLevel.QUORUM);

        }
        catch (IOException e)
        {
            throw new NucleusDataStoreException(e.getMessage(),e);
        } catch (InvalidRequestException e) {
        	throw new NucleusDataStoreException(e.getMessage(),e);
		} catch (UnavailableException e) {
			throw new NucleusDataStoreException(e.getMessage(),e);
		} catch (TimedOutException e) {
			throw new NucleusDataStoreException(e.getMessage(),e);
		} catch (TException e) {
			throw new NucleusDataStoreException(e.getMessage(),e);
		}
        finally
        {
        	managedconnection.release();
        }	
	}

	
	
}
