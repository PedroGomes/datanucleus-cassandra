/**********************************************************************
Copyright (c) 2010 Pedro Gomes and Universidade do Minho. All rights reserved.
(Based on datanucleus-hbase. Copyright (c) 2009 Andy Jefferson and others.)
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.ColumnMetaData;
import org.datanucleus.metadata.IdentityType;
import org.datanucleus.metadata.IndexMetaData;
import org.datanucleus.metadata.InvalidMetaDataException;
import org.datanucleus.metadata.MetaDataListener;

import org.datanucleus.util.Localiser;


public class CassandraMetaDataListener implements MetaDataListener{

	 /** Localiser for messages. */
    protected static final Localiser LOCALISER = Localiser.getInstance(
        "org.datanucleus.store.cassandra.Localisation", CassandraStoreManager.class.getClassLoader());

    private CassandraStoreManager storeManager;
    
    CassandraMetaDataListener(CassandraStoreManager storeManager)
    {
        this.storeManager = storeManager;
    }
	
	
	@Override
	public void loaded(AbstractClassMetaData cmd) {
		
        if (cmd.getIdentityType() == IdentityType.DATASTORE && !cmd.isEmbeddedOnly())
        {
            // Datastore id not supported
            throw new InvalidMetaDataException(LOCALISER, "Cassandra.DatastoreID", cmd.getFullClassName());
        }
	    if (storeManager.isAutoCreateSchema()){
	    	try {
				CassandraUtils.createSchema(cmd,storeManager);
			} catch (IOException e) {
				throw new NucleusException(e.getMessage(), e);
			}
	    }	
	    
	    //load index info from the class to elements  
	    IndexMetaData[] im = cmd.getIndexMetaData();
	    for(IndexMetaData indx : im){ //for all indexes
	    	
	    	AbstractMemberMetaData[] clm = indx.getMemberMetaData();
	    	if(clm.length>1){
	    		//Only one column per index 
	            throw new InvalidMetaDataException(LOCALISER,"Cassandra.Index.multipleColumns");	    		
	    	}
	    	String index_member_name = clm[0].getName();
	    	if(cmd.hasMember(index_member_name)){
		    	AbstractMemberMetaData md = cmd.getMetaDataForMember(index_member_name);
		    	IndexMetaData idm = new IndexMetaData();
		    	
		    	String table =  indx.getTable();
		    	
		    	if(table ==null){
		    		String class_name = cmd.getName();
		    		table =	class_name + "_index";
		    		
		    	}
		    	
		    	idm.setTable(table);
		    	idm.setName(indx.getName());
		    	idm.setUnique(indx.isUnique());
		    	md.setIndexMetaData(idm);
	    	}
	    	else{		
	            throw new InvalidMetaDataException(LOCALISER,"Cassandra.unknowMember",index_member_name);	    		
	    	}

	    }	   
	}
	
}
