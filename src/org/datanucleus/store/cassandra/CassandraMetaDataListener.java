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

import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.IdentityType;
import org.datanucleus.metadata.InvalidMetaDataException;
import org.datanucleus.metadata.MetaDataListener;

import org.datanucleus.util.Localiser;

public class CassandraMetaDataListener implements MetaDataListener{

	 /** Localiser for messages. */
    protected static final Localiser LOCALISER = Localiser.getInstance(
        "org.datanucleus.store.hbase.Localisation", CassandraStoreManager.class.getClassLoader());
	
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
	}
	
	
	
}
