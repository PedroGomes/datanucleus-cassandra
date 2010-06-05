package org.datanucleus.store.cassandra.query;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.datanucleus.ObjectManager;
import org.datanucleus.query.evaluator.JDOQLEvaluator;
import org.datanucleus.query.evaluator.JavaQueryEvaluator;
import org.datanucleus.store.ExecutionContext;


import org.datanucleus.store.cassandra.CassandraManagedConnection;
import org.datanucleus.store.query.AbstractJDOQLQuery;
import org.datanucleus.util.NucleusLogger;

public class JDOQLQuery extends AbstractJDOQLQuery {

	/**
    * Constructs a new query instance that uses the given persistence manager.
    * @param om the associated ExecutiongContext for this query.
    */
   public JDOQLQuery(ExecutionContext ec)
   {
       this(ec, (JDOQLQuery) null);
   }

   /**
    * Constructs a new query instance having the same criteria as the given query.
    * @param om The Executing Manager
    * @param q The query from which to copy criteria.
    */
   public JDOQLQuery(ExecutionContext ec, JDOQLQuery q)
   {
       super(ec, q);
   }

   /**
    * Constructor for a JDOQL query where the query is specified using the "Single-String" format.
    * @param ec The execution context
    * @param query The query string
    */
   public JDOQLQuery(ExecutionContext ec, String query)
   {
       super(ec, query);
   }
	
	
	@Override
	protected Object performExecute(Map parameters) {
		
		
		CassandraManagedConnection mconn = (CassandraManagedConnection) ec.getStoreManager().getConnection(ec);
        try
        {
            long startTime = System.currentTimeMillis();
            if (NucleusLogger.QUERY.isDebugEnabled())
            {
                NucleusLogger.QUERY.debug(LOCALISER.msg("021046", "JDOQL", getSingleStringQuery(), null));
            }
            List candidates = null;
            if (candidateCollection != null)
            {
                candidates = new ArrayList(candidateCollection);
            }
            else if (candidateExtent != null)
            {
            	candidates = new ArrayList();
            	Iterator iter = candidateExtent.iterator();
            	while (iter.hasNext())
            	{
            		candidates.add(iter.next());
            	}
            }
            else
            {
                candidates = CassandraQuery.getObjectsOfCandidateType(ec, mconn, candidateClass, subclasses,
                    ignoreCache);
            }

            // Apply any result restrictions to the results
            JavaQueryEvaluator resultMapper = new JDOQLEvaluator(this, candidates, compilation,
                parameters, ec.getClassLoaderResolver());
            Collection results = resultMapper.execute(true, true, true, true, true);

            if (NucleusLogger.QUERY.isDebugEnabled())
            {
                NucleusLogger.QUERY.debug(LOCALISER.msg("021074", "JDOQL", 
                    "" + (System.currentTimeMillis() - startTime)));
            }

            return results;
        }
        finally
        {
            mconn.release();
        }
    }

}
