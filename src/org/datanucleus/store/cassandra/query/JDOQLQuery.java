package org.datanucleus.store.cassandra.query;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.datanucleus.ObjectManager;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.IndexMetaData;
import org.datanucleus.query.evaluator.JDOQLEvaluator;
import org.datanucleus.query.evaluator.JavaQueryEvaluator;
import org.datanucleus.query.expression.Expression;
import org.datanucleus.query.expression.Literal;
import org.datanucleus.query.expression.PrimaryExpression;
import org.datanucleus.query.expression.Expression.Operator;
import org.datanucleus.query.symbol.Symbol;
import org.datanucleus.store.ExecutionContext;

import org.datanucleus.store.cassandra.CassandraManagedConnection;
import org.datanucleus.store.query.AbstractJDOQLQuery;
import org.datanucleus.util.NucleusLogger;

public class JDOQLQuery extends AbstractJDOQLQuery {

	/**
	 * Constructs a new query instance that uses the given persistence manager.
	 * 
	 * @param om
	 *            the associated ExecutiongContext for this query.
	 */
	public JDOQLQuery(ExecutionContext ec) {
		this(ec, (JDOQLQuery) null);
	}

	/**
	 * Constructs a new query instance having the same criteria as the given
	 * query.
	 * 
	 * @param om
	 *            The Executing Manager
	 * @param q
	 *            The query from which to copy criteria.
	 */
	public JDOQLQuery(ExecutionContext ec, JDOQLQuery q) {
		super(ec, q);
	}

	/**
	 * Constructor for a JDOQL query where the query is specified using the
	 * "Single-String" format.
	 * 
	 * @param ec
	 *            The execution context
	 * @param query
	 *            The query string
	 */
	public JDOQLQuery(ExecutionContext ec, String query) {
		super(ec, query);
	}

	@Override
	protected Object performExecute(Map parameters) {

		String start_key = null;
		String end_key = null;;
		
		if (range != null && !range.equals("")) {
			// Range is of the format "from, to"
			String[] fromTo = range.split(",");
			if (fromTo.length == 2) {
			try {
				fromInclNo = Long.parseLong(fromTo[0].trim());
			} catch (Exception e) {
				throw new NucleusUserException("Malformed RANGE clause: "
						+ range);
			}

			try {
				toExclNo = Long.parseLong(fromTo[1].trim());
			} catch (Exception e) {
				throw new NucleusUserException("Malformed RANGE clause: "
						+ range);
			}
			}else{
				fromTo = range.split("to");
				if (fromTo.length != 2) {
					throw new NucleusUserException("Malformed RANGE clause: "
							+ range);					
				}
				else{
					start_key = fromTo[0].trim(); 
					end_key = fromTo[1].trim();			
				}
			}

		}
		//		    
		Expression exp =  this.compilation.getExprFilter();

		if(exp!=null){
			
			Operator op = exp.getOperator();	
			if(!op.toString().equals(" = ")){
				throw new NucleusUserException("Only simple \"=\" operations are allowed at this phase");	
			}
			PrimaryExpression expression = (PrimaryExpression) exp.getLeft();
			String column = expression.getSymbol().getQualifiedName();
			
			Literal literal = (Literal) exp.getRight();			
			Object o = literal.getLiteral();
			
			AbstractClassMetaData metadata = ec.getMetaDataManager().getMetaDataForClass(candidateClass, ec.getClassLoaderResolver());
			AbstractMemberMetaData memberMetaData =  metadata.getMetaDataForMember(column);
			if(memberMetaData==null){
				throw new NucleusUserException("Unknown field on query");	
			}
			IndexMetaData indexmd = memberMetaData.getIndexMetaData();
			if(indexmd==null){
				throw new NucleusUserException("The used field must have a index");	
			}
			
			String index_table= indexmd.getTable();
			
			CassandraManagedConnection mconn = (CassandraManagedConnection) ec
					.getStoreManager().getConnection(ec);
			try {
				long startTime = System.currentTimeMillis();
				if (NucleusLogger.QUERY.isDebugEnabled()) {
					NucleusLogger.QUERY.debug(LOCALISER.msg("021046", "JDOQL",
							getSingleStringQuery(), null));
				}
				List candidates = null;
				if (candidateCollection != null) {
					candidates = new ArrayList(candidateCollection);
				} 
	//				else if (candidateExtent != null) {
//					candidates = new ArrayList();
//					Iterator iter = candidateExtent.iterator();
//					while (iter.hasNext()) {
//						candidates.add(iter.next());
//					}
//				}
			//		else {
					candidates = CassandraQuery.getObjectsOfCandidateType(ec,
							mconn, candidateClass, subclasses, ignoreCache,
							fromInclNo, toExclNo,index_table,(String)o);
				//}

				// Apply any result restrictions to the results
				JavaQueryEvaluator resultMapper = new JDOQLEvaluator(this,
						candidates, compilation, parameters, ec
								.getClassLoaderResolver());
				Collection results = resultMapper.execute(false, true, true,
						true, false);

				if (NucleusLogger.QUERY.isDebugEnabled()) {
					NucleusLogger.QUERY.debug(LOCALISER.msg("021074", "JDOQL",
							"" + (System.currentTimeMillis() - startTime)));
				}

				return results;
			} finally {
				mconn.release();
			}
			
			
		}
		
		CassandraManagedConnection mconn = (CassandraManagedConnection) ec
				.getStoreManager().getConnection(ec);
		try {
			long startTime = System.currentTimeMillis();
			if (NucleusLogger.QUERY.isDebugEnabled()) {
				NucleusLogger.QUERY.debug(LOCALISER.msg("021046", "JDOQL",
						getSingleStringQuery(), null));
			}
			List candidates = null;
			if (candidateCollection != null) {
				candidates = new ArrayList(candidateCollection);
			} else if (candidateExtent != null) {
				candidates = new ArrayList();
				Iterator iter = candidateExtent.iterator();
				while (iter.hasNext()) {
					candidates.add(iter.next());
				}
			} else {
				candidates = CassandraQuery.getObjectsOfCandidateType(ec,
						mconn, candidateClass, subclasses, ignoreCache,
						fromInclNo, toExclNo);
			}

			// Apply any result restrictions to the results
			JavaQueryEvaluator resultMapper = new JDOQLEvaluator(this,
					candidates, compilation, parameters, ec
							.getClassLoaderResolver());
			Collection results = resultMapper.execute(true, true, true, true,
					true);

			if (NucleusLogger.QUERY.isDebugEnabled()) {
				NucleusLogger.QUERY.debug(LOCALISER.msg("021074", "JDOQL", ""
						+ (System.currentTimeMillis() - startTime)));
			}

			return results;
		} finally {
			mconn.release();
		}
	}

}
