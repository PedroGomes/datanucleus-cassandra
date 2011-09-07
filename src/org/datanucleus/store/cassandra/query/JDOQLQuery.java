package org.datanucleus.store.cassandra.query;

import org.datanucleus.FetchPlan;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.IndexMetaData;
import org.datanucleus.query.evaluator.JDOQLEvaluator;
import org.datanucleus.query.evaluator.JavaQueryEvaluator;
import org.datanucleus.query.expression.Expression;
import org.datanucleus.query.expression.Expression.Operator;
import org.datanucleus.query.expression.Literal;
import org.datanucleus.query.expression.PrimaryExpression;
import org.datanucleus.query.expression.VariableExpression;
import org.datanucleus.store.ExecutionContext;
import org.datanucleus.store.cassandra.CassandraManagedConnection;
import org.datanucleus.store.cassandra.ConversionUtils;
import org.datanucleus.store.query.AbstractJDOQLQuery;
import org.datanucleus.util.NucleusLogger;

import javax.jdo.identity.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class JDOQLQuery extends AbstractJDOQLQuery {


    private String start_key = null;
    private String end_key = null;


    /**
     * Constructs a new query instance that uses the given persistence manager.
     *
     * @param ec the associated ExecutiongContext for this query.
     */
    public JDOQLQuery(ExecutionContext ec) {
        this(ec, (JDOQLQuery) null);
    }

    /**
     * Constructs a new query instance having the same criteria as the given
     * query.
     *
     * @param ec The Executing Manager
     * @param q  The query from which to copy criteria.
     */
    public JDOQLQuery(ExecutionContext ec, JDOQLQuery q) {
        super(ec, q);
    }

    /**
     * Constructor for a JDOQL query where the query is specified using the
     * "Single-String" format.
     *
     * @param ec    The execution context
     * @param query The query string
     */
    public JDOQLQuery(ExecutionContext ec, String query) {
        super(ec, query);
    }
//






    @Override
    public void setRange(String range) {
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

            } else {
                fromTo = range.split("to");
                if (fromTo.length != 2) {
                    throw new NucleusUserException("Malformed RANGE clause: "
                            + range);
                } else {
                    start_key = fromTo[0].trim();
                    end_key = fromTo[1].trim();
                }
            }
        }

    }

	//loads all info in one range and collects keys to load child's
//    @Override
//    protected Object performExecute(Map parameters) {
//
////
////        if(candidateClass.toString().equals("class org.uminho.gsd.benchmarks.TPCW_CassandraOM.entities.Item")){
////          Exception exception = new Exception("");
////          exception.printStackTrace();
////          System.out.println(">1> "+ fromInclNo +" | "+toExclNo + " -- "+candidateClass +" || "+  number_exec);
////        }
//
//        Expression exp = this.compilation.getExprFilter();
//
//        if (exp != null) {
//
//            Operator op = exp.getOperator();
//            if (op.toString().equals(" = ")) {
//                // throw new NucleusUserException("Only simple \"=\" operations are allowed at this phase");
//
//                String column = "";
//                if (exp.getLeft() instanceof PrimaryExpression) {
//                    PrimaryExpression expression = (PrimaryExpression) exp
//                            .getLeft();
//                    column = expression.getSymbol().getQualifiedName();
//                } else if (exp.getLeft() instanceof VariableExpression) {
//                    VariableExpression variableExpression = (VariableExpression) exp
//                            .getLeft();
//                    column = variableExpression.getSymbol().getQualifiedName();
//                }
//
//                Literal literal = (Literal) exp.getRight();
//                Object o = literal.getLiteral();
//
//                AbstractClassMetaData metadata = ec.getMetaDataManager()
//                        .getMetaDataForClass(candidateClass,
//                                ec.getClassLoaderResolver());
//
//                AbstractMemberMetaData memberMetaData = metadata
//                        .getMetaDataForMember(column);
//                if (memberMetaData == null) {
//                    throw new NucleusUserException("Unknown field on query: "
//                            + column);
//                }
//                IndexMetaData indexmd = memberMetaData.getIndexMetaData();
//                if (indexmd == null) {
//                    throw new NucleusUserException("The field " + column
//                            + " must have a index.");
//                }
//
//                String index_table = indexmd.getTable();
//
//                CassandraManagedConnection mconn = (CassandraManagedConnection) ec
//                        .getStoreManager().getConnection(ec);
//                try {
//                    long startTime = System.currentTimeMillis();
//                    if (NucleusLogger.QUERY.isDebugEnabled()) {
//                        NucleusLogger.QUERY.debug(LOCALISER.msg("021046",
//                                "JDOQL", getSingleStringQuery(), null));
//                    }
//                    List candidates = null;
//                    if (candidateCollection != null) {
//                        candidates = new ArrayList(candidateCollection);
//                    }
//                    // else if (candidateExtent != null) {
//                    // candidates = new ArrayList();
//                    // Iterator iter = candidateExtent.iterator();
//                    // while (iter.hasNext()) {
//                    // candidates.add(iter.next());
//                    // }
//                    // }
//                    Collection results = CassandraQuery.getObjectsOfCandidateType(ec,
//                            mconn, candidateClass, subclasses, ignoreCache,
//                            fromInclNo, toExclNo, index_table, (String) o);
//
//                    if (this.getOrdering() != null || this.getGrouping() != null) {
//
//                        // Apply any result restrictions to the results
//                        JavaQueryEvaluator resultMapper = new JDOQLEvaluator(this,
//                                results, compilation, parameters, ec
//                                .getClassLoaderResolver());
//                        results = resultMapper.execute(false, true,
//                                true, true, false);
//                    }
//                    if (NucleusLogger.QUERY.isDebugEnabled()) {
//                        NucleusLogger.QUERY.debug(LOCALISER.msg("021074",
//                                "JDOQL",
//                                "" + (System.currentTimeMillis() - startTime)));
//                    }
//
//                    return results;
//                } finally {
//                    mconn.release();
//                }
//            } else if (op.toString().equals(" > ") || op.toString().equals(" < ")) {
//
//                String column = "";
//                if (exp.getLeft() instanceof PrimaryExpression) {
//                    PrimaryExpression expression = (PrimaryExpression) exp
//                            .getLeft();
//                    column = expression.getSymbol().getQualifiedName();
//                } else if (exp.getLeft() instanceof VariableExpression) {
//                    VariableExpression variableExpression = (VariableExpression) exp
//                            .getLeft();
//                    column = variableExpression.getSymbol().getQualifiedName();
//                }
//
//                Literal literal = (Literal) exp.getRight();
//                Object o = literal.getLiteral();
//
//                AbstractClassMetaData metadata = ec.getMetaDataManager()
//                        .getMetaDataForClass(candidateClass,
//                                ec.getClassLoaderResolver());
//
//                AbstractMemberMetaData memberMetaData = metadata
//                        .getMetaDataForMember(column);
//                if (memberMetaData == null) {
//                    throw new NucleusUserException("Unknown field on query: "
//                            + column);
//                }
//                if (!memberMetaData.isPrimaryKey()) {
//                    throw new NucleusUserException("The > and < operator con only be used on key columns");
//                }
//                if (!(o instanceof String)) {
//                    throw new NucleusUserException("The > and < operator con only be used with String parameters");
//                }
//
//                CassandraManagedConnection mconn = (CassandraManagedConnection) ec
//                        .getStoreManager().getConnection(ec);
//                try {
//                    long startTime = System.currentTimeMillis();
//                    if (NucleusLogger.QUERY.isDebugEnabled()) {
//                        NucleusLogger.QUERY.debug(LOCALISER.msg("021046",
//                                "JDOQL", getSingleStringQuery(), null));
//                    }
//                    List candidates = null;
//                    if (candidateCollection != null) {
//                        candidates = new ArrayList(candidateCollection);
//                    }
//
//                    boolean greater = op.toString().equals(" > ") ? true : false;
//
//                    Collection results = CassandraQuery.getObjectsOfCandidateTypeGreaterOrLowerThan(ec,
//                            mconn, candidateClass, subclasses, ignoreCache, toExclNo, greater, (String) o);
//
//                    if (this.getOrdering() != null || this.getGrouping() != null) {
//
//
//                        // Apply any result restrictions to the results
//                        JavaQueryEvaluator resultMapper = new JDOQLEvaluator(this,
//                                results, compilation, parameters, ec
//                                .getClassLoaderResolver());
//                        results = resultMapper.execute(false, true,
//                                true, true, false);
//
//
//                    }
//                    if (NucleusLogger.QUERY.isDebugEnabled()) {
//                        NucleusLogger.QUERY.debug(LOCALISER.msg("021074",
//                                "JDOQL",
//                                "" + (System.currentTimeMillis() - startTime)));
//                    }
//
//                    return results;
//                } finally {
//                    mconn.release();
//                }
//            }
//
//            return null;
//
//        }
//
//        CassandraManagedConnection mconn = (CassandraManagedConnection) ec
//                .getStoreManager().getConnection(ec);
//        try {
//            long startTime = System.currentTimeMillis();
//            if (NucleusLogger.QUERY.isDebugEnabled()) {
//                NucleusLogger.QUERY.debug(LOCALISER.msg("021046", "JDOQL",
//                        getSingleStringQuery(), null));
//            }
//            List candidates = null;
//            if (candidateCollection != null) {
//                candidates = new ArrayList(candidateCollection);
//            }
////            else if (candidateExtent != null) {
////				candidates = new ArrayList();
////				Iterator iter = candidateExtent.iterator();
////				while (iter.hasNext()) {
////					candidates.add(iter.next());
////				}
////			}
//			FetchPlan fetchPlan = this.getFetchPlan();
//
//
//            Collection results = CassandraQuery.getObjectsOfCandidateType(ec,
//                    mconn, candidateClass,fetchPlan,subclasses, ignoreCache,
//                    fromInclNo, toExclNo);
//
//            if (this.getOrdering() != null || this.getGrouping() != null) {
//
//                // Apply any result restrictions to the results
//                JavaQueryEvaluator resultMapper = new JDOQLEvaluator(this,
//                        results, compilation, parameters, ec
//                        .getClassLoaderResolver());
//                results = resultMapper.execute(true, true, true, true,
//                        true);
//            }
//            if (NucleusLogger.QUERY.isDebugEnabled()) {
//                NucleusLogger.QUERY.debug(LOCALISER.msg("021074", "JDOQL", ""
//                        + (System.currentTimeMillis() - startTime)));
//            }
//
//            return results;
//        } finally {
//            mconn.release();
//        }
//    }
//




    public List<?> getObjectsOfCandidateType(List<byte[]> keys,
                                             AbstractClassMetaData acmd,
                                             boolean subclasses) throws Exception {

        //ConversionUtils conversionUtils = new ConversionUtils();

        List<Object> results = new ArrayList<Object>(keys.size());

        for (byte[] key : keys) {

            Class<?> targetClass = candidateClass;

            Class<?> key_class = acmd
                    .getMetaDataForManagedMemberAtAbsolutePosition(acmd
                            .getPKMemberPositions()[0]).getType();

            Object ojc_id = ec.newObjectId(targetClass, ConversionUtils.convertBytes(key_class, key));

            if (!(ojc_id instanceof SingleFieldIdentity)) {
                throw new NucleusDataStoreException(
                        "Only single field identities are supported");
            }

            Object returned = null;
//            try {

            returned = ec.findObject(ojc_id, true, subclasses,
                    candidateClass.getName());

//            } catch (Exception e) {
//
//                throw new NucleusDataStoreException("",e);
//
//            }

            if (returned != null) {
                results.add(returned);
            }
        }

        return results;

    }

	//loads all ithe keys and then loads the object one by one

    @Override
    protected Object performExecute(Map parameters) {

        Expression exp = this.compilation.getExprFilter();


        AbstractClassMetaData metadata = ec.getMetaDataManager()
                .getMetaDataForClass(candidateClass,
                        ec.getClassLoaderResolver());

        if (exp != null) {

            Operator op = exp.getOperator();
            if (op.toString().equals(" = ")) {
                // throw new NucleusUserException("Only simple \"=\" operations are allowed at this phase");

                String column = "";
                if (exp.getLeft() instanceof PrimaryExpression) {
                    PrimaryExpression expression = (PrimaryExpression) exp
                            .getLeft();
                    column = expression.getSymbol().getQualifiedName();
                } else if (exp.getLeft() instanceof VariableExpression) {
                    VariableExpression variableExpression = (VariableExpression) exp
                            .getLeft();
                    column = variableExpression.getSymbol().getQualifiedName();
                }

                Literal literal = (Literal) exp.getRight();
                Object o = literal.getLiteral();


                AbstractMemberMetaData memberMetaData = metadata
                        .getMetaDataForMember(column);
                if (memberMetaData == null) {
                    throw new NucleusUserException("Unknown field on query: "
                            + column);
                }
                IndexMetaData indexmd = memberMetaData.getIndexMetaData();
                if (indexmd == null) {
                    throw new NucleusUserException("The field " + column
                            + " must have a index.");
                }

                String index_table = indexmd.getTable();

                CassandraManagedConnection mconn = (CassandraManagedConnection) ec
                        .getStoreManager().getConnection(ec);
                try {
                    long startTime = System.currentTimeMillis();
                    if (NucleusLogger.QUERY.isDebugEnabled()) {
                        NucleusLogger.QUERY.debug(LOCALISER.msg("021046",
                                "JDOQL", getSingleStringQuery(), null));
                    }
                    List candidates = null;
                    if (candidateCollection != null) {
                        candidates = new ArrayList(candidateCollection);
                    }
                    // else if (candidateExtent != null) {
                    // candidates = new ArrayList();
                    // Iterator iter = candidateExtent.iterator();
                    // while (iter.hasNext()) {
                    // candidates.add(iter.next());
                    // }
                    // }
                    List<byte[]> keys = CassandraQuery.getKeysOfCandidateTypeFromIndex(ec,
                            mconn, candidateClass, subclasses, ignoreCache,
                            fromInclNo, toExclNo, index_table, (String) o);

                    Collection results = getObjectsOfCandidateType(keys, metadata, subclasses);

                    if (this.getOrdering() != null || this.getGrouping() != null) {

                        // Apply any result restrictions to the results
                        JavaQueryEvaluator resultMapper = new JDOQLEvaluator(this,
                                results, compilation, parameters, ec
                                .getClassLoaderResolver());
                        results = resultMapper.execute(false, true,
                                true, true, false);
                    }
                    if (NucleusLogger.QUERY.isDebugEnabled()) {
                        NucleusLogger.QUERY.debug(LOCALISER.msg("021074",
                                "JDOQL",
                                "" + (System.currentTimeMillis() - startTime)));
                    }

                    return results;
                } catch (Exception e) {
                    throw new NucleusDataStoreException("Error on query execution",e);
                } finally {
                    mconn.release();
                }
            } else if (op.toString().equals(" > ") || op.toString().equals(" < ")) {

                String column = "";
                if (exp.getLeft() instanceof PrimaryExpression) {
                    PrimaryExpression expression = (PrimaryExpression) exp
                            .getLeft();
                    column = expression.getSymbol().getQualifiedName();
                } else if (exp.getLeft() instanceof VariableExpression) {
                    VariableExpression variableExpression = (VariableExpression) exp
                            .getLeft();
                    column = variableExpression.getSymbol().getQualifiedName();
                }

                Literal literal = (Literal) exp.getRight();
                Object o = literal.getLiteral();

                AbstractMemberMetaData memberMetaData = metadata
                        .getMetaDataForMember(column);
                if (memberMetaData == null) {
                    throw new NucleusUserException("Unknown field on query: "
                            + column);
                }
                if (!memberMetaData.isPrimaryKey()) {
                    throw new NucleusUserException("The > and < operator con only be used on key columns");
                }
                if (!(o instanceof String)) {
                    throw new NucleusUserException("The > and < operator con only be used with String parameters");
                }

                CassandraManagedConnection mconn = (CassandraManagedConnection) ec
                        .getStoreManager().getConnection(ec);
                try {
                    long startTime = System.currentTimeMillis();
                    if (NucleusLogger.QUERY.isDebugEnabled()) {
                        NucleusLogger.QUERY.debug(LOCALISER.msg("021046",
                                "JDOQL", getSingleStringQuery(), null));
                    }
                    List candidates = null;
                    if (candidateCollection != null) {
                        candidates = new ArrayList(candidateCollection);
                    }

                    boolean greater = op.toString().equals(" > ") ? true : false;


                    List<byte[]> keys = CassandraQuery.getKeysOfCandidateTypeGreaterOrLowerThan(ec,
                            mconn, candidateClass, subclasses, ignoreCache, toExclNo, greater, (String) o);

                    Collection results = getObjectsOfCandidateType(keys, metadata, subclasses);

                    if (this.getOrdering() != null || this.getGrouping() != null) {


                        // Apply any result restrictions to the results
                        JavaQueryEvaluator resultMapper = new JDOQLEvaluator(this,
                                results, compilation, parameters, ec
                                .getClassLoaderResolver());
                        results = resultMapper.execute(false, true,
                                true, true, false);


                    }
                    if (NucleusLogger.QUERY.isDebugEnabled()) {
                        NucleusLogger.QUERY.debug(LOCALISER.msg("021074",
                                "JDOQL",
                                "" + (System.currentTimeMillis() - startTime)));
                    }

                    return results;
                } catch (Exception e) {
                    throw new NucleusDataStoreException("Error on query execution",e);
                } finally {
                    mconn.release();
                }
            }

            return null;

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
            }
//            else if (candidateExtent != null) {
//				candidates = new ArrayList();
//				Iterator iter = candidateExtent.iterator();
//				while (iter.hasNext()) {
//					candidates.add(iter.next());
//				}
//			}


            List<byte[]> keys = CassandraQuery.getKeysOfCandidateType(ec,
                    mconn, candidateClass, subclasses, ignoreCache,
                    fromInclNo, toExclNo);

            Collection results = getObjectsOfCandidateType(keys, metadata, subclasses);


            if (this.getOrdering() != null || this.getGrouping() != null) {

                // Apply any result restrictions to the results
                JavaQueryEvaluator resultMapper = new JDOQLEvaluator(this,
                        results, compilation, parameters, ec
                        .getClassLoaderResolver());
                results = resultMapper.execute(true, true, true, true,
                        true);
            }
            if (NucleusLogger.QUERY.isDebugEnabled()) {
                NucleusLogger.QUERY.debug(LOCALISER.msg("021074", "JDOQL", ""
                        + (System.currentTimeMillis() - startTime)));
            }

            return results;

        } catch (Exception e) {
            throw new NucleusDataStoreException("Error on query execution",e);
        } finally {
            mconn.release();
        }
    }

}
