package it.unibz.krdb.obda.owlrefplatform.core;

/*
 * #%L
 * ontop-reformulation-core
 * %%
 * Copyright (C) 2009 - 2014 Free University of Bozen-Bolzano
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import com.google.common.collect.HashMultimap;
import it.unibz.krdb.obda.model.*;
import it.unibz.krdb.obda.model.impl.OBDADataFactoryImpl;
import it.unibz.krdb.obda.model.impl.OBDAVocabulary;
import it.unibz.krdb.obda.model.impl.TermUtils;
import it.unibz.krdb.obda.ontology.Assertion;
import it.unibz.krdb.obda.owlrefplatform.core.basicoperations.DatalogNormalizer;
import it.unibz.krdb.obda.owlrefplatform.core.queryevaluation.SPARQLQueryUtility;
import it.unibz.krdb.obda.owlrefplatform.core.resultset.EmptyQueryResultSet;
import it.unibz.krdb.obda.owlrefplatform.core.resultset.QuestGraphResultSet;
import it.unibz.krdb.obda.owlrefplatform.core.resultset.QuestResultset;
import it.unibz.krdb.obda.owlrefplatform.core.translator.DatalogToSparqlTranslator;
import it.unibz.krdb.obda.owlrefplatform.core.translator.SesameConstructTemplate;
import it.unibz.krdb.obda.owlrefplatform.core.translator.SparqlAlgebraToDatalogTranslator;
import it.unibz.krdb.obda.owlrefplatform.core.unfolding.DatalogUnfolder;
import it.unibz.krdb.obda.owlrefplatform.core.unfolding.ExpressionEvaluator;
import it.unibz.krdb.obda.renderer.DatalogProgramRenderer;
import it.unibz.krdb.obda.utils.StrabonParameters;
import it.unibz.krdb.sql.DatabaseRelationDefinition;
import it.unibz.krdb.sql.RelationID;
import madgik.exareme.master.queryProcessor.decomposer.query.SQLQuery;
import madgik.exareme.master.queryProcessor.decomposer.util.Util;
import madgik.exareme.master.queryProcessor.decomposer.util.VarsToAtoms;
import madgik.exareme.master.queryProcessor.estimator.NodeSelectivityEstimator;
import madgik.exareme.master.queryProcessor.sparql.DagCreatorDatalogNew;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.QueryParser;
import org.openrdf.query.parser.QueryParserUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.*;
import java.util.concurrent.CountDownLatch;

//import org.sqlite.SQLiteConfig;

/**
 * The obda statement provides the implementations necessary to query the
 * reformulation platform reasoner from outside, i.e. Protege
 */
public class StrabonStatement implements OBDAStatement {

    private static final OBDADataFactory fac = OBDADataFactoryImpl.getInstance();

    public final Quest questInstance;

    private final QuestConnection conn;

    private final Statement sqlstatement;

    private boolean canceled = false;

    private boolean queryIsParsed = false;

    private ParsedQuery parsedQ = null;

    private QueryExecutionThread executionthread;

    private DatalogProgram programAfterUnfolding;

    private DatalogProgram programAfterSplittingSpatialJoin;

    private SesameConstructTemplate templ;

    private static final Logger log = LoggerFactory.getLogger(StrabonStatement.class);

    private Set<String> cachedTables;

    private Set<String> temporaryCachedTables;

    /*
     * For benchmark purpose
     */
    private long queryProcessingTime = 0;
    private long rewritingTime = 0;
    private long unfoldingTime = 0;

    private NodeSelectivityEstimator nse;
    private boolean cache;
    private Set<String> asWKTTables;
    private Map<String, String> predDictionaryStat;

    public StrabonStatement(Quest questinstance, QuestConnection conn, Statement st, NodeSelectivityEstimator nse) {

        this.questInstance = questinstance;

        this.conn = conn;

        this.sqlstatement = st;

        this.nse = nse;

        this.cachedTables = new HashSet<>();

        this.temporaryCachedTables = new HashSet<>();

        this.cache = false;

        this.predDictionaryStat = null;

    }

    public void useCache(boolean cache) {
        this.cache = cache;
    }

    public void setPredicateDictionaryForStatistics(Map<String, String> predDictionaryStat) {
        this.predDictionaryStat = predDictionaryStat;
    }

    private class QueryExecutionThread extends Thread {

        private final CountDownLatch monitor;
        private final String strquery;
        // private Query query;
        private TupleResultSet tupleResult;
        private GraphResultSet graphResult;
        private Exception exception;
        private boolean error = false;
        private boolean executingSQL = false;

        boolean isBoolean = false, isConstruct = false, isDescribe = false, isSelect = false;

        public QueryExecutionThread(String strquery, CountDownLatch monitor) {
            this.monitor = monitor;
            this.strquery = strquery;
            // this.query = QueryFactory.create(strquery);
        }

        // TODO: replace the magic number by an enum
        public void setQueryType(int type) {
            switch (type) {// encoding of query type to from numbers
                case 1:
                    this.isSelect = true;
                    break;
                case 2:
                    this.isBoolean = true;
                    break;
                case 3:
                    this.isConstruct = true;
                    break;
                case 4:
                    this.isDescribe = true;
                    break;
            }
        }

        public boolean errorStatus() {
            return error;
        }

        public Exception getException() {
            return exception;
        }

        public TupleResultSet getTupleResult() {
            return tupleResult;
        }

        public GraphResultSet getGraphResult() {
            return graphResult;
        }

        public void cancel() throws SQLException {
            canceled = true;
            if (!executingSQL) {
                this.stop();
            } else {
                sqlstatement.cancel();
            }
        }

        @Override
        public void run() {

            log.debug("Dummy run");

        }
    }

    /**
     * Calls the necessary tuple or graph query execution Implements describe uri or
     * var logic Returns the result set for the given query
     */
    @Override
    public it.unibz.krdb.obda.model.ResultSet execute(String strquery) throws OBDAException {
        if (strquery.isEmpty()) {
            throw new OBDAException("Cannot execute an empty query");
        }
        ParsedQuery pq = null;
        QueryParser qp = QueryParserUtil.createParser(QueryLanguage.SPARQL);
        try {
            pq = qp.parseQuery(strquery, null);
        } catch (MalformedQueryException e1) {
            e1.printStackTrace();
        }
        // encoding ofquery type into numbers
        if (SPARQLQueryUtility.isSelectQuery(pq)) {
            parsedQ = pq;
            queryIsParsed = true;
            TupleResultSet executedQuery = executeTupleQuery(strquery, 1);
            return executedQuery;

        } else if (SPARQLQueryUtility.isAskQuery(pq)) {
            parsedQ = pq;
            queryIsParsed = true;
            TupleResultSet executedQuery = executeTupleQuery(strquery, 2);
            return executedQuery;
        } else if (SPARQLQueryUtility.isConstructQuery(pq)) {

            // Here we need to get the template for the CONSTRUCT query results
            try {
                templ = new SesameConstructTemplate(strquery);
            } catch (MalformedQueryException e) {
                e.printStackTrace();
            }

            // Here we replace CONSTRUCT query with SELECT query
            strquery = SPARQLQueryUtility.getSelectFromConstruct(strquery);
            GraphResultSet executedGraphQuery = executeGraphQuery(strquery, 3);
            return executedGraphQuery;

        } else if (SPARQLQueryUtility.isDescribeQuery(pq)) {
            // create list of uriconstants we want to describe
            List<String> constants = new ArrayList<String>();
            if (SPARQLQueryUtility.isVarDescribe(strquery)) {
                // if describe ?var, we have to do select distinct ?var first
                String sel = SPARQLQueryUtility.getSelectVarDescribe(strquery);
                it.unibz.krdb.obda.model.ResultSet resultSet = (it.unibz.krdb.obda.model.ResultSet) this
                        .executeTupleQuery(sel, 1);
                if (resultSet instanceof EmptyQueryResultSet)
                    return null;
                else if (resultSet instanceof QuestResultset) {
                    QuestResultset res = (QuestResultset) resultSet;
                    while (res.nextRow()) {
                        Constant constant = res.getConstant(1);
                        if (constant instanceof URIConstant) {
                            // collect constants in list
                            constants.add(((URIConstant) constant).getIRI());
                        }
                    }
                }
            } else if (SPARQLQueryUtility.isURIDescribe(strquery)) {
                // DESCRIBE <uri> gives direct results, so we put the
                // <uri> constant directly in the list of constants
                try {
                    constants.add(SPARQLQueryUtility.getDescribeURI(strquery));
                } catch (MalformedQueryException e) {
                    e.printStackTrace();
                }
            }

            QuestGraphResultSet describeResultSet = null;
            // execute describe <uriconst> in subject position
            for (String constant : constants) {
                // for each constant we execute a construct with
                // the uri as subject, and collect the results
                // in one graphresultset
                String str = SPARQLQueryUtility.getConstructSubjQuery(constant);
                try {
                    templ = new SesameConstructTemplate(str);
                } catch (MalformedQueryException e) {
                    e.printStackTrace();
                }
                str = SPARQLQueryUtility.getSelectFromConstruct(str);
                if (describeResultSet == null) {
                    // just for the first time
                    describeResultSet = (QuestGraphResultSet) executeGraphQuery(str, 4);
                } else {
                    // 2nd and manyth times execute, but collect result into one
                    // object
                    QuestGraphResultSet set = (QuestGraphResultSet) executeGraphQuery(str, 4);
                    if (set != null) {
                        while (set.hasNext()) {
                            // already process the result, add list<Assertion>
                            // to internal buffer
                            describeResultSet.addNewResultSet(set.next());
                        }
                    }
                }
            }
            // execute describe <uriconst> in object position
            for (String constant : constants) {
                String str = SPARQLQueryUtility.getConstructObjQuery(constant);
                try {
                    templ = new SesameConstructTemplate(str);
                } catch (MalformedQueryException e) {
                    e.printStackTrace();
                }
                str = SPARQLQueryUtility.getSelectFromConstruct(str);
                if (describeResultSet == null) {
                    // just for the first time
                    describeResultSet = (QuestGraphResultSet) executeGraphQuery(str, 4);
                } else {
                    QuestGraphResultSet set = (QuestGraphResultSet) executeGraphQuery(str, 4);
                    if (set != null) {
                        while (set.hasNext()) {
                            describeResultSet.addNewResultSet(set.next());
                        }
                    }
                }
            }
            return describeResultSet;
        }
        throw new OBDAException("Error, the result set was null");
    }

    /**
     * Translates a SPARQL query into Datalog dealing with equivalences and
     * verifying that the vocabulary of the query matches the one in the ontology.
     * If there are equivalences to handle, this is where its done (i.e., renaming
     * atoms that use predicates that have been replaced by a canonical one.
     *
     * @return
     */

    private DatalogProgram translateAndPreProcess(ParsedQuery pq) {
        DatalogProgram program;
        try {
            SparqlAlgebraToDatalogTranslator translator = questInstance.getSparqlAlgebraToDatalogTranslator();
            program = translator.translate(pq);

            log.debug("Datalog program translated from the SPARQL query: \n{}", program);

            DatalogUnfolder unfolder = new DatalogUnfolder(program.clone().getRules(),
                    HashMultimap.<Predicate, List<Integer>>create());
            removeNonAnswerQueries(program);

            program = unfolder.unfold(program, OBDAVocabulary.QUEST_QUERY);

            log.debug("Flattened program: \n{}", program);
        } catch (Exception e) {
            e.printStackTrace();
            OBDAException ex = new OBDAException(e.getMessage());
            ex.setStackTrace(e.getStackTrace());
            throw e;
        }
        log.debug("Replacing equivalences...");
        DatalogProgram newprogram = OBDADataFactoryImpl.getInstance().getDatalogProgram();
        for (CQIE query : program.getRules()) {
            CQIE newquery = questInstance.getVocabularyValidator().replaceEquivalences(query);
            newprogram.appendRule(newquery);
        }
        return newprogram;
    }

    private DatalogProgram getUnfolding(DatalogProgram query) throws OBDAException {

        log.debug("Start the partial evaluation process...");

        DatalogProgram unfolding = questInstance.getUnfolder().unfold(query);
        // log.debug("Partial evaluation: \n{}", unfolding);
        log.debug("Data atoms evaluated: \n{}", unfolding);

        removeNonAnswerQueries(unfolding);

        // log.debug("After target rules removed: \n{}", unfolding);
        log.debug("Irrelevant rules removed: \n{}", unfolding);

        ExpressionEvaluator evaluator = questInstance.getExpressionEvaluator();
        evaluator.evaluateExpressions(unfolding);

        /*
         * UnionOfSqlQueries ucq = new
         * UnionOfSqlQueries(questInstance.getUnfolder().getCQContainmentCheck()); for
         * (CQIE cq : unfolding.getRules()) ucq.add(cq);
         *
         * List<CQIE> rules = new ArrayList<>(unfolding.getRules());
         * unfolding.removeRules(rules);
         *
         * for (CQIE cq : ucq.asCQIE()) { unfolding.appendRule(cq); }
         * log.debug("CQC performed ({} rules): \n{}", unfolding.getRules().size(),
         * unfolding);
         *
         */

        log.debug("Boolean expression evaluated: \n{}", unfolding);
        log.debug("Partial evaluation ended.");

        return unfolding;
    }

    private static void removeNonAnswerQueries(DatalogProgram program) {
        List<CQIE> toRemove = new LinkedList<CQIE>();
        for (CQIE rule : program.getRules()) {
            Predicate headPredicate = rule.getHead().getFunctionSymbol();
            if (!headPredicate.getName().toString().equals(OBDAVocabulary.QUEST_QUERY)
                    && !headPredicate.getName().toString().startsWith(OBDAVocabulary.TEMP_VIEW_QUERY)) {
                toRemove.add(rule);
            }
        }
        program.removeRules(toRemove);
    }

    private SQLResult getSQL(DatalogProgram query, List<String> signature) throws OBDAException {
        if (query.getRules().size() == 0) {
            return new SQLResult("", new ArrayList<String>(0), new ArrayList<String>(0), new ArrayList<String>(0));
        }
        log.debug("Producing the SQL string...");

        // query = DatalogNormalizer.normalizeDatalogProgram(query);
        SQLResult sql = questInstance.getDatasourceQueryGenerator().generateSourceQuery(query, signature);

        //log.debug("Resulting SQL: \n{}", sql);
        return sql;
    }

    /**
     * The method executes select or ask queries by starting a new quest execution
     * thread
     *
     * @param strquery the select or ask query string
     * @param type     1 - SELECT, 2 - ASK
     * @return the obtained TupleResultSet result
     * @throws OBDAException
     */
    private TupleResultSet executeTupleQuery(String strquery, int type) throws OBDAException {

        startExecute(strquery, type);
        TupleResultSet result = executionthread.getTupleResult();

        if (result == null)
            throw new RuntimeException("Error, the result set was null");

        return result;
    }

    /**
     * The method executes construct or describe queries
     *
     * @param strquery the query string
     * @param type     3- CONSTRUCT, 4 - DESCRIBE
     * @return the obtained GraphResultSet result
     * @throws OBDAException
     */
    private GraphResultSet executeGraphQuery(String strquery, int type) throws OBDAException {
        startExecute(strquery, type);
        return executionthread.getGraphResult();
    }

    /**
     * Internal method to start a new query execution thread type defines the query
     * type 1-SELECT, 2-ASK, 3-CONSTRUCT, 4-DESCRIBE
     */
    private void startExecute(String strquery, int type) throws OBDAException {
        CountDownLatch monitor = new CountDownLatch(1);
        executionthread = new QueryExecutionThread(strquery, monitor);
        executionthread.setQueryType(type);
        executionthread.start();
        try {
            monitor.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        if (executionthread.errorStatus()) {
            OBDAException ex = new OBDAException(executionthread.getException().getMessage());
            ex.setStackTrace(executionthread.getStackTrace());
            throw ex;
        }

        if (canceled == true) {
            canceled = false;
            throw new OBDAException("Query execution was cancelled");
        }
    }

    /**
     * Rewrites the given input SPARQL query and returns back an expanded SPARQL
     * query. The query expansion involves query transformation from SPARQL algebra
     * to Datalog objects and then translating back to SPARQL algebra. The
     * transformation to Datalog is required to apply the rewriting algorithm.
     *
     * @param sparql The input SPARQL query.
     * @return An expanded SPARQL query.
     * @throws OBDAException if errors occur during the transformation and
     *                       translation.
     */
    public String getSPARQLRewriting(String sparql) throws OBDAException {
        if (!SPARQLQueryUtility.isSelectQuery(sparql)) {
            throw new OBDAException("Support only SELECT query");
        }
        // Parse the SPARQL string into SPARQL algebra object
        QueryParser qp = QueryParserUtil.createParser(QueryLanguage.SPARQL);
        ParsedQuery query = null;
        try {
            query = qp.parseQuery(sparql, null); // base URI is null
        } catch (MalformedQueryException e) {
            throw new OBDAException(e);
        }

        // Obtain the query signature
        // SparqlAlgebraToDatalogTranslator translator =
        // questInstance.getSparqlAlgebraToDatalogTranslator();
        // List<String> signatureContainer = translator.getSignature(query);

        // Translate the SPARQL algebra to datalog program
        DatalogProgram initialProgram = translateAndPreProcess(query/* , signatureContainer */);
        System.out.println("StrabonStatement.translateAndPreprocessed:" + initialProgram.toString());

        // Translate the output datalog program back to SPARQL string
        // TODO Re-enable the prefix manager using Sesame prefix manager
        // PrefixManager prefixManager = new
        // SparqlPrefixManager(query.getPrefixMapping());
        DatalogToSparqlTranslator datalogTranslator = new DatalogToSparqlTranslator();
        return datalogTranslator.translate(initialProgram);
    }

    /**
     * Returns the final rewriting of the given query
     */
    public String getRewriting(ParsedQuery query) throws Exception {
        // TODO FIX to limit to SPARQL input and output

        DatalogProgram program = translateAndPreProcess(query);

        return DatalogProgramRenderer.encode(program);
    }

    /***
     * Returns the SQL query for a given SPARQL query. In the process, the signature
     * of the query will be set into the query container and the jena Query object
     * created (or cached) will be set as jenaQueryContainer[0] so that it can be
     * used in other process after getUnfolding.
     *
     * If the query is not already cached, it will be cached in this process.
     *
     * @param strquery
     * @return
     * @throws Exception
     */
    public SQLResult getUnfolding(String strquery, boolean splitSpatialJoin) throws Exception {
        SQLResult sql;

        // Check the cache first if the system has processed the query string
        // before
        if (questInstance.hasCachedSQL(strquery)) {
            // Obtain immediately the SQL string from cache
            sql = questInstance.getCachedSQL(strquery);

            // signatureContainer = signaturecache.get(strquery);
            // query = sesameQueryCache.get(strquery);

        } else {

            ParsedQuery query = null;

            if (!queryIsParsed) {
                QueryParser qp = QueryParserUtil.createParser(QueryLanguage.SPARQL);
                query = qp.parseQuery(strquery, null); // base URI is null
                // queryIsParsed = true;
            } else {
                query = parsedQ;
                queryIsParsed = false;
            }
            System.out.println("StrabonStatement: parsed query" + query);

            SparqlAlgebraToDatalogTranslator translator = questInstance.getSparqlAlgebraToDatalogTranslator();
            List<String> signatureContainer = translator.getSignature(query);

            questInstance.getSesameQueryCache().put(strquery, query);
            questInstance.getSignatureCache().put(strquery, signatureContainer);

            DatalogProgram program = translateAndPreProcess(query);


            // create metadata for temp tables
            for (CQIE cq : program.getRules()) {
                Predicate head = cq.getHead().getFunctionSymbol();
                if (head.getName().startsWith(OBDAVocabulary.TEMP_VIEW_QUERY)) {
                    RelationID relation = questInstance.getMetaData().getQuotedIDFactory().createRelationID("",
                            head.getName());
                    // questInstance.getMetaData().removeDatabaseRelation(relation);
                    DatabaseRelationDefinition replacement = questInstance.getMetaData()
                            .createDatabaseRelation(relation);
                    for (String t : cq.getSignature()) {
                        try {
                            if (isGeometry(t, cq)) {
                                replacement.addAttribute(
                                        questInstance.getMetaData().getQuotedIDFactory().createAttributeID(t.toString()),
                                        Types.BINARY, "geometry", false);
                            } else {
                                replacement.addAttribute(
                                        questInstance.getMetaData().getQuotedIDFactory().createAttributeID(t.toString()),
                                        Types.VARCHAR, "VARCHAR", false);
                            }

                        } catch (IllegalArgumentException e) {
                            log.debug("Attribute already exists");
                        }

                    }
                }
            }

            try {
                // log.debug("Input query:\n{}", strquery);

                for (CQIE q : program.getRules())
                    DatalogNormalizer.unfoldJoinTrees(q);

                log.debug("Normalized program: \n{}", program);

                /*
                 * Empty unfolding, constructing an empty result set
                 */
                if (program.getRules().size() < 1)
                    throw new OBDAException(
                            "Error, the translation of the query generated 0 rules. This is not possible for any SELECT query (other queries are not supported by the translator).");


                final long startTime = System.currentTimeMillis();



                if (splitSpatialJoin) {
                    try {
                        programAfterSplittingSpatialJoin = splitSpatialJoin(program);
                    } catch (Exception e) {
                        log.error("Could not split query based on spatial join" + e.getMessage());
                        programAfterSplittingSpatialJoin = program;
                    }
                } else {
                    programAfterSplittingSpatialJoin = program;
                }

                //DatalogProgram programAfterOptimization = null;
                DatalogProgram programAfterOptimization = programAfterSplittingSpatialJoin.clone();
                try {
                    optimize(programAfterOptimization);
                } catch (Exception e) {
                    log.error("Error while optimizing query. Using default translation. " + e.getMessage());
                    programAfterOptimization = programAfterSplittingSpatialJoin;
                }

                programAfterUnfolding = getUnfolding(programAfterSplittingSpatialJoin);


                unfoldingTime = System.currentTimeMillis() - startTime;



                if (cache) {
                    for (CQIE cq : programAfterUnfolding.getRules()) {
                        Set<Variable> previousVariables = new HashSet<>();
                        for (int i = 0; i < cq.getBody().size(); i++) {
                            Function atom = cq.getBody().get(i);
                            if (atom.getFunctionSymbol().toString().startsWith("prop")) {
                                Term t0 = atom.getTerm(0);
                                boolean skip = false;
                                if (t0 instanceof Variable) {
                                    if (previousVariables.contains(t0)) {
                                        skip = true;
                                        temporaryCachedTables.add("cache table subjectPartitioned" + atom.getFunctionSymbol().toString() + " as select * from " +
                                                atom.getFunctionSymbol().toString() + " CLUSTER BY s");
                                        atom.setPredicate(fac.getPredicate("subjectPartitioned" + atom.getFunctionSymbol().toString(), 2));
                                        if (i == 1) {
                                            Function first = cq.getBody().get(0);
                                            if (first.getFunctionSymbol().toString().startsWith("prop")) {
                                                if (first.getTerm(0).equals(t0)) {
                                                    temporaryCachedTables.add("cache table subjectPartitioned" + first.getFunctionSymbol().toString() + " as select * from " +
                                                            first.getFunctionSymbol().toString() + " CLUSTER BY s");
                                                    first.setPredicate(fac.getPredicate("subjectPartitioned" + first.getFunctionSymbol().toString(), 2));
                                                } else if (first.getTerm(1).equals(t0)) {
                                                    temporaryCachedTables.add("cache table objectPartitioned" + first.getFunctionSymbol().toString() + " as select * from " +
                                                            first.getFunctionSymbol().toString() + " CLUSTER BY o");
                                                    first.setPredicate(fac.getPredicate("objectPartitioned" + first.getFunctionSymbol().toString(), 2));
                                                }
                                            }

                                        }
                                    }
                                    previousVariables.add((Variable) t0);
                                } else if (t0 instanceof Constant) {
                                    skip = true;
                                    //temporaryCachedTables.add("cache table subjectPartitioned"+atom.getFunctionSymbol().toString()+" as select * from "+
                                    //		atom.getFunctionSymbol().toString() + " CLUSTER BY s");
                                    //atom.setPredicate(fac.getPredicate("subjectPartitioned"+atom.getFunctionSymbol().toString(), 2));
                                }
                                Term t1 = atom.getTerm(1);
                                if (t1 instanceof Variable) {
                                    if (!skip) {
                                        if (previousVariables.contains(t1)) {
                                            temporaryCachedTables.add("cache table objectPartitioned" + atom.getFunctionSymbol().toString() + " as select * from " +
                                                    atom.getFunctionSymbol().toString() + " CLUSTER BY o");
                                            atom.setPredicate(fac.getPredicate("objectPartitioned" + atom.getFunctionSymbol().toString(), 2));
                                            if (i == 1) {
                                                Function first = cq.getBody().get(0);
                                                if (first.getFunctionSymbol().toString().startsWith("prop")) {
                                                    if (first.getTerm(0).equals(t0)) {
                                                        temporaryCachedTables.add("cache table subjectPartitioned" + first.getFunctionSymbol().toString() + " as select * from " +
                                                                first.getFunctionSymbol().toString() + " CLUSTER BY s");
                                                        first.setPredicate(fac.getPredicate("subjectPartitioned" + first.getFunctionSymbol().toString(), 2));
                                                    } else if (first.getTerm(1).equals(t0)) {
                                                        temporaryCachedTables.add("cache table objectPartitioned" + first.getFunctionSymbol().toString() + " as select * from " +
                                                                first.getFunctionSymbol().toString() + " CLUSTER BY o");
                                                        first.setPredicate(fac.getPredicate("objectPartitioned" + first.getFunctionSymbol().toString(), 2));
                                                    }
                                                }

                                            }
                                        }
                                    }
                                    previousVariables.add((Variable) t1);
                                } else {
                                    if (!skip && t1 instanceof Constant) {
                                        //temporaryCachedTables.add("cache table objectPartitioned"+atom.getFunctionSymbol().toString()+" as select * from "+
                                        //		atom.getFunctionSymbol().toString() + " CLUSTER BY o");
                                        //atom.setPredicate(fac.getPredicate("objectPartitioned"+atom.getFunctionSymbol().toString(), 2));
                                    }
                                }
                            } else {
                                for (Term t : atom.getTerms()) {
                                    if (t instanceof Variable) {
                                        previousVariables.add((Variable) t);
                                    }
                                }
                            }
                        }
                    }
                }
                for (String temp : temporaryCachedTables) {
                    if (!cachedTables.contains(temp)) {
                        RelationID relation = RelationID.createRelationIdFromDatabaseRecord(null, temp.split(" ")[2]);
                        //questInstance.getMetaData().getQuotedIDFactory().createRelationID(null,
                        //temp);
                        // questInstance.getMetaData().removeDatabaseRelation(relation);
                        DatabaseRelationDefinition replacement = questInstance.getMetaData()
                                .createDatabaseRelation(relation);

                        replacement.addAttribute(
                                questInstance.getMetaData().getQuotedIDFactory().createAttributeID("s"),
                                Types.VARCHAR, "VARCHAR", false);

                        replacement.addAttribute(
                                questInstance.getMetaData().getQuotedIDFactory().createAttributeID("o"),
                                Types.VARCHAR, "VARCHAR", false);
                    }
                }


                sql = getSQL(programAfterUnfolding, signatureContainer);
                // cacheQueryAndProperties(strquery, sql);
                questInstance.cacheSQL(strquery, sql);
            } catch (Exception e1) {
                log.debug(e1.getMessage(), e1);

                OBDAException obdaException = new OBDAException(
                        "Error rewriting and unfolding into SQL\n" + e1.getMessage());
                obdaException.setStackTrace(e1.getStackTrace());
                throw obdaException;
            }
        }
        return sql;
    }

    private DatalogProgram optimize(DatalogProgram program) throws SQLException {
        for (CQIE cq : program.getRules()) {
            DagCreatorDatalogNew creator = new DagCreatorDatalogNew(cq, nse, predDictionaryStat);
            SQLQuery result2 = creator.getRootNode();
            //System.out.println(result2.toSQL());
        }
        return null;
    }

    private boolean isGeometry(String name, CQIE cq) {
        for (Function f : cq.getBody()) {
            if (f.getFunctionSymbol().getName().equals("http://www.opengis.net/ont/geosparql#asWKT") ||
                    asWKTTables.contains(f.getFunctionSymbol().getName())) {
                if (f.getTerms().get(1).toString().equals(name)) {
                    return true;
                }
            }
        }
        return false;
    }

    public Set<String> getTablesToCache() {
        Set<String> result = new HashSet<>();
        for (String temp : temporaryCachedTables) {
            if (cachedTables.add(temp)) {
                result.add(temp);
            }
        }
        temporaryCachedTables.clear();
        return result;
    }

    private DatalogProgram splitSpatialJoin(DatalogProgram program) {
        DatalogProgram result = program.clone();
        Set<Predicate> processed = new HashSet<Predicate>();

        for (int k = 0; k < result.getRules().size(); k++) {
            CQIE initial = result.getRules().get(k);
            if (!processed.add(initial.getHead().getFunctionSymbol())) {
                continue;
            }
            List<Function> spatialJoins = new ArrayList<Function>(1);
            Set<Variable> varsInSpatial = new HashSet<>();

            boolean containsSpatialJoin = queryHasSpatialJoin(initial, varsInSpatial, spatialJoins);


            if (!containsSpatialJoin) {
                containsSpatialJoin = hasSpatialDistanceJoinInSelect(initial, varsInSpatial);

            }
            if (!containsSpatialJoin || varsInSpatial.size() != 2) {
                continue;
            }


            //initial.getBody().removeAll(toRemove);


            Set<Variable> existentialVars = new HashSet<>();
            //will be added to temp queries along with vars in spatial join
            TermUtils.addReferencedVariablesTo(existentialVars, initial.getHead());


            List<VarsToAtoms> v2aList = new LinkedList<VarsToAtoms>();
            //keep split sets (connected components) of vars to atoms

            for (Variable spatialVar : varsInSpatial) {
                //start with two initial components each containing a
                //var from the spatial join
                Set<Term> vars1 = new HashSet<>();
                vars1.add(spatialVar);
                Set<Function> atoms1 = new HashSet<>();
                v2aList.add(new VarsToAtoms(vars1, atoms1));
            }
			
			
			/* for (int i = 0; i < initial.getBody().size(); i++) {
				Set<Variable> varsNext = new HashSet<>();
				TermUtils.addReferencedVariablesTo(varsNext, initial.getBody().get(i));
				Set<Function> atomsNext = new HashSet<>();
				atomsNext.add(initial.getBody().get(i));
				Set<Term> converted = new HashSet<Term>();
				converted.addAll(varsNext);
				VarsToAtoms v2aNext = new VarsToAtoms(converted, atomsNext);
				v2aList.add(0, v2aNext);
				for (int j = 1; j < v2aList.size(); j++) {
					if (v2aNext.mergeCommonVar(v2aList.get(j))) {
						v2aList.remove(j);
						j--;
					}
				}

			}*/


            //if (v2aList.size() != 2) {
            // split query to connected components
            // try to split using also constant URIs as "common variables"
            // e.g. consider a join: <id1> :p1 ?a1 . <id1> :p2 ?a2
            //v2aList = new LinkedList<VarsToAtoms>();
            for (int i = 0; i < initial.getBody().size(); i++) {
                if (spatialJoins.contains(initial.getBody().get(i)))
                    continue;
                Set<Term> varsNext = new HashSet<>();
                TermUtils.addReferencedVariablesAndURIsTo(varsNext, initial.getBody().get(i));
                Set<Function> atomsNext = new HashSet<>();
                atomsNext.add(initial.getBody().get(i));
                VarsToAtoms v2aNext = new VarsToAtoms(varsNext, atomsNext);
                v2aList.add(0, v2aNext);
                for (int j = 1; j < v2aList.size(); j++) {
                    if (v2aNext.mergeCommonVar(v2aList.get(j))) {
                        v2aList.remove(j);
                        j--;
                    }
                }

            }

            //each of the two vars in spatial join must have ended
            //on a separate connected component
            //for these two connceted components create temp tables by
            //removing corresponding atoms from initial query and adding them
            //to new temp ones
            for (VarsToAtoms v2a : v2aList) {
                if (v2a.getVars().containsAll(varsInSpatial)) {
                    //spatial vars ended up in the
                    //same component do not split query
                    containsSpatialJoin = false;
                    break;

                } else {
                    for (Variable spatialVar : varsInSpatial) {
                        if (v2a.getVars().contains(spatialVar)) {
                            initial.getBody().removeAll(v2a.getAtoms());
                            //result.addFirstRuleRule(createTempTable(v2a, initial, existentialVars, varsInSpatial));
                            //k++;
                            result.appendRule(createTempTable(v2a, initial, existentialVars, varsInSpatial), k);
                        }
                    }
                }
            }
            if (!containsSpatialJoin)
                continue;

            //contains spatial join, try to also split the produced queries
            k--;


            //initial.getBody().addAll(toRemove);

        }
        return result;
    }

    private CQIE createTempTable(VarsToAtoms cc, CQIE initial, Set<Variable> existentialVars, Set<Variable> varsInSpatial) {
        int id = Util.createUniqueId();
        DatabaseRelationDefinition replacement = questInstance.getMetaData()
                .createDatabaseRelation(questInstance.getMetaData().getQuotedIDFactory().createRelationID("",
                        OBDAVocabulary.TEMP_VIEW_QUERY + id));
        //VarsToAtoms cc = v2aList.get(i);
        List<String> signature = new ArrayList<String>(cc.getVars().size());
        List<Term> outputs = new ArrayList<Term>();
        for (Term t : cc.getVars()) {
            //Constant uriTemplate = fac.getConstantLiteral("{}");
            //Term f = fac.getUriTemplate(uriTemplate, t);
            Term f = fac.getVariable(t.toString());
            if (existentialVars.contains(t) || varsInSpatial.contains(t)) {
                outputs.add(f);
                signature.add(t.toString());
                if (varsInSpatial.contains(t)) {
                    //geometry
                    replacement.addAttribute(
                            questInstance.getMetaData().getQuotedIDFactory().createAttributeID(t.toString()),
                            Types.BINARY, "geometry", false);
                } else {
                    replacement.addAttribute(
                            questInstance.getMetaData().getQuotedIDFactory().createAttributeID(t.toString()),
                            Types.VARCHAR, "VARCHAR", false);
                }

            }
        }
        Function ans1 = fac.getFunction(fac.getPredicate(OBDAVocabulary.TEMP_VIEW_QUERY + id, outputs.size()),
                outputs);
        List<Term> args = new ArrayList<Term>(outputs.size());
        for (String s : signature) {
            args.add(fac.getVariable(s));
        }
        Function input = fac.getFunction(fac.getPredicate(OBDAVocabulary.TEMP_VIEW_QUERY + id, outputs.size()),
                args);
        initial.getBody().add(input);
        List<Function> body = new ArrayList<Function>();
        body.addAll(cc.getAtoms());
        CQIE cq = fac.getCQIE(ans1, body);
        CQIE clone = cq.clone();
        clone.setSignature(signature);
        return clone;
    }

    private boolean hasSpatialDistanceJoinInSelect(CQIE initial, Set<Variable> varsInSpatial) {
        for (Term projection : initial.getHead().getTerms()) {
            // check for spatial distance join in head
            if (projection instanceof Function) {
                Function nested = (Function) projection;
                if (nested.getFunctionSymbol().equals(OBDAVocabulary.SFDISTANCE)) {
                    TermUtils.addReferencedVariablesTo(varsInSpatial, nested);
                    if (varsInSpatial.size() != 2) {
                        return false;
                    } else {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    private boolean queryHasSpatialJoin(CQIE initial, Set<Variable> varsInSpatial, List<Function> spatialJoins) {
        for (Function atom : initial.getBody()) {

            if (StrabonParameters.isSpatialFunction(atom)) {


                TermUtils.addReferencedVariablesTo(varsInSpatial, atom);
                if (varsInSpatial.size() != 2) {
                    log.debug("Spatial join with " + varsInSpatial.size() + " variables. Cannot split spatial join");
                    return false;
                } else {
                    spatialJoins.add(atom);
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Returns the number of tuples returned by the query
     */
    public long getTupleCount(String query) throws Exception {

        throw new SQLException("Not Supported");

    }

    @Override
    public void close() throws OBDAException {
        try {
            if (sqlstatement != null)
                sqlstatement.close();
        } catch (Exception e) {
            throw new OBDAException(e);
        }
    }

    @Override
    public void cancel() throws OBDAException {
        canceled = true;
        try {
            StrabonStatement.this.executionthread.cancel();
        } catch (Exception e) {
            throw new OBDAException(e);
        }
    }

    /**
     * Called to check whether the statement was cancelled on purpose
     *
     * @return
     */
    public boolean isCanceled() {
        return canceled;
    }

    @Override
    public int executeUpdate(String query) throws OBDAException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int getFetchSize() throws OBDAException {
        try {
            return sqlstatement.getFetchSize();
        } catch (Exception e) {
            throw new OBDAException(e);
        }

    }

    @Override
    public int getMaxRows() throws OBDAException {
        try {
            return sqlstatement.getMaxRows();
        } catch (Exception e) {
            throw new OBDAException(e);
        }

    }

    @Override
    public void getMoreResults() throws OBDAException {
        try {
            sqlstatement.getMoreResults();
        } catch (Exception e) {
            throw new OBDAException(e);
        }

    }

    @Override
    public void setFetchSize(int rows) throws OBDAException {
        try {
            sqlstatement.setFetchSize(rows);
        } catch (Exception e) {
            throw new OBDAException(e);
        }

    }

    @Override
    public void setMaxRows(int max) throws OBDAException {
        try {
            sqlstatement.setMaxRows(max);
        } catch (Exception e) {
            throw new OBDAException(e);
        }

    }

    @Override
    public void setQueryTimeout(int seconds) throws OBDAException {
        try {
            sqlstatement.setQueryTimeout(seconds);
        } catch (Exception e) {
            throw new OBDAException(e);
        }
    }

    @Override
    public TupleResultSet getResultSet() throws OBDAException {
        return null;
    }

    @Override
    public int getQueryTimeout() throws OBDAException {
        try {
            return sqlstatement.getQueryTimeout();
        } catch (Exception e) {
            throw new OBDAException(e);
        }
    }

    @Override
    public boolean isClosed() throws OBDAException {
        try {
            return sqlstatement.isClosed();
        } catch (Exception e) {
            throw new OBDAException(e);
        }
    }

    /*
     * Methods for getting the benchmark parameters
     */
    public long getQueryProcessingTime() {
        return queryProcessingTime;
    }

    public long getRewritingTime() {
        return rewritingTime;
    }

    public long getUnfoldingTime() {
        return unfoldingTime;
    }


    public int getUCQSizeAfterUnfolding() {
        if (programAfterUnfolding.getRules() != null)
            return programAfterUnfolding.getRules().size();
        else
            return 0;
    }

    public int getMinQuerySizeAfterUnfolding() {
        int toReturn = Integer.MAX_VALUE;
        List<CQIE> rules = programAfterUnfolding.getRules();
        for (CQIE rule : rules) {
            int querySize = getBodySize(rule.getBody());
            if (querySize < toReturn) {
                toReturn = querySize;
            }
        }
        return (toReturn == Integer.MAX_VALUE) ? 0 : toReturn;
    }

    public int getMaxQuerySizeAfterUnfolding() {
        int toReturn = Integer.MIN_VALUE;
        List<CQIE> rules = programAfterUnfolding.getRules();
        for (CQIE rule : rules) {
            int querySize = getBodySize(rule.getBody());
            if (querySize > toReturn) {
                toReturn = querySize;
            }
        }
        return (toReturn == Integer.MIN_VALUE) ? 0 : toReturn;
    }

    private static int getBodySize(List<? extends Function> atoms) {
        int counter = 0;
        for (Function atom : atoms) {
            Predicate predicate = atom.getFunctionSymbol();
            if (!(predicate instanceof BuiltinPredicate)) {
                counter++;
            }
        }
        return counter;
    }

    /***
     * Inserts a stream of ABox assertions into the repository.
     *
     * @param data
     *
     * @throws SQLException
     */
    public int insertData(Iterator<Assertion> data, int commit, int batch) throws SQLException {
        return -1;
    }

    public void setWKTTables(Set<String> tables) {
        this.asWKTTables = tables;
    }

}
