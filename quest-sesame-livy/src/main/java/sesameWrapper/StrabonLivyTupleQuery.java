package sesameWrapper;

/*
 * #%L
 * ontop-quest-sesame
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

import it.unibz.krdb.obda.owlrefplatform.core.SQLResult;
import it.unibz.krdb.obda.owlrefplatform.core.StrabonStatement;
import okhttp3.OkHttpClient;
import org.openrdf.model.Value;
import org.openrdf.query.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.json.stream.JsonParser;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;


public class StrabonLivyTupleQuery implements TupleQuery {

    private static final Logger log = LoggerFactory.getLogger(StrabonLivyTupleQuery.class);
    private StrabonStatement st;
    private int queryTimeout;
    private String queryString;
    private String sessionUrl;

    public StrabonLivyTupleQuery(String queryString, String baseURI, StrabonStatement st, String sessionUrl)
            throws MalformedQueryException {
        this.st=st;
        this.queryTimeout=0;
        this.queryString=queryString;
        this.sessionUrl = sessionUrl;
    }

    // needed by TupleQuery interface
    public TupleQueryResult evaluate() throws QueryEvaluationException {
        //TupleResultSet res = null;
        List<String> tempTables = new ArrayList<String>();
        try {
            // String sparql = readFile(queryfile);
            log.debug("Start Executing SPARQL query: "+queryString);
            SQLResult sql = st.getUnfolding(queryString, true);
            log.debug("Query unfolded:" + sql.getTextResult() + "\n");
            log.debug("Starting execution");
            long start = System.currentTimeMillis();
            // List<String> tempnames=new ArrayList<String>();
            boolean emptyResult=false;
            OkHttpClient client1  = new OkHttpClient.Builder()
            	    .connectTimeout(10, TimeUnit.SECONDS)
            	    .writeTimeout(10, TimeUnit.SECONDS)
            	    .readTimeout(300000, TimeUnit.SECONDS)
            	    .build();
       
            
            for (int k = 0; k < sql.getTempQueries().size(); k++) {
                String temp = sql.getTempQueries().get(k).replaceAll("\"", "").replaceAll("\n", " ");
                log.debug("creating temp table " + sql.getTempName(k) + " with query: " + temp);
                LivyHelper.sendCommandAndPrint(LivyHelper.getSQLQuery("Create temporary view "+sql.getTempName(k)+ " AS ("+temp+") "), sessionUrl, client1);
                LivyHelper.sendCommandAndPrint(LivyHelper.getSQLQuery("CACHE TABLE "+sql.getTempName(k)), sessionUrl, client1);
                LivyHelper.sendCommandAndPrint(LivyHelper.getSQLQuery("SELECT COUNT(*) FROM "+sql.getTempName(k)), sessionUrl, client1);


                //TODO
                /*if(tempDataset.isEmpty()){
                    log.debug("empty temp query: "+sql.getTempName(k));
                    //return empty result
                    log.debug("Execution finished in " + (System.currentTimeMillis() - start) + " with 0 results.");
                    emptyResult=true;
                    break;
                }*/
                tempTables.add(sql.getTempName(k));
            }

            if(emptyResult){
                StrabonLivyTupleQueryResult tuples= new StrabonLivyTupleQueryResult(null, sql.getSignature());
                tuples.setSessionUrl(sessionUrl);
                tuples.setTempTables(tempTables);
                return tuples;
            }

	    //LivyHelper.sendCommandAndGetBuffer(LivyHelper.getSQLWriteResult(sql.getMainQuery().replaceAll("\"", "").replaceAll("\n", " "), "hdfs:///Projects/FoodSecurity/Resources/1"), sessionUrl, client1);
            JsonParser parser = LivyHelper.sendCommandAndGetBuffer(LivyHelper.getSQLQuery(sql.getMainQuery().replaceAll("\"", "").replaceAll("\n", " ")), sessionUrl, client1);
            
            //LivyHelper.sendCommandAndPrint(LivyHelper.getSQLQuery("Create temporary view ttttt AS ("+sql.getMainQuery().replaceAll("\"", "").replaceAll("\n", " ")+") "), sessionUrl, client1);
            //LivyHelper.sendCommandAndPrint(LivyHelper.getSQLQuery("CACHE TABLE ttttt"), sessionUrl, client1);
            //tempTables.add("ttttt");
            //LivyHelper.sendCommandAndPrint(LivyHelper.getSQLQuery("select count(*) from ttttt"), sessionUrl, client1);
            //JsonParser parser = LivyHelper.sendCommandAndGetBuffer(LivyHelper.getSQLQuery("select * from ttttt"), sessionUrl, client1);
            if(parser == null)
                throw new NullPointerException();

            StrabonLivyTupleQueryResult tuples= new StrabonLivyTupleQueryResult(parser, sql.getSignature());
            tuples.setSessionUrl(sessionUrl);
            tuples.setTempTables(tempTables);
            return tuples;
            //long resultSize = result.count();

            //log.debug("Execution finished in " + (System.currentTimeMillis() - start) + " with "
            //	+ resultSize + " results.");
            //result.show(false);
            //for (int k = 0; k < sql.getTempQueries().size(); k++) {
            //spark.sql("DROP VIEW globaltemp."+sql.getTempName(k));
            //}
        } catch (Exception ex) {
        	ex.printStackTrace();
            throw new QueryEvaluationException("Could not execute query " + queryString + "\nException: " + ex.getMessage());
        }



    }

    // needed by TupleQuery interface
    public void evaluate(TupleQueryResultHandler handler)
            throws QueryEvaluationException, TupleQueryResultHandlerException {
        TupleQueryResult result = evaluate();
        handler.startQueryResult(result.getBindingNames());
        while (result.hasNext()) {
            handler.handleSolution(result.next());
        }
        handler.endQueryResult();
    }

    @Override
    public void setMaxQueryTime(int maxQueryTime) {
        // TODO Auto-generated method stub

    }

    @Override
    public int getMaxQueryTime() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public void setBinding(String name, Value value) {
        // TODO Auto-generated method stub

    }

    @Override
    public void removeBinding(String name) {
        // TODO Auto-generated method stub

    }

    @Override
    public void clearBindings() {
        // TODO Auto-generated method stub

    }

    @Override
    public BindingSet getBindings() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void setDataset(Dataset dataset) {
        // TODO Auto-generated method stub

    }

    @Override
    public Dataset getDataset() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void setIncludeInferred(boolean includeInferred) {
        // TODO Auto-generated method stub

    }

    @Override
    public boolean getIncludeInferred() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public void setMaxExecutionTime(int i) {
        queryTimeout = i;
    }

    @Override
    public int getMaxExecutionTime() {
        return queryTimeout;
    }
}
