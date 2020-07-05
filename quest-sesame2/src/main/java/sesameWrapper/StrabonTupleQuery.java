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

import it.unibz.krdb.obda.model.OBDAException;
import it.unibz.krdb.obda.model.TupleResultSet;
import it.unibz.krdb.obda.owlrefplatform.core.QuestDBConnection;
import it.unibz.krdb.obda.owlrefplatform.core.QuestDBStatement;
import it.unibz.krdb.obda.owlrefplatform.core.SQLResult;
import it.unibz.krdb.obda.owlrefplatform.core.StrabonStatement;
import it.unibz.krdb.obda.strabon.QueryExecutor;

import java.util.List;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.openrdf.model.Value;
import org.openrdf.query.BindingSet;
import org.openrdf.query.Dataset;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.TupleQueryResultHandler;
import org.openrdf.query.TupleQueryResultHandlerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class StrabonTupleQuery implements TupleQuery {
	
	private static final Logger log = LoggerFactory.getLogger(StrabonTupleQuery.class);
	private SparkSession spark;
	private StrabonStatement st;
	private int queryTimeout;
	private String queryString;

	public StrabonTupleQuery(String queryString, String baseURI, StrabonStatement st, SparkSession spark2) 
			throws MalformedQueryException {
        this.spark=spark2;
        this.st=st;
        this.queryTimeout=0;
        this.queryString=queryString;
	}
	
	// needed by TupleQuery interface
	public TupleQueryResult evaluate() throws QueryEvaluationException {
		//TupleResultSet res = null;
		
		try {
			// String sparql = readFile(queryfile);
			log.debug("Start Executing SPARQL query: "+queryString);
			SQLResult sql = st.getUnfolding(queryString);
			log.debug("Query unfolded:" + sql.getTextResult() + "\n");
			log.debug("Strating execution");
			long start = System.currentTimeMillis();
			// List<String> tempnames=new ArrayList<String>();
			for (int k = 0; k < sql.getTempQueries().size(); k++) {
				String temp = sql.getTempQueries().get(k).replaceAll("\"", "");
				log.debug("creating temp table " + sql.getTempName(k) + " with query: " + temp);
				org.apache.spark.sql.Dataset<Row> tempDataset = spark.sql(temp);
				tempDataset.createOrReplaceGlobalTempView(sql.getTempName(k));
			}
			org.apache.spark.sql.Dataset<Row> result = spark.sql(sql.getMainQuery().replaceAll("\"", ""));
			result.cache();
			StrabonTupleQueryResult tuples= new StrabonTupleQueryResult(result.toLocalIterator(), sql.getSignature());
			return tuples;
			//long resultSize = result.count();
			
			//log.debug("Execution finished in " + (System.currentTimeMillis() - start) + " with "
				//	+ resultSize + " results.");
			//result.show(false);
			//for (int k = 0; k < sql.getTempQueries().size(); k++) {
				//spark.sql("DROP VIEW globaltemp."+sql.getTempName(k));
			//}
		} catch (Exception ex) {
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
}