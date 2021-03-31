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


import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.openrdf.query.Binding;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.impl.MapBindingSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;

public class StrabonTupleQueryResult implements TupleQueryResult {

	private Iterator<Row> res;
	private List<String> signature;
	private List<String> tempTables; //the temp tables that have been created for this query. To delete on close
	private SparkSession spark;
	private static final Logger log = LoggerFactory.getLogger(StrabonTupleQueryResult.class);
	
	StrabonTupleQueryResult(Iterator<Row> iterator, List<String> signature){
		if(iterator == null)
			throw new NullPointerException();
		this.res = iterator;
		this.signature = signature;
	}
	
	@Override
	public void close() throws QueryEvaluationException {
		for (int k = 0; k < tempTables.size(); k++) {
			try{
				spark.sql("DROP VIEW globaltemp."+tempTables.get(k));
			}
			catch(Exception ex){
				log.error("Could not delete table "+tempTables.get(k)+". ");
				ex.printStackTrace();
			}
		}
	}

	@Override
	public boolean hasNext() throws QueryEvaluationException {
		return res.hasNext();
	}

	@Override
	public BindingSet next() throws QueryEvaluationException {
		Row row=res.next();
		MapBindingSet set = new MapBindingSet(this.signature.size() * 2); //why *2?
		for (String name : this.signature) {
			Binding binding = createBinding(name, row, this.signature);
			if (binding != null) {
				set.addBinding(binding);
			}
		}
		return set;
	}

	@Override
	public void remove() throws QueryEvaluationException {
		throw new QueryEvaluationException("The query result is read-only. Elements cannot be removed");
	}
	

	private Binding createBinding(String bindingName, Row row, List<String> signature2) {
		StrabonBindingSet bset = new StrabonBindingSet(row, signature2);
		return bset.getBinding(bindingName);
	}


	@Override
	public List<String> getBindingNames() throws QueryEvaluationException {
		return this.signature;
	}

	public void setTempTables(StrabonTupleQueryResult tuples) {
	}

	public void setTempTables(List<String> temp) {
		this.tempTables = temp;
	}

	public void setSparkSession(SparkSession spark) {
	}
}
