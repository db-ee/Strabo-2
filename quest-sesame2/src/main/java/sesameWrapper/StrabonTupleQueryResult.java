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

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.spark.sql.Row;
import org.openrdf.query.Binding;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.impl.MapBindingSet;

public class StrabonTupleQueryResult implements TupleQueryResult {

	Iterator<Row> res;
	List<String> signature;
	
	StrabonTupleQueryResult(Iterator<Row> iterator, List<String> signature){
		if(iterator == null)
			throw new NullPointerException();
		this.res = iterator;
		this.signature = signature;
	}
	
	@Override
	public void close() throws QueryEvaluationException {
		//nothing to do
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

}
