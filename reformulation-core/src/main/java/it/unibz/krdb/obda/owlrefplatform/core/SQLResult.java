package it.unibz.krdb.obda.owlrefplatform.core;

import java.util.List;

public class SQLResult {
	
	private String mainQuery;
	List<String> tempQueries;
	List<String> tempNames;
	List<String> outputs;
	
	public SQLResult(String mainQuery, List<String> tempQueries, List<String> tempNames, List<String> outputs) {
		super();
		this.mainQuery = mainQuery;
		this.tempQueries = tempQueries;
		this.tempNames=tempNames;
		this.outputs=outputs;
	}
	
	public String getMainQuery() {
		return mainQuery;
	}
	
	public List<String> getTempQueries(){
		return tempQueries;
	}

	public void setMainQuery(String sql) {
		this.mainQuery=sql;
		
	}

	public String getTempName(int k) {
		return tempNames.get(k);
	}
	
	public String getTextResult() {
		String toReturn = "";
		for(int i=0;i<tempQueries.size();i++) {
			toReturn+=("Create temporary table "+tempNames.get(i)+" as ");
			toReturn+=(tempQueries.get(i));
			toReturn+=mainQuery;
		}
		return toReturn;
	}

	public List<String> getSignature() {
		return this.outputs;
	}

}
