package it.unibz.krdb.obda.owlrefplatform.core;

import java.util.List;

public class SQLResult {
	
	private String mainQuery;
	List<String> tempQueries;
	List<String> tempNames;
	
	public SQLResult(String mainQuery, List<String> tempQueries, List<String> tempNames) {
		super();
		this.mainQuery = mainQuery;
		this.tempQueries = tempQueries;
		this.tempNames=tempNames;
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
		return tempQueries.get(k);
	}

}
