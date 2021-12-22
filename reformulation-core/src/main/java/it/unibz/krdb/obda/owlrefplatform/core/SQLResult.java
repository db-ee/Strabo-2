package it.unibz.krdb.obda.owlrefplatform.core;

import it.unibz.krdb.obda.model.Constant;
import it.unibz.krdb.obda.model.Function;

import java.util.List;

public class SQLResult {
	
	private String mainQuery;
	private List<String> tempQueries;
	private List<String> tempNames;
	private List<String> outputs;
	private Constant spatialFilterConstant;
	private String spatialTable;
	private Function spatialTableToRemove;
	private boolean condsiderSpatialBoundary;

	
	public SQLResult(String mainQuery, List<String> tempQueries, List<String> tempNames, List<String> outputs) {
		super();
		this.mainQuery = mainQuery;
		this.tempQueries = tempQueries;
		this.tempNames=tempNames;
		this.outputs=outputs;
		this.spatialFilterConstant = null;
		this.spatialTable = null;
		this.condsiderSpatialBoundary = true;
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
			toReturn+="\n";
		}
		toReturn+=mainQuery;
		return toReturn;
	}

	public List<String> getSignature() {
		return this.outputs;
	}

	public Constant getSpatialFilterConstant() {
		return spatialFilterConstant;
	}

	public void setSpatialFilterConstant(Constant spatialFilterConstant) {
		this.spatialFilterConstant = spatialFilterConstant;
	}

	public String getSpatialTable() {
		return spatialTable;
	}

	public void setSpatialTable(String spatialTable) {
		this.spatialTable = spatialTable;
	}

	public Function getSpatialTableToRemove() {
		return spatialTableToRemove;
	}

	public void setSpatialTableToRemove(Function spatialTableToRemove) {
		this.spatialTableToRemove = spatialTableToRemove;
	}

	public boolean isCondsiderSpatialBoundary() {
		return condsiderSpatialBoundary;
	}

	public void setCondsiderSpatialBoundary(boolean condsiderSpatialBoundary) {
		this.condsiderSpatialBoundary = condsiderSpatialBoundary;
	}
}

