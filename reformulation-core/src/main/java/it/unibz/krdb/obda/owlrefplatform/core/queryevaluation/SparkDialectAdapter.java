package it.unibz.krdb.obda.owlrefplatform.core.queryevaluation;


public class SparkDialectAdapter extends SQL99DialectAdapter {

	@Override
	public String sqlCast(String value, int type) {
		//Spark SQL does not support cat
		return value;
	}

	public String getSQLLexicalFormDatetime(String v) {
		return "'"+v+"'";
	}

}
