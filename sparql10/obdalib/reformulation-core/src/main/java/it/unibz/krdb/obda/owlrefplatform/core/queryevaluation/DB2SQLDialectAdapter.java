package it.unibz.krdb.obda.owlrefplatform.core.queryevaluation;

import java.sql.Types;

public class DB2SQLDialectAdapter extends SQL99DialectAdapter {

	@Override
	public String strconcat(String[] strings) {
		if (strings.length == 0)
			throw new IllegalArgumentException("Cannot concatenate 0 strings");
		if (strings.length == 1)
			return strings[0];
		StringBuffer sql = new StringBuffer();

		sql.append(String.format("CONCAT(%s", strings[0]));
		for (int i = 1; i < strings.length; i++) {
			sql.append(String.format(", %s", strings[i]));
		}
		sql.append(")");
		return sql.toString();
	}

	@Override
	public String sqlSlice(long limit, long offset) {
		return String.format("OFFSET %d ROWS\nFETCH FIRST %d ROWS ONLY", offset, limit);
	}

	@Override
	public String sqlCast(String value, int type) {
		String strType = null;
		if (type == Types.VARCHAR) {
			strType = "VARCHAR(666)";
		} else {
			throw new RuntimeException("Unsupported SQL type");
		}
		return "CAST(" + value + " AS " + strType + ")";
	}

}
