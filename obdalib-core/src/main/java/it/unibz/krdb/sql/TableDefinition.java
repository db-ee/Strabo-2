package it.unibz.krdb.sql;

import it.unibz.krdb.sql.api.Attribute;

public class TableDefinition extends DataDefinition {

	@Override
	public String toString() {
		StringBuffer bf = new StringBuffer();
		bf.append(name);
		bf.append("[");
		boolean comma = false;
		for (Integer i : attributes.keySet()) {
			if (comma) {
				bf.append(",");
			}
			Attribute at = attributes.get(i);
			bf.append(at);
			if (at.bPrimaryKey)
				bf.append(":PK");
			comma = true;
		}
		bf.append("]");
		return bf.toString();
	}
}
