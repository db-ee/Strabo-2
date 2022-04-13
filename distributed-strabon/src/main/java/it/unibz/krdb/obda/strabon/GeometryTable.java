package it.unibz.krdb.obda.strabon;

public class GeometryTable {
	
	private String tablename;
	private String hasGeometrySubpropertyIRI;
	private String asWKTSubpropertyIRI;
	
	
	public GeometryTable(String tablename, String hasGeometrySubpropertyIRI, String asWKTSubpropertyIRI) {
		super();
		this.tablename = tablename;
		this.hasGeometrySubpropertyIRI = hasGeometrySubpropertyIRI;
		this.asWKTSubpropertyIRI = asWKTSubpropertyIRI;
	}
	
	public boolean hasGeometrySubproperty(String geometrySubproperty) {
		return this.hasGeometrySubpropertyIRI.equals(geometrySubproperty);
	}
	
	public boolean hasAsWKTSubproperty(String asWKTSubproperty) {
		return this.asWKTSubpropertyIRI.equals(asWKTSubproperty);
	}

	public String getTablename() {
		return tablename;
	}

	public String getHasGeometrySubpropertyIRI() {
		return hasGeometrySubpropertyIRI;
	}

	public String getAsWKTSubpropertyIRI() {
		return asWKTSubpropertyIRI;
	}
	
	

}
