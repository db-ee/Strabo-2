package it.unibz.krdb.obda.strabon.loader;

import it.unibz.krdb.obda.strabon.DataLoader;
import org.apache.log4j.Logger;
import org.eclipse.rdf4j.model.*;
import org.eclipse.rdf4j.model.impl.NumericLiteral;
import org.eclipse.rdf4j.model.impl.SimpleStatement;
import org.eclipse.rdf4j.rio.RDFParseException;
import org.eclipse.rdf4j.rio.ntriples.NTriplesParser;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;

import java.nio.charset.Charset;

public class RDFParser extends NTriplesParser implements java.io.Serializable {

    private SimpleStatement st;
    private RDFStatement rdf;
    protected static final Logger logger = Logger.getLogger(DataLoader.appName);

    public SimpleStatement getStatement() {
        return this.st;
    }

    public RDFStatement getRdf() {
        return rdf;
    }

    RDFStatement parseLine(String line) throws IOException {
        //try {
        this.parse((InputStream) (new ByteArrayInputStream(line.getBytes(Charset.forName("UTF-8")))), "HELLO");

        //} catch (IOException e) {
        //	;
        //}
        return this.rdf;
    }

    @Override
    protected Statement createStatement(Resource subj, IRI pred, Value obj) throws RDFParseException {
        /*
		 * object type: 0:unknown 1:literal 2:resource
         */
        try {
            this.st = (SimpleStatement) super.createStatement(subj, pred, obj);
            int objectType = 0;
            // if it is numeric, string or wkt keep only the value, else keep the whole
            // string
            if (obj instanceof Literal) {
                objectType = 1;
                if (obj instanceof NumericLiteral) {
                    NumericLiteral l = (NumericLiteral) obj;
                    rdf = new RDFStatement(subj.toString(), pred.toString(), l.getLabel(), objectType);
                } else {
                    Literal l = (Literal) obj;
                    if (l.getDatatype().stringValue().equals("http://www.w3.org/1999/02/22-rdf-syntax-ns#langString")
                            || l.getDatatype().stringValue().equals("http://www.w3.org/2001/XMLSchema#string")) {
                        rdf = new RDFStatement(subj.toString(), pred.toString(), l.getLabel(), objectType);
                        // we should also keep the language!
                    } else if (l.getDatatype().stringValue()
                            .equals("http://www.opengis.net/ont/geosparql#wktLiteral")) {
                        // wkt
                        String wkt = l.getLabel().trim();
                        //srid = GeoConstants.default_GeoSPARQL_SRID;

                        if (wkt.length() == 0) { // empty geometry
                            wkt = "";
                        }

                        if (wkt.charAt(0) == '<') {// if a CRS URI is specified
                            int uriIndx = wkt.indexOf('>');

                            // FIXME: handle invalid URIs
                            //URI crs = URI.create(wkt.substring(1, uriIndx));
                            /*
							if (GeoConstants.CRS84_URI.equals(crs.toString())) {
								srid = GeoConstants.EPSG4326_SRID;
							}
							else {
								srid = getEPSG_SRID(crs.toString());
							}*/
                            // FIXME: this code assumes an EPSG URI or CRS84
                            //int srid = WKTHelper.getSRID(crs.toString());
                            // trim spaces after URI and get the WKT value
                            wkt = wkt.substring(uriIndx + 1).trim();
                        }
                        rdf = new RDFStatement(subj.toString(), pred.toString(), wkt, objectType);
                    } else {
                        // No! Do not keep the whole literal including datatype
                        rdf = new RDFStatement(subj.toString(), pred.toString(), l.getLabel(), objectType);
                        //rdf = new RDFStatement(subj.toString(), pred.toString(), obj.toString(), objectType);
                    }
                }

            } else {
                objectType = 2;
                rdf = new RDFStatement(subj.toString(), pred.toString(), obj.toString(), objectType);
            }

            return null;
        } catch (Exception e) {
            logger.error("Could not parse line: " + subj.toString() + " " + pred.toString() + " " + obj.toString());
            throw new RDFParseException(e.getMessage());
        }
    }
}
