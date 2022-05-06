/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package it.unibz.krdb.obda.strabon.loader;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.logging.Level;

import it.unibz.krdb.obda.strabon.DataLoader;
import org.apache.log4j.Logger;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.impl.NumericLiteral;
import org.eclipse.rdf4j.model.impl.SimpleStatement;
import org.eclipse.rdf4j.model.vocabulary.GEO;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.RDFS;
import org.eclipse.rdf4j.model.vocabulary.XMLSchema;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.RDFParseException;
import org.eclipse.rdf4j.rio.ntriples.NTriplesParser;

/**
 *
 * @author tioannid
 */
public class ModularParser extends NTriplesParser implements java.io.Serializable {

    // ----- STATIC MEMBERS -----
    protected static final Logger logger = Logger.getLogger(DataLoader.appName);

    // ----- DATA MEMEBERS -----
    private SimpleStatement st;
    private RDFStatement rdf;

    // ----- CONSTRUCTORS -----
    // ----- DATA ACCESSORS -----
    public SimpleStatement getSt() {
        return st;
    }

    public RDFStatement getRdf() {
        return rdf;
    }

    // ----- METHODS -----
//    RDFStatement parseLine(String line) throws RDFParseException, RDFHandlerException, IOException {
//
//        this.parse((InputStream) (new ByteArrayInputStream(line.getBytes(Charset.forName("UTF-8")))), "HELLO");
//
//        return this.rdf;
//    }
    RDFStatement parseLine(String line) {

        try {
            this.parse((InputStream) (new ByteArrayInputStream(line.getBytes(Charset.forName("UTF-8")))), "HELLO");
        } catch (IOException ex) {
            logger.error("DBG22-IOException in line\n" + line + "\n" + ex.getMessage());
        } catch (RDFParseException ex) {
            logger.error("DBG22-RDFParseException in line\n" + line + "\n" + ex.getMessage());
        } catch (RDFHandlerException ex) {
            logger.error("DBG22-RDFHandlerException in line\n" + line + "\n" + ex.getMessage());
        }

        return this.rdf;
    }

    @Override
    protected Statement createStatement(Resource subj, IRI pred, Value obj) throws RDFParseException {
        /*
		 * object type: 0:unknown 1:literal 2:resource
         */
        try {
            this.st = (SimpleStatement) super.createStatement(subj, pred, obj);

            // Calculate Object type and Object string
            int objectType = 0;
            // if it is numeric, string or wkt keep only the value, else keep the whole
            // string
            String objString = obj.toString();

            if (obj instanceof Literal) {
                objectType = 1;
                if (obj instanceof NumericLiteral) {
                    NumericLiteral l = (NumericLiteral) obj;
                    objString = l.getLabel();
                } else {
                    Literal l = (Literal) obj;
                    IRI liri = l.getDatatype();
                    String lirival = liri.stringValue();
                    if (lirival.equals(RDF.LANGSTRING) || lirival.equals(XMLSchema.STRING)) {
                        objString = l.getLabel();
                        // we should also keep the language!
                    } else if (lirival.equals(GEO.WKT_LITERAL)) {
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
                        objString = wkt;
                    } else {
                        // No! Do not keep the whole literal including datatype
                        objString = l.getLabel();
                    }
                }
            } else {
                objectType = 2;
            }

            rdf = new RDFStatement(subj.toString(), pred.toString(), objString, objectType);

            return null;
        } catch (Exception e) {
            logger.error("Could not parse line: " + subj.toString() + " " + pred.toString() + " " + obj.toString());
            throw new RDFParseException(e.getMessage());
        }
    }
}
