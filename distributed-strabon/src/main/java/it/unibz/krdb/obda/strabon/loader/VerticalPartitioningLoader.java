package it.unibz.krdb.obda.strabon.loader;

import java.util.List;

import it.unibz.krdb.obda.strabon.configuration.PredTbl;
import it.unibz.krdb.obda.strabon.configuration.TripleTableSchema;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.col;

/**
 *
 * @author tioannid
 */
public class VerticalPartitioningLoader extends TripleTableLoader {

    // ----- DATA MEMBERS -----
    private boolean generateExtVP;
    private String dictionaryTable;
    private List<PredTbl> predDictionary;
    private double threshold;

    // ----- CONSTRUCTORS -----
    public VerticalPartitioningLoader(SparkSession spark, String dbName,
                                      boolean flagDBExists, boolean flagCreateDB, String hdfsInputDir,
                                      boolean requiresInference, boolean ttPartitionedBySub,
                                      boolean ttPartitionedByPred, boolean dropDuplicates,
                                      TripleTableSchema tttschema, TripleTableSchema gttschema,
                                      String namespacePrefixJSONFile, boolean createUseNsDict,
                                      boolean useHiveQL_TableCreation, String asWKTFile,
                                      final String dictionaryTable, boolean generateExtVP,
                                      double thresholdExtVP, String hiveTableFormat, boolean createSeparateGeometryTable, String inputFormat) throws Exception {
        super(spark, dbName, flagDBExists, flagCreateDB, hdfsInputDir,
                requiresInference, ttPartitionedBySub, ttPartitionedByPred,
                dropDuplicates, tttschema, gttschema, namespacePrefixJSONFile,
                createUseNsDict, useHiveQL_TableCreation, asWKTFile, hiveTableFormat, createSeparateGeometryTable, inputFormat);
        this.generateExtVP = generateExtVP;
        this.dictionaryTable = dictionaryTable;
        this.threshold = thresholdExtVP;
    }

    // ----- DATA ACCESSORS -----
    // ----- METHODS --------
    @Override
    public void load() throws Exception {
        //super.load();
        logger.info("PHASE 2: creating VP tables on...");

        // 2.1 Create dictionary table for predicates with their statistics
        Dataset<Row> propsDS = spark.sql(
                String.format("SELECT %1$s, count(*), count(%2$s), count(%3$s) from %4$s group by %1$s",
                        this.tttschema.getColname_pred(),
                        this.tttschema.getColname_subj(),
                        this.tttschema.getColname_obj(),
                        this.tttschema.getTblname())).cache();
        
        JavaPairRDD<Row, Long> indxRDD = propsDS.javaRDD().zipWithIndex().cache();
        JavaRDD<PredTbl> propsRDD = indxRDD.map(t -> new PredTbl(t._1().getString(0), "prop" + t._2(), t._1().getLong(1), t._1().getLong(2), t._1().getLong(3))).cache();
        
        this.predDictionary = propsRDD.collect();
        Dataset<Row> predtblDS = spark.createDataFrame(propsRDD, PredTbl.class);
        spark.sql(String.format("DROP TABLE IF EXISTS %s", dictionaryTable));
        // Create the propdict table and collect statistics s, p 
        if (this.useHiveQL_TableCreation) { // use HiveQL
            predtblDS.createOrReplaceTempView("tmp_propdict");
            spark.sql(String.format(
                    "CREATE TABLE %1$s AS SELECT * FROM tmp_propdict",
                    dictionaryTable));
        } else {    // use Spark SQL
            predtblDS.write().format(hiveTableFormat).saveAsTable(dictionaryTable);
        }
        //dictionaryTable is not used in queries. no need to analyze
        //logger.info("ANALYZE TABLE " + dictionaryTable + " COMPUTE STATISTICS");
        //spark.sql(String.format(
        //        "ANALYZE TABLE %1$s COMPUTE STATISTICS",
        //        dictionaryTable));
        // 2.2. Create all property tables and compute statistics
        String createVPTable;
        if (this.useHiveQL_TableCreation) { // use HiveQL
            for (PredTbl predtbl : this.predDictionary) {
            spark.sql(String.format("DROP TABLE IF EXISTS %s", predtbl.getTblName()));
                createVPTable = String.format(
                        "CREATE TABLE %1$s AS SELECT %2$s, %3$s FROM %4$s WHERE %5$s = '%6$s'",
                        predtbl.getTblName(), tttschema.getColname_subj(),
                        tttschema.getColname_obj(), tttschema.getTblname(),
                        tttschema.getColname_pred(), predtbl.getPred());
                spark.sql(createVPTable);
                logger.info("ANALYZE TABLE " + predtbl.getTblName() + " COMPUTE STATISTICS FOR COLUMNS s,o");
                spark.sql(String.format(
                        "ANALYZE TABLE %1$s COMPUTE STATISTICS FOR COLUMNS s,o",
                        predtbl.getTblName()));
            }
        } else {    // use Spark SQL
            Dataset<Row> triplesDS = spark.sql(String.format("SELECT * FROM %1$s", tttschema.getTblname()));
            for (PredTbl predtbl : this.predDictionary) {
            	spark.sql(String.format("DROP TABLE IF EXISTS %s", predtbl.getTblName()));
                triplesDS.select(col("s"), col("o")).filter(col("p").equalTo(predtbl.getPred())).write().format(hiveTableFormat).saveAsTable(predtbl.getTblName());
                logger.info("ANALYZE TABLE " + predtbl.getTblName() + " COMPUTE STATISTICS FOR COLUMNS s,o");
                spark.sql(String.format(
                        "ANALYZE TABLE %1$s COMPUTE STATISTICS FOR COLUMNS s,o",
                        predtbl.getTblName()));
            }
        }
    }
}
