package sesameWrapper;


import it.unibz.krdb.obda.utils.GenericProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ConnectionConstants {

    public static final Logger log = LoggerFactory.getLogger(ConnectionConstants.class);

    public static final String PROXYUSER;
    public static final int NUMEXECUTORS;
    public static final int EXECUTORCORES;
    public static final String EXECUTORMEMORY;
    public static final String SPARKDRIVEREXTRACLASSPATH;
    public static final String SPARKEXECUTOREXTRACLASSPATH;
    public static final String APPMASTERENVSPARKHOME;
    public static final String EXECUTORENVSPARKHOME;
    public static final String APPMASTERENVSPARKCONFDIR;
    public static final String EXECUTORENVSPARKCONFDIR;
    public static final String APPMASTERENVHADOOPHOME;
    public static final String EXECUTORENVHADOOPHOME;
    public static final String APPMASTERENVHDFSHOME;
    public static final String EXECUTORENVHDFSHOME;
    public static final String APPMASTERENVHADOOPUSERNAME;
    public static final String EXECUTORENVHADOOPUSERNAME;
    public static final String SPARKYARNDISTFILES;
    public static final String SPARKYARNDISTJARS;
    public static final String LIVYRPCSERVERADDRESS;
    public static final String SPARKYARNSTAGINGDIR;
    public static final String SPARKJARS;
    public static final String SPARKSERIALIZER;
    public static final String SPARKKRYOREGISTRATOR;
    //public static String sparkJarsIvy = "/tmp/geospark*";
    public static final String SPARKEXTRALISTENERS;


    public static final String LIVYURL;
    public static final String DATABASENAME;
    public static final String DRIVERMEMORY;
    public static final int DRIVERCORES;

    public static String DICTIONARYFILE;
    public static String ASWKTFILE;
    public static String STATISTICSFILE;
    public static String GOOGLEMAPSKEY;


    static {
        GenericProperties properties = ConnectionProperties.getConnectionProperties();

        PROXYUSER = properties.getString("proxyUser");
        NUMEXECUTORS = properties.getInt("numExecutors");
        EXECUTORCORES = properties.getInt("executorCores");
        DRIVERCORES = properties.getInt("driverCores");
        EXECUTORMEMORY = properties.getString("executorMemory");
        DRIVERMEMORY = properties.getString("driverMemory");
        SPARKDRIVEREXTRACLASSPATH = properties.getString("sparkDriverExtraClassPath");
        SPARKEXECUTOREXTRACLASSPATH = properties.getString("sparkExecutorExtraClassPath");
        APPMASTERENVSPARKHOME = properties.getString("appMasterEnvSparkHome");
        EXECUTORENVSPARKHOME = properties.getString("executorEnvSparkHome");
        APPMASTERENVSPARKCONFDIR = properties.getString("appMasterEnvSparkConfDir");
        EXECUTORENVSPARKCONFDIR = properties.getString("executorEnvSparkConfDir");
        APPMASTERENVHADOOPHOME = properties.getString("appMasterEnvHadoopHome");
        EXECUTORENVHADOOPHOME = properties.getString("executorEnvHadoopHome");
        APPMASTERENVHDFSHOME = properties.getString("appMasterEnvHdfsHome");
        EXECUTORENVHDFSHOME = properties.getString("executorEnvHdfsHome");
        APPMASTERENVHADOOPUSERNAME = properties.getString("appMasterEnvHadoopUserName");
        EXECUTORENVHADOOPUSERNAME = properties.getString("executorEnvHadoopUserName");
        SPARKYARNDISTFILES = properties.getString("sparkYarnDistFiles");
        SPARKYARNDISTJARS = properties.getString("sparkYarnDistJars");
        LIVYRPCSERVERADDRESS = properties.getString("livyRpcServerAddress");
        SPARKYARNSTAGINGDIR = properties.getString("sparkYarnStagingDir");
        SPARKJARS = properties.getString("sparkJars");
        SPARKSERIALIZER = properties.getString("sparkSerializer");
        SPARKKRYOREGISTRATOR = properties.getString("sparkKryoRegistrator");
//sparkJarsIvy = properties "/tmp/geospark*" = properties
        SPARKEXTRALISTENERS = properties.getString("sparkExtraListeners");

        LIVYURL = properties.getString("livyUrl");
        DATABASENAME = properties.getString("databaseName");

        DICTIONARYFILE = properties.getString("dictionaryfile");
        STATISTICSFILE = properties.getString("statisticsfile");
        ASWKTFILE = properties.getString("asWKTfile");
        GOOGLEMAPSKEY = properties.getString("googlemapskey");

        log.trace("Strabon Properties Loaded.");
    }


}
