package org.catais;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Properties;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.catais.addresses.UpdateAddresses;
import org.catais.avwms.Avwms;
import org.catais.exportdata.ExportData;
import org.catais.fusion.Fusion;
import org.catais.geobau.Geobau;
import org.catais.github.GitHub;
import org.catais.ili2ch.Convert;
import org.catais.ili2freeframe.Transform;
import org.catais.importdata.ImportDataAVGeoPortal;
import org.catais.maintenance.Reindex;
import org.catais.maintenance.Vacuum;
import org.catais.mopublic.ExportMopublic;
import org.catais.postprocessing.PostProcessing;
import org.catais.searchtables.SearchTables;
import org.catais.svn.Commit2Svn;
import org.catais.utils.DeleteFiles;
import org.catais.utils.IOUtils;
import org.catais.utils.ReadProperties;
import org.postgresql.util.PSQLException;

import ch.interlis.ili2c.Ili2cException;
import ch.interlis.iox.IoxException;


public class Basisplan 
{
    private static Logger logger = Logger.getLogger(Basisplan.class);
    
    public static void main( String[] args )
    {       
        logger.setLevel(Level.DEBUG);

        String propFileName = null;
        ArrayList errorFileList = new ArrayList();
        
        try {
            // Read log4j properties file
            File tempDir = IOUtils.createTempDirectory("itf2avdpoolng");
            InputStream is =  App.class.getResourceAsStream("log4j.properties");
            File log4j = new File(tempDir, "log4j.properties");
            IOUtils.copy(is, log4j);
    
            // Configure log4j with properties file
            PropertyConfigurator.configure(log4j.getAbsolutePath());
    
            // Begin logging
            logger.info("Start: "+ new Date());
        
            // Get the properties file with all the things we need to know
            propFileName = (String) args[0];
            logger.debug("Properties filename: " + propFileName);
            
            // Read all the properties into a map.
            ReadProperties readproperties = new ReadProperties(propFileName);
            HashMap params = readproperties.read();
            logger.debug(params);
            
            boolean doImport = (Boolean) params.get("doImport");
            String doAvwms = (String) params.get("doAvwms");
            boolean doIli2ch = (Boolean) params.get("doIli2ch");
            boolean doIli2freeframe = (Boolean) params.get("doIli2freeframe");
            boolean doGeobau = (Boolean) params.get("doGeobau");
            boolean doMopublic = (Boolean) params.get("doMopublic");
            boolean doCommit2Svn = (Boolean) params.get("doCommit2Svn");
            boolean doCommit2GitHub = (Boolean) params.get("doCommit2GitHub");
            boolean doDeleteFiles = (Boolean) params.get("doDeleteFiles");
            String doVacuum = (String) params.get("vacuum");
            String doReindex = (String) params.get("reindex");
            boolean doExport = (Boolean) params.get("doExport");
            boolean doFusion = (Boolean) params.get("doFusion");
            boolean doPostprocessing = (Boolean) params.get("doPostprocessing");
            boolean doUpdateAddresses = (Boolean) params.get("doUpdateAddresses");
            boolean doUpdateSearchTables = (Boolean) params.get("doUpdateSearchTables");
            
            logger.info("doImport: " + doImport);
            logger.info("doAvwms: " + doAvwms);
            logger.info("doIli2ch: " + doIli2ch);       
            logger.info("doIli2freeframe: " + doIli2freeframe); 
            logger.info("doGeobau: " + doGeobau);
            logger.info("doMopublic: " + doMopublic);
            logger.info("doCommit2Svn: " + doCommit2Svn);
            logger.info("doCommit2GitHub: " + doCommit2GitHub);
            logger.info("doDeleteFiles: " + doDeleteFiles);
            logger.info("doVacuum: " + doVacuum);
            logger.info("doReindex: " + doReindex);
            logger.info("doExport: " + doExport);
            logger.info("doFusion: " + doFusion);
            logger.info("doPostprocessing: " + doPostprocessing);
            logger.info("doUpdateAddresses: " + doUpdateAddresses);
            logger.info("doUpdateSearchTables: " + doUpdateSearchTables);

            
            // Do the action:
            // Import
            if (doImport == true) {
                logger.info("Start import..."); 
                try {
                    ImportDataAVGeoPortal importData = new ImportDataAVGeoPortal(params);
                    errorFileList = importData.run();
                    logger.info("Filelist with errors: " + errorFileList.toString());
                    logger.info("End import.");
                    
                    if (doFusion == true) {
                        logger.info("Start fusion...");
                        Fusion fusion = new Fusion(params, "so");
                        fusion.run();
                        logger.info("End fusion.");
                    }
                    
                } catch (NullPointerException npe) {
                    npe.printStackTrace();
                    logger.error("NullPointException.");
                    logger.error(npe.getMessage());
                } catch (Exception e) {
                    logger.error(e.getMessage());
                }
            }
            
            // Vacuum
            if (doVacuum != null) {
                logger.info("Start Vacuum...");
                try {
                    Vacuum vacuum = new Vacuum(params);
                    vacuum.run();
                } catch (ClassNotFoundException cnfe) {
                    logger.error(cnfe.getMessage());    
                } catch (SQLException sqle) {
                    logger.error(sqle.getMessage());
                } catch (Exception e) {
                    logger.error(e.getMessage());
                }
                logger.info("End Vacuum.");
            }

            // Reindex
            if (doReindex != null) {
                logger.info("Start Reindexing...");
                try {
                    Reindex reindex = new Reindex(params);
                    reindex.run();
                } catch (ClassNotFoundException cnfe) {
                    logger.error(cnfe.getMessage());    
                } catch (SQLException sqle) {
                    logger.error(sqle.getMessage());
                } catch (Exception e) {
                    logger.error(e.getMessage());
                }
                logger.info("End Reindexing.");
            }
            
            // Delete files
            if (doDeleteFiles == true) {
                logger.info("Start deleting files...");
                try {
                    DeleteFiles deleteFiles = new DeleteFiles(params);
                    deleteFiles.run(errorFileList);
                } catch (Exception e) {
                    logger.error(e.getMessage());
                }
                logger.info("End deleting files.");
            }

        } catch (NullPointerException npe) {
            logger.fatal("NullPointException.");
            logger.fatal(npe.getMessage());
        } catch (IllegalArgumentException iae) {
            logger.fatal(iae.getMessage());
            iae.printStackTrace();
        } catch (FileNotFoundException fnfe) {
            logger.fatal(fnfe.getMessage());
            fnfe.printStackTrace();
        } catch (IOException ioe) {
            logger.fatal(ioe.getMessage());
            ioe.printStackTrace();
        } catch (ArrayIndexOutOfBoundsException aioobe) {
            logger.fatal(aioobe.getMessage());
            aioobe.printStackTrace();
        } finally {
            // Stop logging
            logger.info("End: "+ new Date());
        }
       
    }
}
