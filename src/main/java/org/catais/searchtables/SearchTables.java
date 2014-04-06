package org.catais.searchtables;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Properties;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.catais.avwms.Avwms;

public class SearchTables {
    private static Logger logger = Logger.getLogger(SearchTables.class);

    private HashMap params = null;

    private String importSourceDir = null;
    private String sqlTable = "lookup_tables_sql";

    private String host = null;
    private String port = null;
    private String dbname = null;
    private String schema = "av_gebaeudeadressen";
    private String user = null;
    private String pwd = null;

    private ArrayList tables = new ArrayList();
    private ArrayList queries = new ArrayList();

    Connection conn = null;

    private String sql = "DELETE FROM av_gebaeudeadressen.searchtable WHERE bfsnr = ___GEM_BFS;\n" + 
    		"\n" + 
    		"INSERT INTO av_gebaeudeadressen.searchtable(searchstring, displaytext, search_category, the_geom, geometry_type, searchstring_tsvector, bfsnr)\n" + 
    		"SELECT lower(geb.lokalisationsname || ' ' || geb.hausnummer || ' ' || geb.plz || ' ' || geb.ortschaftsname || ' ' || gem.bfsnr || ' ' || gem.\"name\") as searchstring,\n" + 
    		"       CASE WHEN gueltigkeit = 'projektiert' THEN geb.lokalisationsname || ' ' || geb.hausnummer || ', ' || geb.plz || ' ' || geb.ortschaftsname  || ' (projektiert)'\n" + 
    		"       ELSE geb.lokalisationsname || ' ' || geb.hausnummer || ', ' || geb.plz || ' ' || geb.ortschaftsname END as displaytext,\n" + 
    		"       '01_addresses'::text as search_category,\n" + 
    		"       geb.lage as the_geom,\n" + 
    		"       'POINT'::text as geometry_type,\n" + 
    		"       to_tsvector('finnish', lower(geb.lokalisationsname || ' ' || geb.hausnummer || ' ' || geb.plz || ' ' || geb.ortschaftsname || ' ' || gem.bfsnr || ' ' || gem.\"name\")) as searchstring_tsvector,\n" +
    		"       geb.bfsnr\n" +
    		"       \n" + 
    		"FROM av_gebaeudeadressen.gebaeudeadressen__gebaeudeeingang as geb, av_avdpool_ng.gemeindegrenzen_gemeinde as gem\n" + 
    		"WHERE geb.bfsnr = ___GEM_BFS AND gem.bfsnr = ___GEM_BFS\n" + 
    		"AND geb.bfsnr = gem.gem_bfs\n" + 
    		"AND geb.istoffiziellebezeichnung = 'ja'";
    
    public SearchTables(HashMap params) throws ClassNotFoundException, SQLException {
        logger.setLevel(Level.INFO);

        this.params = params;
        readParams();
    
        Class.forName("org.postgresql.Driver"); 
        this.conn = DriverManager.getConnection("jdbc:postgresql://"+this.host+"/"+this.dbname, this.user, this.pwd);
    }
    
    public void update() throws NullPointerException, IndexOutOfBoundsException  {
        File dir = new File(importSourceDir);
        String[] itfFileList = dir.list(new FilenameFilter() {
            public boolean accept(File d, String name) {
                return name.toLowerCase().endsWith(".itf"); 
            }
        });
        logger.debug("Count of itf files: " + itfFileList.length);

        if (itfFileList != null) {
            for(String f : itfFileList) {
                logger.info("SearchTables: " + dir.getAbsolutePath() + dir.separator + f);

                String gem_bfs = f.substring(0, 4);
                String sql1 = sql.replace("___GEM_BFS", gem_bfs);
                logger.debug(sql1);

                try {
                    Statement t = null;
                    t = conn.createStatement();

                    try {
                        conn.setAutoCommit(false);

                        t.executeUpdate(sql1);
                                            
                        conn.commit();
                        conn.setAutoCommit(true);
                        logger.info("SearchTables: " + gem_bfs + " commited.");

                    } catch(SQLException e) {
                        logger.error(e.getMessage());
                        conn.rollback();
                    } 
                    
                    t.close();
                    
                } catch(SQLException e) {
                    logger.error(e.getMessage());
                } 
            }
        }
    }
    
    private void readParams() {
        this.importSourceDir = (String) params.get("importSourceDir");
        logger.debug("Source Directory: " + this.importSourceDir);
        if (importSourceDir == null) {
            throw new IllegalArgumentException("Source dir not set.");
        }       

        this.host = (String) params.get("host");
        logger.debug("host: " + this.host);
        if (this.host == null) {
            throw new IllegalArgumentException("host not set.");
        }   

        this.port = (String) params.get("port");
        logger.debug("port: " + this.port);     
        if (this.port == null) {
            throw new IllegalArgumentException("port not set.");
        }       

        this.dbname = (String) params.get("dbname");
        logger.debug("port: " + this.dbname);       
        if (this.dbname == null) {
            throw new IllegalArgumentException("dbname not set.");
        }   

        this.user = (String) params.get("user");
        logger.debug("user: " + this.user);     
        if (this.user == null) {
            throw new IllegalArgumentException("user not set.");
        }   

        this.pwd = (String) params.get("pwd");
        logger.debug("pwd: " + this.pwd);       
        if (this.pwd == null) {
            throw new IllegalArgumentException("pwd not set.");
        }   
    }
}
