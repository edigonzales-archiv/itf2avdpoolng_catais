package org.catais.addresses;

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

public class UpdateAddresses {
    private static Logger logger = Logger.getLogger(UpdateAddresses.class);

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

    private String sql = "DELETE FROM av_gebaeudeadressen.gebaeudeadressen__gebaeudeeingang WHERE bfsnr = ___GEM_BFS;\n" + 
    		"\n" + 
    		"INSERT INTO av_gebaeudeadressen.gebaeudeadressen__gebaeudeeingang (tid, gebaeudeeingang_von, gueltigkeit, lage, istoffiziellebezeichnung, hoehenlage, hausnummer, gebaeudename, gwr_egid, gwr_edid, lokalisationsname, plz, zusatzziffern, ortschaftsname, stand_am, bfsnr) \n" + 
    		"\n" + 
    		"SELECT tid, gebaeudeeingang_von, gueltigkeit, lage, istoffiziellebezeichnung, hoehenlage, hausnummer, gebaeudename, gwr_egid, gwr_edid, lokalisationsname, plz, zusatzziffern, ortschaftsname, stand_am, bfsnr\n" + 
    		"FROM\n" + 
    		"(\n" + 
    		" SELECT c.tid as tid, a.tid as gebaeudeeingang_von, d.designation_d as gueltigkeit, \n" + 
    		"       c.lage as lage, e.designation_d as istoffiziellebezeichnung, \n" + 
    		"       c.hoehenlage as hoehenlage, c.hausnummer as hausnummer, NULL::varchar as gebaeudename,\n" + 
    		"       c.gwr_egid::INTEGER as gwr_egid, c.gwr_edid as gwr_edid, \n" + 
    		"       b.text as lokalisationsname,\n" + 
    		"       to_date(f.gueltigereintrag, 'YYYYMMDD')as stand_am,\n" + 
    		"       a.gem_bfs as bfsnr\n" + 
    		" FROM av_avdpool_ng.gebaeudeadressen_lokalisation as a,\n" + 
    		"  av_avdpool_ng.gebaeudeadressen_lokalisationsname as b, \n" + 
    		"  (\n" + 
    		"    SELECT ogc_fid, tid, entstehung, gebaeudeeingang_von, CASE WHEN status IS NULL THEN 1 WHEN status = 0 THEN 0 ELSE 1 END as status, inaenderung, attributeprovisorisch, CASE WHEN istoffiziellebezeichnung = 0 THEN 1 ELSE 0 END as istoffiziellebezeichnung, lage, hoehenlage, hausnummer, im_gebaeude, gwr_egid, gwr_edid, gem_bfs, los, lieferdatum \n" + 
    		"    FROM av_avdpool_ng.gebaeudeadressen_gebaeudeeingang\n" + 
    		"  ) as c,\n" + 
    		"  av_mopublic_meta.lookup_tables_validity_type as d,\n" + 
    		"  av_mopublic_meta.lookup_tables_boolean_type as e,\n" + 
    		"  av_avdpool_ng.gebaeudeadressen_gebnachfuehrung as f\n" + 
    		" WHERE a.gem_bfs = ___GEM_BFS AND b.gem_bfs = ___GEM_BFS AND c.gem_bfs = ___GEM_BFS AND f.gem_bfs = ___GEM_BFS\n" + 
    		" AND c.gebaeudeeingang_von = a.tid\n" + 
    		" AND b.benannte = a.tid\n" + 
    		" AND c.status = d.code\n" + 
    		" AND c.istoffiziellebezeichnung = e.code\n" + 
    		" AND c.entstehung = f.tid\n" + 
    		") as eingang,\n" + 
    		"(\n" + 
    		" SELECT geom as geom_os, langtext as ortschaftsname\n" + 
    		" FROM av_plzortschaft.v_plzo_os\n" + 
    		") as ortschaft,\n" + 
    		"(\n" + 
    		" SELECT geom as geom_plz, plz::INTEGER, zusziff::INTEGER as zusatzziffern\n" + 
    		" FROM av_plzortschaft.plzo_plz\n" + 
    		") as plz\n" + 
    		"WHERE eingang.lage && ortschaft.geom_os\n" + 
    		"AND eingang.lage && plz.geom_plz\n" + 
    		"AND ST_Distance(eingang.lage, ortschaft.geom_os) = 0\n" + 
    		"AND ST_Distance(eingang.lage, plz.geom_plz) = 0;";
    
    public UpdateAddresses(HashMap params) throws ClassNotFoundException, SQLException {
        logger.setLevel(Level.INFO);

        this.params = params;
        readParams();
    
        Class.forName("org.postgresql.Driver"); 
        this.conn = DriverManager.getConnection("jdbc:postgresql://"+this.host+"/"+this.dbname, this.user, this.pwd);
    }
    
    public void run() throws NullPointerException, IndexOutOfBoundsException  {
        File dir = new File(importSourceDir);
        String[] itfFileList = dir.list(new FilenameFilter() {
            public boolean accept(File d, String name) {
                return name.toLowerCase().endsWith(".itf"); 
            }
        });
        logger.debug("Count of itf files: " + itfFileList.length);

        if (itfFileList != null) {
            for(String f : itfFileList) {
                logger.info("UpdateAddresses: " + dir.getAbsolutePath() + dir.separator + f);

                String gem_bfs = f.substring(0, 4);
                String sql1 = sql.replace("___GEM_BFS", gem_bfs);
//                logger.debug(sql1);

                try {
                    Statement t = null;
                    t = conn.createStatement();

                    try {
                        conn.setAutoCommit(false);

                        t.executeUpdate(sql1);
                                            
                        conn.commit();
                        conn.setAutoCommit(true);
                        logger.info("UpdateAddresses: " + gem_bfs + " commited.");

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
