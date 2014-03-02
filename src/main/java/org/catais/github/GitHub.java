package org.catais.github;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.catais.svn.Commit2Svn;
import org.eclipse.jgit.api.*;
import org.eclipse.jgit.api.errors.*;
import org.eclipse.jgit.api.CreateBranchCommand.SetupUpstreamMode;
import org.eclipse.jgit.internal.storage.file.FileRepository;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.api.CommitCommand;
import org.eclipse.jgit.storage.file.*;


public class GitHub {
    private static Logger logger = Logger.getLogger(GitHub.class);
    
    private String localPath;
    private String remotePath;
    private Repository localRepo;
    private Git git;

    public GitHub(String localPath, String remotePath) throws IOException {
        logger.setLevel(Level.INFO);
        
        this.localPath = localPath;
        this.remotePath = remotePath;
        localRepo = new FileRepository(localPath + "/.git");
        git = new Git(localRepo);  
    }
    
    public void commit() throws NoHeadException, NoMessageException, UnmergedPathsException, ConcurrentRefUpdateException, WrongRepositoryStateException, GitAPIException {
        CommitCommand gc = git.commit();
        gc.setCommitter("sogeo", "stefan.ziegler.de@gmail.com");
        gc.setAll(true);
        
        Calendar calendar = Calendar.getInstance();
        int dayOfYear = calendar.get(Calendar.DAY_OF_YEAR);
        int weekOfYear = calendar.get(Calendar.WEEK_OF_YEAR);
        
        Date today = Calendar.getInstance().getTime();
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd-hh.mm.ss");
        String commitDate = formatter.format(today);
        
        gc.setMessage("Wöchentliche Datenlieferung (" + weekOfYear + " / " + dayOfYear + " / " + commitDate + ")");
        gc.call();
    }
    
    public void push() throws InvalidRemoteException, TransportException, GitAPIException {
        git.push().call();
    }
}
