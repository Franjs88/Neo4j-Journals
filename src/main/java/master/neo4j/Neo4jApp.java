package master.neo4j;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.RelationshipType;

/**
 *
 * @author fran
 */
public class Neo4jApp {
    
    private static final String DB_PATH = "";    
    GraphDatabaseService graphDb;
    
    private static enum RelTypes implements RelationshipType
    {
        HAS, IS_FRIEND, WROTE, REVIEWED
    }

    public Neo4jApp(String filePath) {
        
    }
    
    /**
     * Receives directory path in which csv files are located, and populates the 
     * database with them.
     * @param path 
     */
    public void populate(String path) {
        
    }
    
    /**
     *
     * @param paperName
     */
    public void runQ1(String paperName) {
        
    }
    
    /**
     *
     * @param conferenceName
     */
    public void runQ2(String conferenceName) {
        
    }
    
    /**
     *
     * @param autorName
     */
    public void runQ3(String autorName) {
        
    }
    
    /**
     *
     * @param journalName
     */
    public void runQ4(String journalName) {
        
    }
    
    /**
     * 
     * @param args 
     */
    public static void main(String[] args) {
        
    }
}
