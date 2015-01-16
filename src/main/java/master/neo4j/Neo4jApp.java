package master.neo4j;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.neo4j.cypher.ExecutionEngine;
import org.neo4j.cypher.ExecutionResult;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.kernel.impl.util.StringLogger;

/**
 *
 * @author fran
 */
public class Neo4jApp {

    // TODO: Cambiar a uno editable con properties
    private static final String DB_PATH = "/opt/neo4j-community-2.1.4/";
    private final GraphDatabaseService db;

    private final ExecutionEngine engine;

    private static enum RelTypes implements RelationshipType {

        HAS, IS_FRIEND, WROTE, REVIEWED
    }

    public Neo4jApp(String filePath) {
        this.db = new GraphDatabaseFactory().
                newEmbeddedDatabase(DB_PATH);
        this.engine = new ExecutionEngine(db, StringLogger.SYSTEM);

    }

    private ExecutionResult loadConferences(String path) {
        ExecutionResult result = null;
        try (Transaction ignored = db.beginTx()) {
            result = engine.execute("USING PERIODIC COMMIT 1000\n"
                    + "LOAD CSV WITH HEADERS FROM \"file:////" + path + "/processed_conferences.csv\"\n"
                    + "AS conferences\n"
                    + "WITH conferences, toInt(conferences.Year) as Year, [w in split(conferences.Authors,\";\")] AS auths\n"
                    + "MERGE (p:Paper {title:conferences.Title, year:coalesce(Year,\"none\")})\n"
                    + "FOREACH (auth IN auths |\n"
                    + "	MERGE (a:Author {surname:trim(auth)})\n"
                    + "	MERGE (a)-[:WROTE]->(p)\n"
                    + ")\n"
                    + "MERGE (c:Conference {name:conferences.ConferenceName,city:conferences.City,year:Year})\n"
                    + "MERGE (c)-[r:HAS]->(p)\n"
                    + "MERGE (rev:Reviewer {surname:conferences.Reviewer})\n"
                    + "MERGE (rev)-[re:REVIEWED]->(p)");
        } catch (Exception e) {
        }
        return result;
    }

    private ExecutionResult loadJournals(String path) {
        ExecutionResult result = null;
        try (Transaction ignored = db.beginTx()) {
            result = engine.execute("USING PERIODIC COMMIT 1000\n"
                    + "LOAD CSV WITH HEADERS FROM \"file:////" + path + "/processed_journals.csv\"\n"
                    + "AS journals\n"
                    + "WITH journals, [w in split(journals.Authors,\";\")] AS auths\n"
                    + "MERGE(p:Paper {title:journals.Title})\n"
                    + "MERGE (j:Journal {name:journals.JournalName,volume:journals.Volume})\n"
                    + "MERGE (j)-[r:HAS]->(p)\n"
                    + "FOREACH (auth IN auths |\n"
                    + "	MERGE (a:Author {surname:auth})\n"
                    + "	MERGE (a)-[w:WROTE]->(p)\n"
                    + ")\n"
                    + "MERGE (rev:Reviewer {surname:journals.Reviewer})\n"
                    + "MERGE (rev)-[re:REVIEWED]->(p)");
        } catch (Exception e) {
        }
        return result;
    }

    private ExecutionResult loadFriendships(String path) {
        ExecutionResult result = null;
        try (Transaction ignored = db.beginTx()) {
            result = engine.execute("USING PERIODIC COMMIT 1000\n"
                    + "LOAD CSV WITH HEADERS FROM \"file:////" + path + "/processed_friendships.csv\"\n"
                    + "AS f\n"
                    + "WITH f\n"
                    + "MERGE (rev:Reviewer {surname:f.Reviewer})\n"
                    + "MERGE (a:Author {surname:f.Author})\n"
                    + "MERGE (a)-[i:IS_FRIEND]->(rev)");
        } catch (Exception e) {
        }
        return result;
    }

    private void deleteTempFiles(File[] files) {
        for (File file : files) {
            if (file.delete()) {
                System.out.println(file.getName() + " is deleted!");
            } else {
                System.out.println("Delete operation is failed.");
            }
        }
    }

    /**
     * Process a line of the file by trimming whitespaces of each array of
     * Strings passed as an argument.
     *
     * @param colStrings
     * @param authStrings
     * @return String with the processedLine ready to be written in a file
     */
    private String processLine(String[] colStrings, String[] authStrings) {
        String processedLine = "";
        for (int i = 0; i <= colStrings.length - 1; i++) {
            if (i != 1) {
                processedLine += colStrings[i].trim();
                // We don't write the separator in the last element
                if (i != colStrings.length - 1) {
                    processedLine += ",";
                }
            } else {
                for (int j = 0; j <= authStrings.length - 1; j++) {
                    processedLine += authStrings[j].trim();
                    // We don't write the separator in the last element
                    if (j != (authStrings.length - 1)) {
                        processedLine += ";";
                    } else {
                        processedLine += ",";
                    }
                }
            }
        }
        // We turn to lowerCase to avoid problems at load
        return processedLine.toLowerCase();
    }

    private File preProcessFile(File file) throws IOException {
        String line;
        String[] columStrings;
        String[] authors;
        // We create the postProcessed file
        File fout = new File("processed_" + file.getName());
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(fout, true))) {
            System.out.println("PreProcessing file" + file.getName());
            // We now read the original file to process it
            BufferedReader fileReader = new BufferedReader(new FileReader(file));
            String processedLine;

            // we write the first line with the column names
            bw.write(fileReader.readLine());
            bw.newLine();
            while ((line = fileReader.readLine()) != null) {
                // We extract all columns
                columStrings = line.split(",");
                // We extract the authors
                authors = columStrings[1].split(";");
                // We write the processed line to the temp file
                processedLine = processLine(columStrings, authors);
                bw.write(processedLine);
                bw.newLine();
            }
            System.out.println("File succesfully written: " + fout.getAbsolutePath());
        }
        return fout;
    }

    /**
     * Ingests all csv files into Neo4j
     *
     * @param path
     */
    private void ingestDatabase(String path) {
        loadConferences(path);
        loadJournals(path);
        loadFriendships(path);
    }

    /**
     * Receives directory path in which csv files are located, and populates the
     * database with them.
     *
     * @param path
     */
    public void populate(String path) {
        File folder = new File(path);
        File[] listOfFiles = folder.listFiles();
        File[] tempFiles = new File[3];
        if (listOfFiles != null) {
            int i = 0;
            // We read all csv files in path
            for (File file : listOfFiles) {
                if (file.isFile() && file.getName().endsWith(".csv")) {
                    try {
                        tempFiles[i] = preProcessFile(file);
                        i++;
                    } catch (IOException ex) {
                        Logger.getLogger(Neo4jApp.class.getName()).log(Level.SEVERE, null, ex);
                    }
                }
            }
        } else {
            try {
                throw new IOException("Path: " + path + " doesn't seem to be a directory");
            } catch (IOException ex) {
                Logger.getLogger(Neo4jApp.class.getName()).log(Level.SEVERE, null, ex);
            }
        }

        // When all files have been processed we ingest the database
        ingestDatabase(path);
        deleteTempFiles(tempFiles);
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
        // TODO: Cambiar para que el dbpath sea un parametro
        Neo4jApp neo4jApp = new Neo4jApp(DB_PATH);
        // TODO: Path cambiar como parametro
        neo4jApp.populate("/home/fran/Proyectos/PracticaNeo4j");
    }
}
