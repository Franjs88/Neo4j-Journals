package master.neo4j;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.neo4j.cypher.ExecutionEngine;
import org.neo4j.cypher.ExecutionResult;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.kernel.impl.util.StringLogger;

/**
 *
 * @author fran
 */
public class Neo4jApp {

    private static String DB_PATH;
    private final GraphDatabaseService db;

    // File in which results are written down
    private final File fout = new File("Fco.JavierSanchezCarmona.log");

    private final ExecutionEngine engine;

    private static enum RelTypes implements RelationshipType {

        HAS, IS_FRIEND, WROTE, REVIEWED
    }

    public Neo4jApp(String filePath) {
        this.db = new GraphDatabaseFactory().newEmbeddedDatabase(DB_PATH);
        registerShutdownHook(db);
        this.engine = new ExecutionEngine(db, StringLogger.SYSTEM);
    }

    private static void registerShutdownHook(final GraphDatabaseService graphDb) {
        // Registers a shutdown hook for the Neo4j instance so that it
        // shuts down nicely when the VM exits (even if you "Ctrl-C" the
        // running application).
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                graphDb.shutdown();
            }
        });
    }

    private ExecutionResult loadConferences(String path) {
        ExecutionResult result = null;
        try (Transaction tx = db.beginTx()) {
            result = engine.execute("LOAD CSV WITH HEADERS FROM \"file:////"
                    + path + "/processed_conferences.csv\"\n"
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
            tx.success();
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("Transaction loadconfs failed \n");
        }
        return result;
    }

    private ExecutionResult loadJournals(String path) {
        ExecutionResult result = null;
        try (Transaction tx = db.beginTx()) {
            result = engine.execute("LOAD CSV WITH HEADERS FROM \"file:////"
                    + path + "/processed_journals.csv\"\n"
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
            tx.success();
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("Transaction loadJournals failed \n");
        }
        return result;
    }

    private ExecutionResult loadFriendships(String path) {
        ExecutionResult result = null;
        try (Transaction tx = db.beginTx()) {
            result = engine.execute("LOAD CSV WITH HEADERS FROM \"file:////"
                    + path + "/processed_friendships.csv\"\n"
                    + "AS f\n"
                    + "WITH f\n"
                    + "MERGE (rev:Reviewer {surname:f.Reviewer})\n"
                    + "MERGE (a:Author {surname:f.Author})\n"
                    + "MERGE (a)-[i:IS_FRIEND]->(rev)");
            tx.success();
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("Transaction loadFs failed \n");
        }
        return result;
    }

    private void deleteTempFiles(File[] files) {
        System.out.println("Now we delete the temporal files:\n");
        for (File file : files) {
            if (file.delete()) {
                System.out.println(file.getName() + " is deleted!");
            } else {
                System.out.println("Delete operation is failed. \n");
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
        File fileOutput = new File("processed_" + file.getName());
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(fileOutput, true))) {
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
            System.out.println("File succesfully written: " + fileOutput.getAbsolutePath() + "\n");
        }
        return fileOutput;
    }

    /**
     * Ingests all csv files into Neo4j
     *
     * @param path
     */
    private void ingestDatabase(String path) {
        ExecutionResult result = loadConferences(path);
        System.out.println("Ingestion of conferences return: \n" + result.dumpToString() + "\n");
        result = loadJournals(path);
        System.out.println("Ingestion of journals return: \n" + result.dumpToString() + "\n");
        result = loadFriendships(path);
        System.out.println("Ingestion of friendships return: \n" + result.dumpToString() + "\n");
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
                    } catch (IOException ex) {;
                        System.err.println("Hay ficheros corruptos con nombre 'processed_*'"
                                + " eliminelos, para poder "
                                + "ejecutar el programa correctamente");
                        
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
        // and delete them after the ingestion
        deleteTempFiles(tempFiles);
    }

    /**
     * Writes down a result from Neo4j to a log file with the name of the
     * programmer. Query RETURNS: a:Author, r:Reviewer, p:Paper
     *
     * @param result
     * @return
     * @throws IOException
     */
    private void writeQuery1ResultFile(ResourceIterator<Map<String, Object>> iter)
            throws IOException {
        Map<String, Object> next;
        String paper = "";
        ArrayList<String> authors = new ArrayList<>();
        String reviewer = "";
        String result = "Q1: ";
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(fout, true))) {
            // We append the name of the paper first
            if (iter.hasNext()) {
                next = iter.next();
                paper = (String) ((Node) next.get("p")).getProperty("title");
                result += paper;
                // And start to add authors and reviewers to their collections
                authors.add((String) ((Node) next.get("a")).getProperty("surname"));
                reviewer = (String) ((Node) next.get("r")).getProperty("surname");
            }

            // Now we iterate over the rest of the Resources
            while (iter.hasNext()) {
                next = iter.next();
                authors.add((String) ((Node) next.get("a")).getProperty("surname"));
            }
            result += "," + authors.toString() + "," + reviewer;
            System.out.println(
                    "\n===========================\n"
                    + "Consulta 1 devuelve: \n"
                    + "===========================\n"
                    + "autor: " + authors.toString()
                    + "\n"
                    + "reviewer: " + reviewer + "\n"
                    + "paper: " + paper
            );
            // We now write the result to file
            bw.write(result);
            bw.newLine();
            System.out.println("File succesfully written: " + fout.getAbsolutePath() + "\n");
        }
    }

    /**
     * Writes down a result from Neo4j to a log file with the name of the
     * programmer. Query RETURNS: c:Conference p:Paper
     *
     * @param result
     * @return
     * @throws IOException
     */
    private void writeQuery2ResultFile(ResourceIterator<Map<String, Object>> iter)
            throws IOException {
        Map<String, Object> next;
        ArrayList<String> papers = new ArrayList<>();
        String conference = "";
        String result = "Q2: ";
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(fout, true))) {
            // We append the name of the conference first
            if (iter.hasNext()) {
                next = iter.next();
                conference = (String) ((Node) next.get("c")).getProperty("name");
                result += conference;
                // And start to add papers to its collection
                papers.add((String) ((Node) next.get("p")).getProperty("title"));
            }

            // Now we iterate over the rest of the Resources
            while (iter.hasNext()) {
                next = iter.next();
                System.out.println("");
                papers.add((String) ((Node) next.get("p")).getProperty("title"));
            }
            result += "," + papers.toString();
            System.out.println(
                    "\n===========================\n"
                    + "Consulta 2 devuelve: \n"
                    + "===========================\n"
                    + "conference: " + conference
                    + "\n"
                    + "paper: " + papers.toString()
            );
            // We now write the result to file
            bw.write(result);
            bw.newLine();
            System.out.println("File succesfully written: " + fout.getAbsolutePath() + "\n");
        }
    }

    /**
     * Writes down a result from Neo4j to a log file with the name of the
     * programmer. Query RETURNS: a:Author p:Paper
     *
     * @param result
     * @return
     * @throws IOException
     */
    private void writeQuery3ResultFile(ResourceIterator<Map<String, Object>> iter)
            throws IOException {
        Map<String, Object> next;
        String author = "";
        ArrayList<String> papers = new ArrayList<>();
        String result = "Q3: ";
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(fout, true))) {
            // We append the surname of the author first
            if (iter.hasNext()) {
                next = iter.next();
                author = (String) ((Node) next.get("a")).getProperty("surname");
                result += author;
                // And start to add papers to its collection
                papers.add((String) ((Node) next.get("p")).getProperty("title"));
            }

            // Now we iterate over the rest of the Resources
            while (iter.hasNext()) {
                next = iter.next();
                papers.add((String) ((Node) next.get("p")).getProperty("title"));
            }
            result += "," + papers.toString();
            System.out.println(
                    "\n===========================\n"
                    + "Consulta 3 devuelve: \n"
                    + "===========================\n"
                    + "autor: " + author
                    + "\n"
                    + "paper: " + papers.toString()
            );
            // We now write the result to file
            bw.write(result);
            bw.newLine();
            System.out.println("File succesfully written: " + fout.getAbsolutePath() + "\n");
        }
    }

    /**
     * Writes down a result from Neo4j to a log file with the name of the
     * programmer. Query RETURNS: p:Paper a:Author r:Reviewer
     *
     * @param result
     * @return
     * @throws IOException
     */
    private void writeQuery4ResultFile(ResourceIterator<Map<String, Object>> iter)
            throws IOException {
        Map<String, Object> next;
        String author;
        String paper;
        String reviewer;
        String result = "Q4: ";
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(fout, true))) {
            while (iter.hasNext()) {
                next = iter.next();
                paper = (String) ((Node) next.get("p")).getProperty("title");
                author = (String) ((Node) next.get("a")).getProperty("surname");
                reviewer = (String) ((Node) next.get("r")).getProperty("surname");
                result += "[" + paper + ", " + author + ", " + reviewer + "]";
                // We now write the result to file
                bw.write(result);
                bw.newLine();
                // We start again
                result = "Q4: ";
            }
            System.out.println(
                    "\n===========================\n"
                    + "Consulta 4 escrita: \n"
                    + "==========================="
            );
            System.out.println("File succesfully written: " + fout.getAbsolutePath() + "\n");
        }
    }

    /**
     * We must give parameters with lowercase because preprocessing has changed
     * the original names.
     *
     * @param paperName
     * @throws java.io.IOException
     */
    public void runQ1(String paperName) throws IOException {
        ExecutionResult result;
        ResourceIterator iter;
        try (Transaction tx = db.beginTx()) {
            result = engine.execute(
                    "MATCH (a:Author)-[WROTE]->(p:Paper)<-[REVIEWED]-(r:Reviewer)\n"
                    + "WHERE p.title ='" + paperName + "' \n"
                    + "RETURN p,a,r;");
            iter = result.javaIterator();
            tx.success();
            writeQuery1ResultFile(iter);
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("Transaction runQ1:"
                    + " reviewers and authors of a given paper failed \n");
        }
    }

    /**
     * We must give parameters with lowercase because preprocessing has changed
     * the original names.
     *
     * @param conferenceName
     * @throws java.io.IOException
     */
    public void runQ2(String conferenceName) throws IOException {
        ExecutionResult result;
        ResourceIterator iter;
        try (Transaction tx = db.beginTx()) {
            result = engine.execute(
                    "MATCH (c:Conference)-[HAS]->(p:Paper)\n"
                    + "WHERE c.name ='" + conferenceName + "'\n"
                    + "RETURN c,p;");
            iter = result.javaIterator();
            tx.success();
            writeQuery2ResultFile(iter);
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("Transaction runQ2: conferences by name failed\n");
        }
    }

    /**
     * We must give parameters with lowercase because preprocessing has changed
     * the original names.
     *
     * @param autorName
     * @throws java.io.IOException
     */
    public void runQ3(String autorName) throws IOException {
        ExecutionResult result;
        ResourceIterator iter;
        try (Transaction tx = db.beginTx()) {
            result = engine.execute(
                    "MATCH (a:Author {surname:'" + autorName + "'})-[:WROTE]->(p:Paper)\n"
                    + "RETURN a,p;");
            iter = result.javaIterator();
            tx.success();
            writeQuery3ResultFile(iter);
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("Transaction runQ3:"
                    + " papers written by author failed\n");
        }
    }

    /**
     * We must give parameters with lowercase because preprocessing has changed
     * the original names to lowercase.
     *
     * @param journalName
     * @param journalVolume
     */
    public void runQ4(String journalName, int journalVolume) {
        ExecutionResult result;
        ResourceIterator iter;
        try (Transaction tx = db.beginTx()) {
            result = engine.execute(
                    "MATCH ("
                    + "j:Journal {volume:'" + journalVolume + "',name:'" + journalName + "'}"
                    + ")-[:HAS]->(p:Paper)<-[:WROTE]-(a:Author)-[:IS_FRIEND]->(r:Reviewer)\n"
                    + "RETURN p,a,r;");
            iter = result.javaIterator();
            tx.success();
            writeQuery4ResultFile(iter);
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("Transaction runQ4:"
                    + " Query 4 failed \n");
        }
    }

    /**
     *
     * @param args
     * @throws java.io.IOException
     */
    public static void main(String[] args) throws IOException {
        // We load parameters from a property file
        Properties p = new Properties();
        p.load(new InputStreamReader(new FileInputStream(new File("Neo4jAppParams.property")), "UTF-8"));
        for (String key : p.stringPropertyNames()) {
            System.out.println("key=" + key + ", value=" + p.getProperty(key));
        }
        // We load parameters from the property file
        DB_PATH = p.getProperty("DB_PATH");
        String CSVPath = p.getProperty("path_to_csv_files");
        String Q1Param = p.getProperty("Q1_param");
        String Q2Param = p.getProperty("Q2_param");
        String Q3Param = p.getProperty("Q3_param");
        String Q4Param1 = p.getProperty("Q4_param1");
        int Q4Param2 = Integer.parseInt(p.getProperty("Q4_param2"));

        // Start the program
        Neo4jApp neo4jApp = new Neo4jApp(DB_PATH);
        neo4jApp.populate(CSVPath);
        // We execute the queries with the given parameters
        neo4jApp.runQ1(Q1Param);
        neo4jApp.runQ2(Q2Param);
        neo4jApp.runQ3(Q3Param);
        neo4jApp.runQ4(Q4Param1, Q4Param2);
    }
}
