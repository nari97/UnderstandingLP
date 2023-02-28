package Graph_JAVA;
import com.google.common.io.MoreFiles;
import com.google.common.io.RecursiveDeleteOption;
import org.neo4j.batchinsert.BatchInserter;
import org.neo4j.batchinsert.BatchInserters;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.graphdb.*;
import org.neo4j.io.layout.DatabaseLayout;

import java.io.*;
import java.util.*;
import java.util.stream.IntStream;

public class RuleQuery {

    public static void create_neo4j_database(String database_folder_path, ArrayList<ArrayList<Integer>> triples_to_add) throws IOException {
        /**
         Creates a neo4j database based on the given triples.
         @param database_folder_path the path of the folder where the neo4j database will be stored
         @param triples_to_add an ArrayList of ArrayList of integers, where each inner ArrayList represents a triple in the form (subject, predicate, object)
         @throws IOException if an I/O error occurs while accessing the database folder
         */

        File neo4j_folder = new File(database_folder_path + "/db/");
        if (neo4j_folder.exists())
            MoreFiles.deleteRecursively(neo4j_folder.toPath(), RecursiveDeleteOption.ALLOW_INSECURE);
        BatchInserter inserter = BatchInserters.inserter(DatabaseLayout.of(
                Config.newBuilder().set(GraphDatabaseSettings.neo4j_home, neo4j_folder.toPath()).build()));

        for (ArrayList<Integer> triple: triples_to_add){
            int s = triple.get(0);
            int p = triple.get(1);
            int o = triple.get(2);

            if (!inserter.nodeExists(s))
                inserter.createNode(s, new HashMap<>(), Label.label("Node"));
            if (!inserter.nodeExists(o))
                inserter.createNode(o, new HashMap<>(), Label.label("Node"));

            inserter.createRelationship(s, o, RelationshipType.withName("" + p), new HashMap<>());
        }

        inserter.shutdown();
    }

    public static ArrayList<Double> query_rule(Rule rule_to_query, String database_folder_path){
        /**

         This method performs a query on a given rule and returns the head coverage (HC) and partial completeness association confidence (PCA) of the rule.
         @param rule_to_query The rule to be queried.
         @param database_folder_path The path to the folder containing the Neo4j database.
         @return An ArrayList of two Double values, representing the HC and PCA respectively.
         */
        File neo4j_folder = new File(database_folder_path + "/db/");
        DatabaseManagementService service = new DatabaseManagementServiceBuilder(neo4j_folder.toPath()).
                setConfig(GraphDatabaseSettings.keep_logical_logs, "false").
                setConfig(GraphDatabaseSettings.preallocate_logical_logs, false).
                setConfig(GraphDatabaseSettings.pagecache_memory, "64G").
                setConfig(GraphDatabaseSettings.keep_logical_logs, "false").
                setConfig(GraphDatabaseSettings.preallocate_logical_logs, false).build();

        GraphDatabaseService db = service.database("neo4j");
        String nonFuncVar = rule_to_query.functional_variable.equals("a")?"b":"a";

        //Build query
        String query = "";
        Result res = null;

        for(Atom atom: rule_to_query.body_atoms)
            query += " MATCH " + atom.neo4j_print();

        Transaction tx = db.beginTx();

        System.out.println(new Date() + " -- Running body query");

        //Define hashmap
        Map<Long, Set<Long>> bodyPairs = new HashMap<>();
        try {
            res = tx.execute(query + " RETURN id(a) AS a, id(b) AS b");
            while (res.hasNext()) {
                Map<String, Object> row = res.next();

                // Get functional and non functional variable
                long fv = (long) row.get(rule_to_query.functional_variable), nfv = (long) row.get(nonFuncVar);

                // If nv does not exist, add nv using fv as key
                if (!bodyPairs.containsKey(fv))
                    bodyPairs.put(fv, new HashSet<>());

                bodyPairs.get(fv).add(nfv);
            }
            res.close();
        } catch(Exception e){
            e.printStackTrace();
        }

        System.out.println(new Date() + " -- Running head query");

        //Define hash set for support and pca
        Set<String> support = new HashSet<>(), pca = new HashSet<>();
        int totalHeads = 0;

        // Define set for functional variables
        Set<Long> allFVs = new HashSet<>();

        // Define query for head atom
        res = tx.execute("MATCH " + rule_to_query.head_atom.neo4j_print() + " RETURN id(a) AS a, id(b) AS b");
        while (res.hasNext()) {
            Map<String, Object> row = res.next();

            //Get fv and nfv
            long fv = (long) row.get(rule_to_query.functional_variable), nfv = (long) row.get(nonFuncVar);

            // Add fv to fv set
            allFVs.add(fv);

            // If bodypairs contains fv and the value is the nfv, add to support
            if (bodyPairs.containsKey(fv) && bodyPairs.get(fv).contains(nfv))
                support.add(fv + "," + nfv);

            totalHeads++;
        }
        res.close();

        System.out.println(new Date() + " -- Computing PCA");

        // Compute pca
        // Iterate through all fvs
        for (Long fv : allFVs)
            // If bodyPairs contains the fv
            if (bodyPairs.containsKey(fv))
                // For all values in key of body pairs, add to pca
                for (Long other : bodyPairs.get(fv))
                    pca.add(fv + "," + other);

        System.out.println("Body pairs size: " + bodyPairs.size());
        System.out.println("Head size: " + totalHeads);
        System.out.println("Support size: " + support.size());
        System.out.println("PCA size: " + pca.size());

        // make pca and hc
        double hc_for_rule = (1.0*support.size())/totalHeads;
        double pca_for_rule = (1.0*support.size())/pca.size();

        ArrayList<Double> metrics = new ArrayList<>();
        metrics.add(hc_for_rule);
        metrics.add(pca_for_rule);

        service.shutdown();

        return metrics;
    }

    public static HashMap<Integer, ArrayList> collect_materializations(String materialization_folder_path, String train_triples_path) throws FileNotFoundException {

        ArrayList<ArrayList<Integer>> triples = new ArrayList<>();

        HashMap<Integer, ArrayList> triple_dict = new HashMap<>();
        Scanner sc = new Scanner(new File(materialization_folder_path));
        int materialization_count = 0;
        int train_count = 0;
        while(sc.hasNext()){

            String line = sc.nextLine();
            if (line.equals("\n") || line.equals(""))
                continue;
            String[] splits = line.strip().split("\t");
            materialization_count++;
            int s = Integer.parseInt(splits[0]);
            int p =Integer.parseInt(splits[1]);
            int o = Integer.parseInt(splits[2]);

            if (!triple_dict.containsKey(p)){
                ArrayList<ArrayList<Integer>> temp= new ArrayList();
                triple_dict.put(p, temp);
            }

            ArrayList<Integer> triple = new ArrayList<>();
            triple.add(s);
            triple.add(p);
            triple.add(o);
            triple_dict.get(p).add(triple);
        }

        sc.close();

        sc = new Scanner(new File(train_triples_path));
        sc.nextLine();
        while(sc.hasNext()){

            String line = sc.nextLine();
            if (line.equals("\n") || line.equals(""))
                continue;
            train_count++;
            String[] splits = line.strip().split("\t");
            if (splits.length == 1){
                splits = line.strip().split(" ");
            }
            int s = Integer.parseInt(splits[0]);
            int o =Integer.parseInt(splits[1]);
            int p = Integer.parseInt(splits[2]);

            if (!triple_dict.containsKey(p)){
                ArrayList<ArrayList<Integer>> temp= new ArrayList();
                triple_dict.put(p, temp);
            }

            ArrayList<Integer> triple = new ArrayList<>();
            triple.add(s);
            triple.add(p);
            triple.add(o);
            triple_dict.get(p).add(triple);
        }

        System.out.println("Materialization count: " + materialization_count + ", Train count: " + train_count);
        return triple_dict;
    }

    public static ArrayList collect_materializations_for_rule(Rule rule, HashMap<Integer, ArrayList> triple_dict){
        Set<ArrayList<Integer>> triples = new HashSet<>();

        for(Atom atom: rule.body_atoms){

            int relationship = Integer.parseInt(atom.relationship);
//            System.out.println(triple_dict.get(relationship).size());
            triples.addAll(triple_dict.get(relationship));
        }

        int relationship = Integer.parseInt(rule.head_atom.relationship);
//        System.out.println(triple_dict.get(relationship).size());
        triples.addAll(triple_dict.get(relationship));

        return new ArrayList(triples);
    }



    public static void main(String[] args) throws IOException {

        String dataset_name = args[0];
        String model_name = args[1];
        String mat_file_name = args[2];
        String rule_file_name = args[3];
        double beta = Double.parseDouble(args[4]);
        String path_to_config_folder = args[5];

        Properties prop = new Properties();
        InputStream input = null;

        input = new FileInputStream(path_to_config_folder + "/config.properties");
        prop.load(input);

        String materialization_file_path = prop.getProperty("materialization_file_path") + dataset_name + "/" +  mat_file_name;
        String database_folder_path = prop.getProperty("database_folder_path");
        String rules_file_path = prop.getProperty("rules_file_path") + dataset_name + "/" +  rule_file_name;
        String dataset_folder_path = prop.getProperty("dataset_folder_path");
        String output_file_path = prop.getProperty("output_file_path") + dataset_name + "/" +  mat_file_name;
        String train_triples_path = dataset_folder_path + dataset_name + "/train2id.txt";

        RuleParser rp = new RuleParser(rules_file_path, dataset_folder_path, model_name, dataset_name, "\t");
        rp.parse_rules_from_file(beta);

        HashMap<Integer, ArrayList> triple_dict = collect_materializations(materialization_file_path, train_triples_path);
        FileWriter fileWriter = new FileWriter(output_file_path);
        BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
        int ctr = 0;
        for(Rule this_rule: rp.rules){
//            if (ctr == 25){
//                break;
//            }
            System.out.println("\nProcessing rule " + ctr + "/" + rp.rules.size() + ": " + this_rule.id_print());
            ArrayList<ArrayList<Integer>> triples = collect_materializations_for_rule(this_rule, triple_dict);
            create_neo4j_database(database_folder_path, triples);
            ArrayList<Double> metrics = query_rule(this_rule, database_folder_path);

            bufferedWriter.write(this_rule.id_print() + "\t" + this_rule.head_coverage + "\t" + this_rule.pca_confidence + "\t" + metrics.get(0) + "\t" + metrics.get(1) + "\n");
            bufferedWriter.flush();
            ctr++;
        }
        bufferedWriter.close();
    }
}