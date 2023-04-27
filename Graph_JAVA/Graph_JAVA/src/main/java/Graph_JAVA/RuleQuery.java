package Graph_JAVA;
import com.google.common.io.MoreFiles;
import com.google.common.io.RecursiveDeleteOption;
import org.apache.shiro.crypto.hash.Hash;
import org.neo4j.batchinsert.BatchInserter;
import org.neo4j.batchinsert.BatchInserters;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.graphdb.*;
import org.neo4j.io.layout.DatabaseLayout;

import java.io.*;
import java.lang.reflect.Array;
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

        File neo4j_folder = new File(database_folder_path);
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

    public static long get_number_of_entities(String path_to_neo4j_database){
        File neo4j_folder = new File(path_to_neo4j_database);
        DatabaseManagementService service = new DatabaseManagementServiceBuilder(neo4j_folder.toPath()).
                setConfig(GraphDatabaseSettings.keep_logical_logs, "false").
                setConfig(GraphDatabaseSettings.preallocate_logical_logs, false).
                setConfig(GraphDatabaseSettings.pagecache_memory, "64G").
                setConfig(GraphDatabaseSettings.keep_logical_logs, "false").
                setConfig(GraphDatabaseSettings.preallocate_logical_logs, false).build();

        GraphDatabaseService db = service.database("neo4j");
        Transaction tx = db.beginTx();
        String query = "MATCH (a) RETURN count(a) as cnt";
        long n_entities = 0;
        try{
            Result res = tx.execute(query);
            while(res.hasNext()){
                Map<String, Object> row = res.next();
                n_entities = (long) row.get("cnt");
            }
        } catch (Exception e){
            e.printStackTrace();
        }
        service.shutdown();
        return n_entities;
    }

    public static long get_pca_denominator(Transaction tx, Rule r, HashMap<Long, ArrayList<Long>> node_degree_dict, long n_entities){

        String fv = r.functional_variable;
        String query = "MATCH " + r.body_atoms.get(0).neo4j_print() + " RETURN id(a) as a, id(b) as b";
        long pca_denom = 0;
        try{
            Result res = tx.execute(query);

            while(res.hasNext()){
                Map<String, Object> row = res.next();

                long functionalVar = (long) row.get(fv);
                if (node_degree_dict.containsKey(functionalVar)){
                    if (fv.equals("b")){
                        if (n_entities - node_degree_dict.get(functionalVar).get(1) > 0)
                            ++pca_denom;
                    }
                    else{
                        if (n_entities - node_degree_dict.get(functionalVar).get(0) > 0) {
                            ++pca_denom;
                        }
                    }

                }

            }
        } catch (Exception e){
            e.printStackTrace();
        }

        return pca_denom;
    }

    public static HashMap<Long, ArrayList<Long>> get_node_degrees(String path_to_neo4j_database){

        String query = "MATCH (z) RETURN id(z) as var, size(()-[]->(z)) as inDegree, size((z)-[]->()) as outDegree";
        HashMap<Long, ArrayList<Long>> node_degree_dict = new HashMap<>();
        File neo4j_folder = new File(path_to_neo4j_database);
        DatabaseManagementService service = new DatabaseManagementServiceBuilder(neo4j_folder.toPath()).
                setConfig(GraphDatabaseSettings.keep_logical_logs, "false").
                setConfig(GraphDatabaseSettings.preallocate_logical_logs, false).
                setConfig(GraphDatabaseSettings.pagecache_memory, "64G").
                setConfig(GraphDatabaseSettings.keep_logical_logs, "false").
                setConfig(GraphDatabaseSettings.preallocate_logical_logs, false).build();

        GraphDatabaseService db = service.database("neo4j");
        Transaction tx = db.beginTx();

        try{
            Result res = tx.execute(query);

            while(res.hasNext()){
                Map<String, Object> row = res.next();
                long var = (long) row.get("var");
                long inDegree = (long) row.get("inDegree");
                long outDegree = (long) row.get("outDegree");

                ArrayList<Long> degrees = new ArrayList<>();
                degrees.add(outDegree);
                degrees.add(inDegree);
                node_degree_dict.put(var, degrees);
            }
            res.close();
        } catch (Exception e){
            e.printStackTrace();
        }
        service.shutdown();

        return node_degree_dict;
    }

    public static ArrayList<Double> query_rule_asymmetry(Rule rule_to_query, String database_folder_path, HashMap<Long, ArrayList<Long>> node_degree_dict, long n_entities){

        File neo4j_folder = new File(database_folder_path);
        DatabaseManagementService service = new DatabaseManagementServiceBuilder(neo4j_folder.toPath()).
                setConfig(GraphDatabaseSettings.keep_logical_logs, "false").
                setConfig(GraphDatabaseSettings.preallocate_logical_logs, false).
                setConfig(GraphDatabaseSettings.pagecache_memory, "64G").
                setConfig(GraphDatabaseSettings.keep_logical_logs, "false").
                setConfig(GraphDatabaseSettings.preallocate_logical_logs, false).build();

        GraphDatabaseService db = service.database("neo4j");
        Transaction tx = db.beginTx();
        long support = 0;

        try {
            Result res = tx.execute("MATCH " + rule_to_query.body_atoms.get(0).neo4j_print() + " WHERE NOT EXISTS(" + rule_to_query.head_atom.neo4j_print() + ") WITH DISTINCT a, b RETURN COUNT(*) as cnt");
            while (res.hasNext()) {
                Map<String, Object> row = res.next();
                support = (long) row.get("cnt");
            }
            res.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        long heads = 0;
        try {
            Result res = tx.execute("MATCH (a) MATCH (b) WHERE NOT EXISTS(" + rule_to_query.head_atom.neo4j_print() + ") WITH DISTINCT a, b RETURN count(*) as cnt");
            while (res.hasNext()) {
                Map<String, Object> row = res.next();
                heads = (long) row.get("cnt");
            }
            res.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        long pca = get_pca_denominator(tx, rule_to_query, node_degree_dict, n_entities);

        System.out.println("\tComputed support: " + support);
        System.out.println("\tComputed tHeads: " + heads);
        System.out.println("\tComputed PCA: " + pca);

        System.out.println("\tHead coverage: " + (support * 1.0 / heads));
        System.out.println("\tPCA confidence: " + (support * 1.0 / pca));

        double hc_for_rule = (1.0*support)/heads;
        double pca_for_rule = (1.0*support)/pca;

        ArrayList<Double> metrics = new ArrayList<>();
        metrics.add(hc_for_rule);
        metrics.add(pca_for_rule);

        service.shutdown();

        return metrics;
    }

    public static ArrayList<Double> query_rule(Rule rule_to_query, String database_folder_path){
        /**

         This method performs a query on a given rule and returns the head coverage (HC) and partial completeness association confidence (PCA) of the rule.
         @param rule_to_query The rule to be queried.
         @param database_folder_path The path to the folder containing the Neo4j database.
         @return An ArrayList of two Double values, representing the HC and PCA respectively.
         */
        File neo4j_folder = new File(database_folder_path );
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

        System.out.println("\tBody pairs size: " + bodyPairs.size());
        System.out.println("\tHead size: " + totalHeads);
        System.out.println("\tSupport size: " + support.size());
        System.out.println("\tPCA size: " + pca.size());

        // make pca and hc
        double hc_for_rule = (1.0*support.size())/totalHeads;
        double pca_for_rule = (1.0*support.size())/pca.size();

        ArrayList<Double> metrics = new ArrayList<>();
        metrics.add(hc_for_rule);
        metrics.add(pca_for_rule);

        service.shutdown();

        return metrics;
    }

    public static HashMap<Integer, ArrayList<ArrayList<Integer>>> collect_materializations(String materialization_folder_path, String train_triples_path, String valid_triples_path, String test_triples_path) throws FileNotFoundException {

        HashMap<Integer, ArrayList<ArrayList<Integer>>> triple_dict = new HashMap<>();
        Scanner sc = null;
        int materialization_count = 0;
        int train_count = 0;
        int valid_count = 0;
        int test_count = 0 ;

        if (materialization_folder_path!=null) {
            sc = new Scanner(new File(materialization_folder_path));

            while (sc.hasNext()) {

                String line = sc.nextLine();
                if (line.equals("\n") || line.equals(""))
                    continue;
                String[] splits = line.strip().split("\t");
                materialization_count++;
                int s = Integer.parseInt(splits[0]);
                int p = Integer.parseInt(splits[1]);
                int o = Integer.parseInt(splits[2]);

                if (!triple_dict.containsKey(p)) {
                    ArrayList<ArrayList<Integer>> temp = new ArrayList<>();
                    triple_dict.put(p, temp);
                }

                ArrayList<Integer> triple = new ArrayList<>();
                triple.add(s);
                triple.add(p);
                triple.add(o);
                triple_dict.get(p).add(triple);
            }

            sc.close();
        }

        if(train_triples_path!=null) {
            sc = new Scanner(new File(train_triples_path));
            sc.nextLine();
            while (sc.hasNext()) {

                String line = sc.nextLine();
                if (line.equals("\n") || line.equals(""))
                    continue;
                train_count++;
                String[] splits = line.strip().split("\t");
                if (splits.length == 1) {
                    splits = line.strip().split(" ");
                }
                int s = Integer.parseInt(splits[0]);
                int o = Integer.parseInt(splits[1]);
                int p = Integer.parseInt(splits[2]);

                if (!triple_dict.containsKey(p)) {
                    ArrayList<ArrayList<Integer>> temp = new ArrayList<>();
                    triple_dict.put(p, temp);
                }

                ArrayList<Integer> triple = new ArrayList<>();
                triple.add(s);
                triple.add(p);
                triple.add(o);
                triple_dict.get(p).add(triple);
            }
        }

        if(test_triples_path!=null) {
            sc = new Scanner(new File(test_triples_path));
            sc.nextLine();
            while (sc.hasNext()) {

                String line = sc.nextLine();
                if (line.equals("\n") || line.equals(""))
                    continue;
                test_count++;
                String[] splits = line.strip().split("\t");
                if (splits.length == 1) {
                    splits = line.strip().split(" ");
                }
                int s = Integer.parseInt(splits[0]);
                int o = Integer.parseInt(splits[1]);
                int p = Integer.parseInt(splits[2]);

                if (!triple_dict.containsKey(p)) {
                    ArrayList<ArrayList<Integer>> temp = new ArrayList<>();
                    triple_dict.put(p, temp);
                }

                ArrayList<Integer> triple = new ArrayList<>();
                triple.add(s);
                triple.add(p);
                triple.add(o);
                triple_dict.get(p).add(triple);
            }
        }

        if(valid_triples_path!=null) {
            sc = new Scanner(new File(valid_triples_path));
            sc.nextLine();
            while (sc.hasNext()) {

                String line = sc.nextLine();
                if (line.equals("\n") || line.equals(""))
                    continue;
                valid_count++;
                String[] splits = line.strip().split("\t");
                if (splits.length == 1) {
                    splits = line.strip().split(" ");
                }
                int s = Integer.parseInt(splits[0]);
                int o = Integer.parseInt(splits[1]);
                int p = Integer.parseInt(splits[2]);

                if (!triple_dict.containsKey(p)) {
                    ArrayList<ArrayList<Integer>> temp = new ArrayList<>();
                    triple_dict.put(p, temp);
                }

                ArrayList<Integer> triple = new ArrayList<>();
                triple.add(s);
                triple.add(p);
                triple.add(o);
                triple_dict.get(p).add(triple);
            }
        }

        System.out.println("Materialization count: " + materialization_count + ", Train count: " + train_count + ", Valid count: " + valid_count + "" + ", Test count: " + test_count);
        return triple_dict;
    }

    public static ArrayList<ArrayList<Integer>> collect_materializations_for_rule(Rule rule, HashMap<Integer, ArrayList<ArrayList<Integer>>> triple_dict){
        Set<ArrayList<Integer>> triples = new HashSet<>();

        for(Atom atom: rule.body_atoms){

            int relationship = Integer.parseInt(atom.relationship);
//            System.out.println(triple_dict.get(relationship).size());
            triples.addAll(triple_dict.get(relationship));
        }

        int relationship = Integer.parseInt(rule.head_atom.relationship);
//        System.out.println(triple_dict.get(relationship).size());
        triples.addAll(triple_dict.get(relationship));

        return new ArrayList<>(triples);
    }



    public static void main(String[] args) throws IOException {

        String dataset_name = args[0];
        String model_name = args[1];
        String mat_file_name = args[2];
        String rule_file_name = args[3];
        double beta = Double.parseDouble(args[4]);
        String path_to_materialization_folder = args[5];
        String path_to_neo4j_database_folder = args[6] + "_" + mat_file_name.replace(".tsv", "") + "/db/";
        String path_to_processed_folder = args[7];
        String path_to_dataset_folder = args[8];
        System.out.println("Dataset name: " + dataset_name + "; Model name: " + model_name);
        String materialization_file_path = path_to_materialization_folder + "/" + dataset_name + "/" + model_name +"/" +  mat_file_name;
        String rules_file_path = path_to_dataset_folder + "/" + dataset_name + "/" +  rule_file_name;
        String output_file_path = path_to_processed_folder + "/" + dataset_name + "/" + model_name + "/" +  mat_file_name.replace(".","_processed.");
        String train_triples_path = path_to_dataset_folder + "/" + dataset_name + "/train2id.txt";
        String valid_triples_path = path_to_dataset_folder + "/" + dataset_name + "/valid2id.txt";
        String test_triples_path = path_to_dataset_folder + "/" + dataset_name + "/test2id.txt";
        RuleParser rp = new RuleParser(rules_file_path, null, model_name, dataset_name, "\t");
        rp.parse_rules_from_file(beta);

        HashMap<Integer, ArrayList<ArrayList<Integer>>> triple_dict = collect_materializations(materialization_file_path, train_triples_path, valid_triples_path, null);
//        HashMap<Integer, ArrayList<ArrayList<Integer>>> triple_dict_for_dataset = collect_materializations(null, train_triples_path, valid_triples_path, test_triples_path);
        FileWriter fileWriter = new FileWriter(output_file_path);
        BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
        HashMap<String, String> fv_for_relation = new HashMap<>();

        for(Rule this_rule: rp.rules){
            String relation = this_rule.head_atom.relationship;
            if(!fv_for_relation.containsKey(relation)){
                fv_for_relation.put(relation, this_rule.functional_variable);
            }
        }

//        int relation_total = rp.rules_by_predicate.keySet().size();
//        int relation_count = 0;
//        HashMap<Long, ArrayList<Long>> node_degrees_dict = null;
//        long n_entities = 0;
//        for(String relation: rp.rules_by_predicate.keySet()){
//            ArrayList<Atom> body = new ArrayList<>();
//            body.add(new Atom(relation, "b", "a", ""));
//            Atom head = new Atom(relation, "a", "b", "");
//
//            Rule r = new Rule(head, body, 0.1, 0.1, fv_for_relation.get(relation), beta);
//            System.out.println("\nProcessing asymmetry rule on train+test+valid " + relation_count + "/" + relation_total + ": NOT: " + r.id_print());
//            System.out.println("\tCreating and initializing database");
//            ArrayList<ArrayList<Integer>> triples_train = collect_materializations_for_rule(r, triple_dict_for_dataset);
//            create_neo4j_database(path_to_neo4j_database_folder, triples_train);
//            System.out.println("\tPre-computing number of nodes and node degrees");
//            node_degrees_dict = get_node_degrees(path_to_neo4j_database_folder);
//            n_entities = get_number_of_entities(path_to_neo4j_database_folder);
//            System.out.println("\tComputing metrics");
//            ArrayList<Double> og_metrics = query_rule_asymmetry(r, path_to_neo4j_database_folder, node_degrees_dict, n_entities);
//            r.head_coverage = og_metrics.get(0);
//            r.pca_confidence = og_metrics.get(1);
//
//            System.out.println("\nProcessing asymmetry rule on train+predictions " + relation_count + "/" + relation_total + ": NOT: " + r.id_print());
//            System.out.println("\tCreating and initializing database");
//            ArrayList<ArrayList<Integer>> triples = collect_materializations_for_rule(r, triple_dict);
//            create_neo4j_database(path_to_neo4j_database_folder, triples);
//            System.out.println("\tPre-computing number of nodes and node degrees");
//            node_degrees_dict = get_node_degrees(path_to_neo4j_database_folder);
//            n_entities = get_number_of_entities(path_to_neo4j_database_folder);
//            System.out.println("\tComputing metrics");
//            ArrayList<Double> metrics = query_rule_asymmetry(r, path_to_neo4j_database_folder, node_degrees_dict, n_entities);
//            bufferedWriter.write("NOT:" + r.id_print() + "\t" + r.head_coverage + "\t" + r.pca_confidence + "\t" + metrics.get(0) + "\t" + metrics.get(1) + "\n");
//            bufferedWriter.flush();
//            relation_count++;
//        }

        int ctr = 0;
        for(Rule this_rule: rp.rules){

            System.out.println("\nProcessing rule " + ctr + "/" + rp.rules.size() + ": " + this_rule.id_print());
            ArrayList<ArrayList<Integer>> triples = collect_materializations_for_rule(this_rule, triple_dict);
            create_neo4j_database(path_to_neo4j_database_folder, triples);
            ArrayList<Double> metrics = query_rule(this_rule, path_to_neo4j_database_folder);

            bufferedWriter.write(this_rule.id_print() + "\t" + this_rule.head_coverage + "\t" + this_rule.pca_confidence + "\t" + metrics.get(0) + "\t" + metrics.get(1) + "\n");
            bufferedWriter.flush();
            ctr++;
        }
        bufferedWriter.close();
    }
}