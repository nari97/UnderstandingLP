package Graph_JAVA;

import com.google.common.io.MoreFiles;
import com.google.common.io.RecursiveDeleteOption;
import org.apache.shiro.crypto.hash.Hash;
import org.neo4j.batchinsert.BatchInserter;
import org.neo4j.batchinsert.BatchInserters;
import org.neo4j.batchinsert.internal.BatchRelationship;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.cypher.internal.expressions.In;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.graphdb.*;
import org.neo4j.io.layout.DatabaseLayout;
import scala.Int;

import java.io.*;
import java.lang.reflect.Array;
import java.util.*;

class BuildTestGraph{

    public static void build_graph(ArrayList<ArrayList<Integer>> triples, String database_folder_path) throws IOException {


        File neo4j_folder = new File(database_folder_path + "/db/");
        if (neo4j_folder.exists())
            MoreFiles.deleteRecursively(neo4j_folder.toPath(), RecursiveDeleteOption.ALLOW_INSECURE);
        BatchInserter inserter = BatchInserters.inserter(DatabaseLayout.of(
                Config.newBuilder().set(GraphDatabaseSettings.neo4j_home, neo4j_folder.toPath()).build()));

        for(ArrayList<Integer> triple: triples){

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

    public static long get_pca_denominator(Transaction tx, Rule r){

        String fv = r.functional_variable;
        String nfv = r.functional_variable.equals("a")?"b":"a";
        String relation = r.body_atoms.get(0).relationship;
        HashMap<Long, Long> hm = new HashMap<>();
        long n_entities = 0;
        String query = "MATCH (a) RETURN count(a) as cnt";
        try{
            Result res = tx.execute(query);
            while(res.hasNext()){
                Map<String, Object> row = res.next();
                n_entities = (long) row.get("cnt");
            }
        } catch (Exception e){
            e.printStackTrace();
        }

        if (nfv.equals("a"))
            query = "MATCH (a) RETURN id(a) as var, size(()-[:`" + relation +"`]->(a)) as cnt";
        else
            query = "MATCH (a) RETURN id(a) as var, size((a)-[:`" + relation +"`]->()) as cnt";
//        System.out.println(query);
        try{
            Result res = tx.execute(query);

            while(res.hasNext()){
                Map<String, Object> row = res.next();
                long var = (long) row.get("var");
                long count = (long) row.get("cnt");
                hm.put(var, count);
            }
            res.close();
        } catch (Exception e){
            e.printStackTrace();
        }
        System.out.println(hm);
        query = "MATCH " + r.body_atoms.get(0).neo4j_print() + " RETURN id(a) as a, id(b) as b";
//        System.out.println(query);
        long pca_denom = 0;
        try{
            Result res = tx.execute(query);

            while(res.hasNext()){
                Map<String, Object> row = res.next();

                long functionalVar = (long) row.get(fv);
                long nonFunctionalVar = (long) row.get(nfv);
//                System.out.println(functionalVar + " " + nonFunctionalVar);
                if (hm.containsKey(functionalVar) && (n_entities - hm.get(functionalVar) > 0)){
                    ++pca_denom;
                }
            }
        } catch (Exception e){
            e.printStackTrace();
        }

        return pca_denom;
    }

    public static void print_metrics(String path_to_database, String path_to_rules, int relation, String functional_variable) throws IOException {

        RuleParser rp = new RuleParser(path_to_rules, null, "", "", "\t");
        rp.parse_rules_from_file(1.0);

        ArrayList<Atom> body = new ArrayList<>();
        body.add(new Atom("" + relation, "b", "a", "0"));
        Rule r = new Rule(new Atom("" + (relation+1), "?a", "?b", "1"), body, 0.2, 0.2, functional_variable, 1.0);



        ArrayList<Double> metrics = RuleQuery.query_rule(r, path_to_database);
        System.out.println("Rule: " + r.id_print() + " Metrics: " + metrics);

        File neo4j_folder = new File(path_to_database + "/db/");
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
            Result res = tx.execute("MATCH " + r.body_atoms.get(0).neo4j_print() + " WHERE NOT EXISTS(" + r.head_atom.neo4j_print().replace("" + (relation+1), "" + relation) + ") WITH DISTINCT a, b RETURN COUNT(*) as cnt");
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
            Result res = tx.execute("MATCH (a) MATCH (b) WHERE NOT EXISTS(" + r.head_atom.neo4j_print().replace("" + (relation+1), "" + relation) + ") WITH DISTINCT a, b RETURN count(*) as cnt");
            while (res.hasNext()) {
                Map<String, Object> row = res.next();
//                System.out.println(row);
                heads = (long) row.get("cnt");
            }
            res.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        long pca = get_pca_denominator(tx, r);

        System.out.println("Computed support: " + support);
        System.out.println("Computed tHeads: " + heads);
        System.out.println("Computed PCA: " + pca);

        System.out.println("Head coverage: " + (support * 1.0 / heads));
        System.out.println("PCA confidence: " + (support * 1.0 / pca));
        service.shutdown();
    }


    public static ArrayList<ArrayList<Integer>> get_triples_with_negatives(String path_to_triples, String path_to_negatives, int relation_of_interest) throws IOException {
        Scanner sc = new Scanner(new File(path_to_triples));
        BufferedWriter bf = new BufferedWriter(new FileWriter(path_to_negatives));

        HashMap<Integer, HashMap<Integer, ArrayList<Integer>>> triples_by_relation = new HashMap<>();
        Random rand = new Random();

        sc.nextLine();
        ArrayList<Integer> nodes = new ArrayList<>();
        ArrayList<Integer> relationships = new ArrayList<>();
        ArrayList<ArrayList<Integer>> triples = new ArrayList<>();

        while (sc.hasNext()) {
            String line = sc.nextLine();

            String[] splits = line.strip().split("\t");

            int s = Integer.parseInt(splits[0]);
            int p = Integer.parseInt(splits[1]);
            int o = Integer.parseInt(splits[2]);

            double randomNum = rand.nextDouble();
            if (p != relation_of_interest || randomNum<=0.5){
                continue;
            }

            ArrayList<Integer> temp_list1 = new ArrayList<>();
            temp_list1.add(s);
            temp_list1.add(p);
            temp_list1.add(o);
            triples.add(temp_list1);

            if (!nodes.contains(s))
                nodes.add(s);

            if (!nodes.contains(o))
                nodes.add(o);

            if (!relationships.contains(p))
                relationships.add(p);

            if (!triples_by_relation.containsKey(p)) {
                HashMap<Integer, ArrayList<Integer>> temp_map = new HashMap<>();
                ArrayList<Integer> temp_list = new ArrayList<>();

                temp_list.add(o);
                temp_map.put(s, temp_list);
                triples_by_relation.put(p, temp_map);
            } else {
                if (!triples_by_relation.get(p).containsKey(s)) {
                    ArrayList<Integer> temp_list = new ArrayList<>();
                    temp_list.add(o);
                    triples_by_relation.get(p).put(s, temp_list);
                } else {
                    triples_by_relation.get(p).get(s).add(o);
                }
            }
        }

        System.out.println("Including positives: " + triples.size());
        for (int r: relationships){
            triples_by_relation.put(r+1, new HashMap<>());
            HashMap<Integer, ArrayList<Integer>> temp_map = triples_by_relation.get(r);
            for (int key: nodes){
//                System.out.println(key);
                for (int e: nodes){
                    if (!temp_map.containsKey(key) || !temp_map.get(key).contains(e)){
                        ArrayList<Integer> temp_list = new ArrayList<>();
                        temp_list.add(key);
                        temp_list.add(r+ relationships.size());
                        temp_list.add(e);
                        triples.add(temp_list);
                    }
                }
            }
        }

        System.out.println("Including negatives: " + triples.size());
        for (ArrayList<Integer> triple: triples){
            bf.write(triple.get(0) + "\t" + triple.get(1) + "\t" + triple.get(2) + "\n");
            bf.flush();
        }

        bf.close();
        return triples;
    }

    public static void main(String[] args) throws IOException {

        String path_to_triples = "D:\\PhD\\Work\\UnderstandingLP\\data\\Materializations\\WN18\\ComplEx_materialized.tsv";
        String path_to_negatives = "D:/PhD/Work/UnderstandingLP/Graph_JAVA/Graph_JAVA/src/main/java/Graph_JAVA/files/asymmetry-test-graph-negatives.tsv";
        String path_to_rules = "D:/PhD/Work/UnderstandingLP/Graph_JAVA/Graph_JAVA/src/main/java/Graph_JAVA/files/asymmetry-test-graph-rules.tsv";
        String path_to_database = "D:/PhD/Work/UnderstandingLP/Graph_JAVA/Graph_JAVA/db/test";
        String path_to_amie = "D:/PhD/Work/UnderstandingLP/amie-dev.jar";

        int relation = 0;
        build_graph(get_triples_with_negatives(path_to_triples, path_to_negatives, relation), path_to_database);


        System.out.println("For functional variable a");
        print_metrics(path_to_database, path_to_rules, relation, "a");
        System.out.println("For functional variable b");
        print_metrics(path_to_database, path_to_rules, relation, "b");

    }
}