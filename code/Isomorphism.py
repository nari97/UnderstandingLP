import networkx
import subprocess
from ExtractMetrics import extract_metrics
from Score import Score

universal_node_ids = None


def get_bucket(rules):
    global universal_node_ids

    rules_base = rules

    universal_node_ids = get_universal_node_id_mapping([rules_base])
    base_graphs, base_nwx_mapping = convert_rules_to_networkx_graphs(rules_base)
    networkx_to_rule_mapping = {**base_nwx_mapping}

    base_bucket_type = create_bucket_by_type(base_graphs)

    return base_bucket_type, networkx_to_rule_mapping


def get_universal_node_id_mapping(rules_list):
    """
        Takes input of list of list of rules (Multiple Rules) and maps variables found to IDs

        Args:
            rules_list (List[List[Rule]]): Multiple outputs from ParseRules concatenated into a list

        Returns:
            inner_universal_node_ids (dict): Mapping of variables found in rules to their corresponding ID numbers
    """
    inner_universal_node_ids = {}
    for current_rule_list in rules_list:
        for rule in current_rule_list:
            variables = rule.get_variables()

            for variable in variables:
                if variable not in inner_universal_node_ids:
                    inner_universal_node_ids[variable] = len(inner_universal_node_ids)

    return inner_universal_node_ids


def get_networkx_representation(rule):
    """
        Convert given Rule object into its NetworkX Graph implementation

        Args:
            rule (Rule): The given input rule object

        Returns:
            G (networkx.DiGraph): The converted graph

    """
    global universal_node_ids
    variables = rule.get_variables()
    G = networkx.DiGraph()

    for variable in variables:
        G.add_node(variable, id=universal_node_ids[variable])

    for atom in rule.body_atoms:
        G.add_edge(atom.variable1.replace("?", ""), atom.variable2.replace("?", ""), r=atom.relationship)

    return G


def convert_rules_to_networkx_graphs(rules):
    """
        Convert list of Rules to list of networkx.DiGraphs

        Args:
            rules: List of Rule

        Returns:
            graphs: List of Networkx.DiGraph
            networkx_to_rule_mapping: Mapping of networkx.DiGraph to Rule
    """

    networkx_to_rule_mapping = {}
    graphs = []

    for rule in rules:
        nwx_graph = get_networkx_representation(rule)
        networkx_to_rule_mapping[nwx_graph] = rule
        graphs.append(nwx_graph)

    return graphs, networkx_to_rule_mapping


def create_bucket_by_type(networkx_rules):
    """
        Add rules to buckets based on the type of rule using networkx.is_isomorphic

        Args:
            networkx_rules: List of networkX.DiGraphs
            networkx_to_rule_mapping: Dict containing the mapping from networkx.DiGraph back to Rule

        Returns:
            bucket: Rules categorized by type
    """
    global universal_node_ids
    bucket = {}

    for current_graph in networkx_rules:

        current_graph_found_flag = False
        for key in bucket:
            bucket_graph = bucket[key][0]
            if networkx.is_isomorphic(bucket_graph, current_graph, node_match=node_match):
                bucket[key].append(current_graph)
                current_graph_found_flag = True
                break

        if not current_graph_found_flag:
            bucket[len(bucket)] = [current_graph]

    return bucket


def node_match(node1, node2):
    """
        Function for matching two nodes for isomorphism

        Args:
            node1: 1st node
            node2: 2nd node

        Returns:
            boolean: If the two nodes were a match or not
    """

    global universal_node_ids
    # if "id" not in node1 or "id" not in node2:
    #     return True
    if node1["id"] == universal_node_ids["a"]:
        if node2["id"] == universal_node_ids["a"]:
            return True
        else:
            return False

    elif node1["id"] == universal_node_ids["b"]:
        if node2["id"] == universal_node_ids["b"]:
            return True
        else:
            return False

    else:
        if node2["id"] != universal_node_ids["a"] and node2["id"] != universal_node_ids["b"]:
            return True
        else:
            return False


def compute_selectivity(head_coverage, pca_confidence, beta=1.0):
    """
    Compute selectivity based on head coverage and PCA confidence.

    Parameters:
    head_coverage (float): Head coverage value.
    pca_confidence (float): PCA confidence value.
    beta (float, optional): The beta value for selectivity calculation. Default is 1.0.

    Returns:
    float: Selectivity value.

    """
    selectivity = ((1 + beta * beta) * pca_confidence * head_coverage) / (
            beta * beta * pca_confidence + head_coverage)
    return selectivity


def aggregate_score(base_rule_bucket, networkx_to_rule_mapping, beta=1.0):
    """
        Compute the aggregate difference between the metrics for the base rule and the metrics for the mispredictions

        Args:
            base_rule_bucket: Bucket containing rules from base graph
            networkx_to_rule_mapping: Dict containing the mapping between networkx.DiGraph and Rule

        Returns:
            aggregator_dict: HC, PCA and Selectivity aggregated by key on input dicts
    """

    score_by_key = {}
    rule_matches = []
    match_by_key = {}

    for key in base_rule_bucket:
        match_by_key[key] = False

    for key in base_rule_bucket:
        score_by_key[key] = []

    for key in base_rule_bucket:
        print("\nKey: ", key)
        print("\n============================")
        for rule in base_rule_bucket[key]:
            my_rule = networkx_to_rule_mapping[rule]
            print(my_rule.id_print())
            hc_base = my_rule.head_coverage
            hc_predictions = my_rule.new_head_coverage
            pca_base = my_rule.pca_confidence
            pca_predictions = my_rule.new_pca_confidence
            selec_base = compute_selectivity(hc_base, pca_base, beta)
            selec_predictions = compute_selectivity(hc_predictions, pca_predictions, beta)
            hc_score = hc_predictions - hc_base
            pca_score = pca_predictions - pca_base
            selec_score = selec_predictions - selec_base

            score = Score(hc_score, pca_score, selec_score)

            score_by_key[key].append(score)

    aggregator_dict = {}

    for key in base_rule_bucket:
        agg_score = Score(0.0, 0.0, 0.0)

        for score_object in score_by_key[key]:
            agg_score.add(score_object.hc, score_object.pca, score_object.selec)

        agg_score.divide(len(score_by_key[key]), len(score_by_key[key]), len(score_by_key[key]))
        aggregator_dict[key] = agg_score

    return aggregator_dict


def create_bucket_by_head(bucket, networkx_to_rule_mapping):
    """
        Break bucket by head predicate

        Args:
            bucket: Dict containing the rules by key
            networkx_to_rule_mapping: Dict containing the mapping between networkx.DiGraph and Rule

        Returns:
            head_buckets: Dict containing head predicate as keys, and rules as values
    """

    head_buckets = {}

    for key in bucket:
        for rule in bucket[key]:
            my_rule = networkx_to_rule_mapping[rule]
            my_rule_head = my_rule.head_atom.relationship

            if my_rule_head not in head_buckets:
                head_buckets[my_rule_head] = []

            head_buckets[my_rule_head].append(rule)

    return head_buckets


def write_aggregated_score(folder_to_write, agg_score, bucket, networkx_to_rule_mapping, results_file_name,
                           relations=None):
    """
    Write the aggregated scores of a given set of relations to a file.

    Args:
    - folder_to_write (str): The path of the folder where the results file will be written.
    - agg_score (dict): A dictionary mapping relation type to aggregated scores.
    - bucket (dict): A dictionary mapping relation type to a tuple of networkx node and rule id.
    - networkx_to_rule_mapping (dict): A dictionary mapping networkx node to the corresponding rule object.
    - results_file_name (str): The name of the results file to be written.
    - relations (List[str], optional): A list of relation types to be written. Defaults to None.
    """

    results_file_name = f"{folder_to_write}\\{results_file_name}"
    keys = bucket.keys() if relations is None else relations

    with open(results_file_name, "w+") as file_obj:
        file_obj.write("Type,Rule,HC,PCA,Selec\n")
        for key in keys:

            if key not in bucket:
                hc, pca, selec = Score(-2.0, -2.0, -2.0).get()
            else:
                rule = networkx_to_rule_mapping[bucket[key][0]]
                hc, pca, selec = agg_score[key].get()
            file_obj.write(f"{key},{rule.id_print()},{hc},{pca},{selec}\n")


def compile_results_single(model_name, dataset_name, results_file_name, processed_file_name,
                           folder_to_write, folder_to_processed_rules, beta=1.0, relations=None):
    """
    Compile and aggregate the results for a single model-dataset combination.

    Args:
    - model_name (str): The name of the model.
    - dataset_name (str): The name of the dataset.
    - results_file_name (str): The name of the results file to be written.
    - processed_file_name (str): The name of the processed rules file.
    - folder_to_write (str): The path of the folder where the results file will be written.
    - folder_to_processed_rules (str): The path of the folder where the processed rules are stored.
    - beta (float, optional): A weight parameter used in score aggregation. Defaults to 1.0.
    - relations (int, optional): The number of relation types to be written. Defaults to None.
    """

    global universal_node_ids

    processed_rules_path = f"{folder_to_processed_rules}{dataset_name}\\{processed_file_name}"

    rules = extract_metrics(processed_rules_path)

    universal_node_ids = get_universal_node_id_mapping([rules])
    base_graphs, base_nwx_mapping = convert_rules_to_networkx_graphs(rules)
    networkx_to_rule_mapping = {**base_nwx_mapping}

    base_bucket_type = create_bucket_by_type(base_graphs)

    agg_score_by_type= aggregate_score(base_rule_bucket=base_bucket_type,
                                                            networkx_to_rule_mapping=networkx_to_rule_mapping,
                                                            beta=beta)
    write_aggregated_score(folder_to_write=folder_to_write, agg_score=agg_score_by_type, bucket=base_bucket_type,
                           networkx_to_rule_mapping=networkx_to_rule_mapping, results_file_name=results_file_name)

    # base_bucket_head = create_bucket_by_head(bucket=base_bucket_type, networkx_to_rule_mapping=networkx_to_rule_mapping)
    # augment_bucket_head = create_bucket_by_head(bucket=augment_bucket_type,
    #                                             networkx_to_rule_mapping=networkx_to_rule_mapping)
    # agg_score_by_head, rule_match_by_head = aggregate_score(base_rule_bucket=base_bucket_head,
    #                                                         augment_rule_bucket=augment_bucket_head,
    #                                                         networkx_to_rule_mapping=networkx_to_rule_mapping)
    # write_aggregated_score(dataset_name=dataset_name, model_name=model_name, mat_type=mat_type,
    #                        folder_to_write=folder_to_write, agg_score=agg_score_by_head, bucket=base_bucket_head,
    #                        networkx_to_rule_mapping=networkx_to_rule_mapping, additional=additional,
    #                        relations=relations)
    # write_rule_matches(dataset_name=dataset_name, model_name=model_name, mat_type=mat_type,
    #                    folder_to_write=folder_to_write, rule_matches=rule_match_by_head, additional=additional)


def process_rules(model_name, dataset_name, mat_file_name, rules_file_name, processed_rule_folder_path):
    java_class_path = r"D:\PhD\Work\UnderstandingLP\Graph_JAVA\Graph_JAVA\build\install\Graph_JAVA\bin\Graph_JAVA.bat"

    print("Starting post processing of rules")
    process = subprocess.Popen([java_class_path, dataset_name, model_name, mat_file_name, rules_file_name, "1.0"],
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = process.communicate()
    print("the commandline is {}".format(process.args))
    print(stdout.decode())
    print(stderr.decode())
    print("Finished processing of rules")

    processed_rule_file_name = f"{processed_rule_folder_path}\\{dataset_name}\\{mat_file_name}"
    rules = extract_metrics(processed_rule_file_name)

    return rules


if __name__ == "__main__":
    model_name = "TransE"
    dataset_name = "WN18"
    results_file_name = "WN18_TransE_type_aggregated.tsv"
    processed_file_name = "TransE_materialized.tsv"
    folder_to_write = f"D:\\PhD\\Work\\UnderstandingLP\\data\\Results\\{dataset_name}\\{model_name}"
    folder_to_processed_rules = f"D:\\PhD\\Work\\UnderstandingLP\\data\\ProcessedRules\\"
    compile_results_single(model_name, dataset_name, results_file_name, processed_file_name, folder_to_write,
                           folder_to_processed_rules)
