import math
import pickle
import sys

import networkx
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


def aggregate_score(base_rule_bucket, networkx_to_rule_mapping, triples_in_test, beta=1.0):
    """
        Compute the aggregate difference between the metrics for the base rule and the metrics for the mispredictions

        Args:
            base_rule_bucket: Bucket containing rules from base graph
            networkx_to_rule_mapping: Dict containing the mapping between networkx.DiGraph and Rule

        Returns:
            aggregator_dict: HC, PCA and Selectivity aggregated by key on input dicts
    """

    score_by_key = {}
    match_by_key = {}

    for key in base_rule_bucket:
        match_by_key[key] = False

    for key in base_rule_bucket:
        score_by_key[key] = []

    for key in base_rule_bucket:

        if len(base_rule_bucket[key]) == 0:
            score = Score(-2.0, -2.0, -2.0)
            score_by_key[key].append(score)

        for rule in base_rule_bucket[key]:
            my_rule = networkx_to_rule_mapping[rule]
            relation = my_rule.head_atom.relationship
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

    total_triples_in_test = 0

    for key in triples_in_test:
        total_triples_in_test += triples_in_test[key]

    for key in base_rule_bucket:
        agg_score = Score(0.0, 0.0, 0.0)

        for score_object in score_by_key[key]:
            agg_score.add(abs(score_object.hc), abs(score_object.pca), abs(score_object.selec))

        #agg_score.divide(total_triples_in_test, total_triples_in_test, total_triples_in_test)
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


def write_aggregated_score(model_name, dataset_name, folder_to_results, agg_score, bucket, networkx_to_rule_mapping, k,
                           additional, relations=None):
    """
    Write the aggregated scores of a given set of rules to a file.

    Args:
        model_name (str): The name of the model.
        dataset_name (str): The name of the dataset.
        folder_to_results (str): The path of the folder where the results file will be written.
        agg_score (dict): A dictionary mapping relation type to aggregated scores.
        bucket (dict): A dictionary mapping relation type to a tuple of networkx node and rule id.
        networkx_to_rule_mapping (dict): A dictionary mapping networkx node to the corresponding rule object.
        k (int): The number of candidate rules per relation type.
        additional (str): Additional information to include in the results file name.
        relations (List[str], optional): A list of relation types to be written. Defaults to None.

    Returns:
        None
    """

    results_file_name = f"{folder_to_results}/{dataset_name}/{model_name}/{dataset_name}_{model_name}_k_{k}_aggregated_{additional}.csv"
    keys = bucket.keys() if relations is None else relations

    with open(results_file_name, "w+") as file_obj:
        file_obj.write("Type,Rule,HC,PCA,Selec\n")
        for key in keys:
            if key not in bucket:
                hc, pca, selec = Score(-2.0, -2.0, -2.0).get()
                file_obj.write(f"{key},NULL,{hc},{pca},{selec}\n")
            else:
                rule = networkx_to_rule_mapping[bucket[key][0]]
                hc, pca, selec = agg_score[key].get()

                file_obj.write(f"{key},{rule.id_print()},{hc},{pca},{selec}\n")


def get_relation_list(folder_to_dataset, dataset_name):
    f = open(f"{folder_to_dataset}/{dataset_name}/relation2id.txt")
    n_relations = int(f.readline())

    relations = [i for i in range(0, n_relations)]

    f.close()
    return relations


def get_rules_with_unique_atoms(base_graphs, networkx_to_rule_mapping):
    """
    Find rules with unique atoms in a list of base graphs.

    Args:
        base_graphs (List[networkx.Graph]): A list of base graphs.
        networkx_to_rule_mapping (Dict[networkx.Graph, Rule]): A dictionary mapping a base graph to its corresponding rule object.

    Returns:
        Tuple[List[networkx.Graph], List[networkx.Graph]]: A tuple of two lists containing base graphs:
            - The first list contains base graphs whose corresponding rules have unique atoms.
            - The second list contains base graphs whose corresponding rules do not have unique atoms.

    """

    graphs_with_unique_atoms = []
    graphs_without_unique_atoms = []

    for graph in base_graphs:
        relation_set = set()
        flag = True
        rule = networkx_to_rule_mapping[graph]
        atoms = rule.body_atoms.copy()
        atoms.append(rule.head_atom)
        for atom in atoms:
            relationship = atom.relationship

            if relationship not in relation_set:
                relation_set.add(relationship)
            else:
                flag = False
                break

        if len(relation_set) == len(rule.body_atoms) + 1:
            flag = True

        if flag:
            graphs_with_unique_atoms.append(graph)
        else:
            graphs_without_unique_atoms.append(graph)

    return graphs_with_unique_atoms, graphs_without_unique_atoms


def get_types_of_rules(base_graphs, networkx_mapping):
    symmetry = []
    hierarchy = []
    inversion = []
    intersection = []

    composition_path = []
    composition_non_path = []

    for graph in base_graphs:
        rule = networkx_mapping[graph]

        if len(rule.body_atoms) == 1:
            if rule.body_atoms[0].relationship == rule.head_atom.relationship:
                atom1 = rule.body_atoms[0]
                atom2 = rule.head_atom

                if atom1.variable1 == atom2.variable2 and atom1.variable2 == atom2.variable1:
                    symmetry.append(graph)
            else:
                atom1 = rule.body_atoms[0]
                atom2 = rule.head_atom

                if atom1.variable1 == atom2.variable2 and atom1.variable2 == atom2.variable1:
                    inversion.append(graph)
                else:
                    hierarchy.append(graph)

        else:
            atom1, atom2 = rule.body_atoms
            atom3 = rule.head_atom

            if atom1.relationship != atom2.relationship != atom3.relationship:
                if atom1.variable1 == atom2.variable1 == atom3.variable1 and atom1.variable2 == atom2.variable2 == atom3.variable2:
                    intersection.append(graph)
                else:
                    if atom1.variable2 == atom2.variable1:
                        composition_path.append(graph)
                    else:
                        composition_non_path.append(graph)

    # if len(symmetry) > 0:
    #     print(f"Symmetry present, example: {symmetry[0].id_print()}")
    #
    # if len(inversion) > 0:
    #     print(f"Inversion present, example: {inversion[0].id_print()}")
    #
    # if len(hierarchy) > 0:
    #     print(f"Hierarchy present, example: {hierarchy[0].id_print()}")
    #
    # if len(composition) > 0:
    #     print(f"Composition present, example: {composition[0].id_print()}")
    #
    # if len(intersection) > 0:
    #     print(f"Intersection present, example: {intersection[0].id_print()}")

    return {"Symmetry": symmetry, "Inversion": inversion, "Hierarchy": hierarchy, "Composition_Path": composition_path,
            "Composition_Non_Path": composition_non_path, "Intersection": intersection}


def filter_rules_by_head_in_test(rules, folder_to_datasets, dataset_name):
    heads_in_test = []

    with open(f"{folder_to_datasets}/{dataset_name}/test2id.txt") as f:
        f.readline()

        for line in f:
            line = line.strip()

            splits = line.split("\t")

            if len(splits) < 2:
                splits = line.split(" ")

            relation = int(splits[2])

            if relation not in heads_in_test:
                heads_in_test.append(relation)

    filtered_rules = []
    for rule in rules:
        if rule.head_atom.relationship in heads_in_test:
            filtered_rules.append(rule)

    return filtered_rules


def compile_results_single(model_name, dataset_name, folder_to_results, folder_to_processed_rules, folder_to_datasets,
                           k, beta=1.0):
    global universal_node_ids

    relations = get_relation_list(folder_to_datasets, dataset_name)
    processed_rules_path = f"{folder_to_processed_rules}/{dataset_name}/{model_name}/{dataset_name}_{model_name}_k_{k}_processed.tsv"

    rules = extract_metrics(processed_rules_path)

    universal_node_ids = get_universal_node_id_mapping([rules])
    base_graphs, base_nwx_mapping = convert_rules_to_networkx_graphs(rules)
    networkx_to_rule_mapping = {**base_nwx_mapping}

    # base_bucket_type = create_bucket_by_type(base_graphs)
    #
    # agg_score_by_type = aggregate_score(base_rule_bucket=base_bucket_type,
    #                                     networkx_to_rule_mapping=networkx_to_rule_mapping,
    #                                     beta=beta)
    #
    # write_aggregated_score(folder_to_results=folder_to_results, agg_score=agg_score_by_type, bucket=base_bucket_type,
    #                        networkx_to_rule_mapping=networkx_to_rule_mapping, model_name=model_name,
    #                        dataset_name=dataset_name, k=k
    #                        , additional="type")
    #
    # base_bucket_head = create_bucket_by_head(bucket=base_bucket_type, networkx_to_rule_mapping=networkx_to_rule_mapping)
    #
    # agg_score_by_head = aggregate_score(base_rule_bucket=base_bucket_head,
    #                                     networkx_to_rule_mapping=networkx_to_rule_mapping,
    #                                     beta=beta)
    #
    # write_aggregated_score(dataset_name=dataset_name, model_name=model_name,
    #                        folder_to_results=folder_to_results, agg_score=agg_score_by_head, bucket=base_bucket_head,
    #                        networkx_to_rule_mapping=networkx_to_rule_mapping,
    #                        relations=relations, k=k, additional="head")

    graphs_with_unique_atoms, graphs_with_repeating_atoms = get_rules_with_unique_atoms(base_graphs,
                                                                                        networkx_to_rule_mapping)

    unique_bucket_by_type = create_bucket_by_type(graphs_with_unique_atoms)
    repeating_bucket_by_type = create_bucket_by_type(graphs_with_repeating_atoms)
    unique_bucket_by_head = create_bucket_by_head(unique_bucket_by_type, networkx_to_rule_mapping)
    repeating_bucket_by_head = create_bucket_by_head(repeating_bucket_by_type, networkx_to_rule_mapping)

    agg_scores_unique_type = aggregate_score(base_rule_bucket=unique_bucket_by_type,
                                             networkx_to_rule_mapping=networkx_to_rule_mapping, beta=1)
    agg_scores_repeating_type = aggregate_score(base_rule_bucket=repeating_bucket_by_type,
                                                networkx_to_rule_mapping=networkx_to_rule_mapping, beta=1)
    agg_scores_unique_head = aggregate_score(base_rule_bucket=unique_bucket_by_head,
                                             networkx_to_rule_mapping=networkx_to_rule_mapping, beta=1)
    agg_scores_repeating_head = aggregate_score(base_rule_bucket=repeating_bucket_by_head,
                                                networkx_to_rule_mapping=networkx_to_rule_mapping, beta=1)

    write_aggregated_score(dataset_name=dataset_name, model_name=model_name,
                           folder_to_results=folder_to_results, agg_score=agg_scores_unique_type,
                           bucket=unique_bucket_by_type,
                           networkx_to_rule_mapping=networkx_to_rule_mapping,
                           k=k, additional="unique_type")

    write_aggregated_score(dataset_name=dataset_name, model_name=model_name,
                           folder_to_results=folder_to_results, agg_score=agg_scores_repeating_type,
                           bucket=repeating_bucket_by_type,
                           networkx_to_rule_mapping=networkx_to_rule_mapping,
                           k=k, additional="repeating_type")

    write_aggregated_score(dataset_name=dataset_name, model_name=model_name,
                           folder_to_results=folder_to_results, agg_score=agg_scores_unique_head,
                           bucket=unique_bucket_by_head,
                           networkx_to_rule_mapping=networkx_to_rule_mapping,
                           relations=relations, k=k, additional="unique_head")

    write_aggregated_score(dataset_name=dataset_name, model_name=model_name,
                           folder_to_results=folder_to_results, agg_score=agg_scores_repeating_head,
                           bucket=repeating_bucket_by_head,
                           networkx_to_rule_mapping=networkx_to_rule_mapping,
                           relations=relations, k=k, additional="repeating_head")


def get_correct_predictions(dataset_name, model_name, folder_to_datasets, folder_to_materializations):
    triple_dict = pickle.load(
        open(f"{folder_to_materializations}/{dataset_name}/{model_name}/{dataset_name}_{model_name}_top_100.pickle",
             "rb"))

    triples_in_mat = []
    for key, value in triple_dict.items():
        # print(key, value)
        if value <= k:
            triples_in_mat.append((int(key[0]), int(key[1]), int(key[2])))

    triples_in_test = []

    with open(f"{folder_to_datasets}/{dataset_name}/test2id.txt") as f:
        f.readline()

        for line in f:
            line = line.strip()

            splits = line.split("\t")

            if len(splits) < 2:
                splits = line.split(" ")

            triples_in_test.append((int(splits[0]), int(splits[2]), int(splits[1])))

    # print(triples_in_test)
    # print("----")
    # print(triples_in_mat)
    # print("----")
    ctr = 0
    for triple in triples_in_test:
        if triple in triples_in_mat:
            ctr += 1

    print(
        f"Dataset: {dataset_name}, Model: {model_name}, Number of predictions: {len(triples_in_test)}, Accurate prediction percentage: {ctr * 1.0 / len(triples_in_test)}, Number of test triples missing: {len(triples_in_test)-ctr}")


def get_triples_in_test_by_relation(folder_to_datasets, dataset_name):

    triples_in_test = {}
    with open(f"{folder_to_datasets}/{dataset_name}/test2id.txt") as f:
        f.readline()

        for line in f:
            line = line.strip()

            splits = line.split("\t")

            if len(splits) < 2:
                splits = line.split(" ")

            if int(splits[2]) not in triples_in_test:
                triples_in_test[int(splits[2])] = []

            triples_in_test[int(splits[2])].append((int(splits[0]), int(splits[2]), int(splits[1])))

    triples = {}

    for key in triples_in_test:
        triples[key] = len(triples_in_test[key])
    return triples

def compile_results_with_specified_types(model_name, dataset_name, folder_to_results, folder_to_processed_rules,
                                         folder_to_datasets, folder_to_materializations,
                                         k, beta=1.0):
    global universal_node_ids

    relations = get_relation_list(folder_to_datasets, dataset_name)
    processed_rules_path = f"{folder_to_processed_rules}/{dataset_name}/{model_name}/{dataset_name}_{model_name}_k_{k}_processed.tsv"

    triples_in_test = get_triples_in_test_by_relation(folder_to_datasets, dataset_name)
    get_correct_predictions(dataset_name, model_name, folder_to_datasets, folder_to_materializations)
    all_rules = extract_metrics(processed_rules_path)

    rules = filter_rules_by_head_in_test(all_rules, folder_to_datasets, dataset_name)

    universal_node_ids = get_universal_node_id_mapping([rules])
    base_graphs, base_nwx_mapping = convert_rules_to_networkx_graphs(rules)
    networkx_to_rule_mapping = {**base_nwx_mapping}

    buckets_by_specified_types = get_types_of_rules(base_graphs, networkx_to_rule_mapping)

    agg_score = aggregate_score(buckets_by_specified_types, networkx_to_rule_mapping, triples_in_test, beta)

    for key in agg_score:
        print(
            f"{key}: HC: {round(agg_score[key].hc, 3)}, PCA: {round(agg_score[key].pca, 3)}, Selectivity: {round(agg_score[key].selec, 3)}")


if __name__ == "__main__":
    # model_name = sys.argv[1]
    # dataset_name = sys.argv[2]
    # folder_to_write = sys.argv[3]
    # folder_to_processed_rules = sys.argv[4]
    # folder_to_datasets = sys.argv[5]
    # k = int(sys.argv[6])

    model_name = "tucker"
    dataset_name = "WN18"
    folder_to_write = "D:/PhD/Work/UnderstandingLP/data/AggregatedRules"
    folder_to_processed_rules = "D:/PhD/Work/UnderstandingLP/data/ProcessedRules"
    folder_to_datasets = "D:/PhD/Work/UnderstandingLP/data/Datasets"
    folder_to_materializations = "D:/PhD/Work/UnderstandingLP/data/Materializations"
    k = 1

    models = ["boxe", "complex", "hake", "hole", "quate", "rotate", "rotpro", "toruse", "transe", "tucker"]
    datasets = ["WN18"]

    for dataset_name in datasets:
        for model_name in models:
            print(f"\nDataset: {dataset_name}, Model: {model_name}")
            # compile_results_single(model_name=model_name, dataset_name=dataset_name, folder_to_results=folder_to_write,
            #                        folder_to_processed_rules=folder_to_processed_rules, k=k,
            #                        folder_to_datasets=folder_to_datasets)
            compile_results_with_specified_types(model_name=model_name, dataset_name=dataset_name,
                                                 folder_to_results=folder_to_write,
                                                 folder_to_processed_rules=folder_to_processed_rules, k=k,
                                                 folder_to_datasets=folder_to_datasets, folder_to_materializations=folder_to_materializations)

            print(
                "=====================================================================================================")
