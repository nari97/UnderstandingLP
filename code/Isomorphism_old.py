import math

import networkx

from ParseRules import ParseRule, Rule, Atom
from Score import Score

universal_node_ids = None


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


def edge_match(edge1, edge2):
    """
        Function for matching two edges for isomorphism

        Args:
            node1: 1st edge
            node2: 2nd edge

        Returns:
            boolean: If the two edges were a match or not
    """
    return edge1["r"] == edge2["r"]


def write_rule_matches(dataset_name, model_name, mat_type, folder_to_write, rule_matches, additional=""):
    """
        Write all matching rules to a file

        Args:
            dataset_name: Name of the dataset
            model_name: Name of the link prediction model
            mat_type: Materialization type
            folder_to_write: Path to folder for writing results
            rule_matches: List of List, where inner List contains two elements. Index 0 is the base rule and Index 1 is the augmented rule
            additional: Additional text for filename
    """

    rule_match_filename = f"{folder_to_write}{dataset_name.lower()}_{model_name.lower()}_{mat_type.lower()}"

    if additional != "":
        rule_match_filename += f"_{additional}"

    rule_match_filename += "_rule_matches.csv"

    with open(rule_match_filename, "w+") as file_obj:
        file_obj.write(
            "Base rule,Base rule HC,Base rule PCA,Base rule Selec,Augmented rule,Augmented rule HC,Augmented Rule PCA,Augmented Rule Selec\n")

        for rule_base, rule_augment in rule_matches:
            string = ""
            string += rule_base.id_print() + ","
            string += str(rule_base.head_coverage) + ","
            string += str(rule_base.pca_confidence) + ","
            string += str(rule_base.selectivity) + ","
            string += str(rule_augment.id_print()) + ","
            string += str(rule_augment.head_coverage) + ","
            string += str(rule_augment.pca_confidence) + ","
            string += str(rule_augment.selectivity) + "\n"

            file_obj.write(string)


def aggregate_score(base_rule_bucket, augment_rule_bucket, networkx_to_rule_mapping):
    """
        Compute the aggregate difference between base_rule_bucket and augment_rule_bucket

        Args:
            base_rule_bucket: Bucket containing rules from base graph
            augment_rule_bucket: Bucket containing rules from augmented graph
            networkx_to_rule_mapping: Dict containing the mapping between networkx.DiGraph and Rule

        Returns:
            aggregator_dict: HC, PCA and Selectivity aggregated by key on input dicts
    """

    score_by_key = {}
    rule_matches = []
    match_by_key = {}

    for key in base_rule_bucket:
        match_by_key[key] = False

    augment_indexed_by_head = {}

    for key in augment_rule_bucket:
        for i in range(len(augment_rule_bucket[key])):
            my_rule = networkx_to_rule_mapping[augment_rule_bucket[key][i]]
            my_rule_head = my_rule.head_atom.relationship
            if my_rule_head not in augment_indexed_by_head:
                augment_indexed_by_head[my_rule_head] = []
            augment_indexed_by_head[my_rule_head].append([key, i])

    for key in base_rule_bucket:
        score_by_key[key] = []

    for key in base_rule_bucket:
        for rule in base_rule_bucket[key]:
            my_rule = networkx_to_rule_mapping[rule]
            my_rule_head = my_rule.head_atom.relationship
            score = None
            if my_rule_head not in augment_indexed_by_head:
                score = Score(-my_rule.head_coverage, -my_rule.pca_confidence, -my_rule.selectivity)
                score_by_key[key].append(score)
            else:
                rule_found_flag = False
                for augment_key, augment_index in augment_indexed_by_head[my_rule_head]:
                    augment_rule = augment_rule_bucket[augment_key][augment_index]
                    if networkx.is_isomorphic(rule, augment_rule, node_match=node_match, edge_match=edge_match):
                        augment_actual = networkx_to_rule_mapping[augment_rule]
                        hc_base = my_rule.head_coverage
                        hc_augment = augment_actual.head_coverage
                        pca_base = my_rule.pca_confidence
                        pca_augment = augment_actual.pca_confidence
                        selec_base = my_rule.selectivity
                        selec_augment = augment_actual.selectivity
                        rule_matches.append([my_rule, augment_actual])
                        hc_score = hc_augment - hc_base
                        pca_score = pca_augment - pca_base
                        selec_score = selec_augment - selec_base

                        score = Score(hc_score, pca_score, selec_score)
                        rule_found_flag = True
                        break

                if rule_found_flag:
                    score_by_key[key].append(score)
                    match_by_key[key] = True
                else:
                    score = Score(-my_rule.head_coverage, -my_rule.pca_confidence,
                                  -my_rule.selectivity)
                    score_by_key[key].append(score)

    aggregator_dict = {}

    for key in base_rule_bucket:
        agg_score = Score(0.0, 0.0, 0.0)

        if not match_by_key[key]:
            aggregator_dict[key] = Score(-1.0, -1.0, -1.0)
        else:
            for score_object in score_by_key[key]:
                agg_score.add(score_object.hc, score_object.pca, score_object.selec)

            agg_score.divide(len(score_by_key[key]), len(score_by_key[key]), len(score_by_key[key]))
            aggregator_dict[key] = agg_score

    return aggregator_dict, rule_matches


def create_bucket_by_head_in_body(bucket, networkx_to_rule_mapping):
    """
        Break bucket by whether predicate in head is also in body

        Args:
            bucket: Dict containing the rules by key
            networkx_to_rule_mapping: Dict containing the mapping between networkx.DiGraph and Rule

        Returns:
            head_in_body: Dict containing rules where head is in body (keyed by type)
            head_not_in_body: Dict containing rules where head is not in body (keyed by type)
    """

    head_in_body = {}
    head_not_in_body = {}

    for key in bucket:
        for rule in bucket[key]:
            my_rule = networkx_to_rule_mapping[rule]
            my_rule_head = my_rule.head_atom.relationship
            head_found_flag = False
            for body_atom in my_rule.body_atoms:
                if body_atom.relationship == my_rule_head:
                    head_found_flag = True

            if head_found_flag:
                if key not in head_in_body:
                    head_in_body[key] = []
                head_in_body[key].append(rule)
            else:
                if key not in head_not_in_body:
                    head_not_in_body[key] = []
                head_not_in_body[key].append(rule)

    return head_in_body, head_not_in_body


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


def write_aggregated_score(dataset_name, model_name, mat_type, folder_to_write, agg_score, bucket,
                           networkx_to_rule_mapping, relations=None, additional=""):
    """
        Write aggregated scores to a file

        Args:
            dataset_name: Name of the dataset
            model_name: Name of the link prediction model
            mat_type: Materialization type
            folder_to_write: Path to folder for writing results
            agg_score: Dict containing aggregated scores for each key
            networkx_to_rule_mapping: Dict containing the mapping between networkx.DiGraph and Rule
            bucket: Bucket that scores have been aggregated on
            relations: List of relations in dataset
            additional: Additional text for filename
    """
    filename = f"{folder_to_write}{dataset_name.lower()}_{model_name.lower()}_{mat_type.lower()}"

    if additional != "":
        filename += f"_{additional}"

    filename += ".csv"
    keys = bucket.keys() if relations is None else relations

    with open(filename, "w+") as file_obj:
        file_obj.write("Type,Rule,HC,PCA,Selec\n")
        for key in keys:

            if key not in bucket:
                hc, pca, selec = Score(-2.0, -2.0, -2.0).get()
            else:
                rule = networkx_to_rule_mapping[bucket[key][0]]
                hc, pca, selec = agg_score[key].get()
            file_obj.write(f"{key},{rule.id_print()},{hc},{pca},{selec}\n")


def get_bucket(model_name, dataset_name, mat_type, relations):
    global universal_node_ids

    folder_to_rules = "D:\PhD\Work\EmbeddingInterpretibility\RulePatterns\data\Experiments\Rules\\"
    rules1 = ParseRule(
        filename=f"{folder_to_rules}{mat_type}\\{dataset_name}\\{model_name}\\{dataset_name.lower()}_{model_name.lower()}_{mat_type.lower()}_base_rules.tsv",
        model_name=model_name, dataset_name=dataset_name)
    rules2 = ParseRule(
        filename=f"{folder_to_rules}{mat_type}\\{dataset_name}\\{model_name}\\{dataset_name.lower()}_{model_name.lower()}_{mat_type.lower()}_augment_rules.tsv",
        model_name=model_name, dataset_name=dataset_name)

    rules1.parse_rules_from_file()
    rules2.parse_rules_from_file()

    rules_base = rules1.rules
    rules_augment = rules2.rules

    universal_node_ids = get_universal_node_id_mapping([rules_base, rules_augment])
    base_graphs, base_nwx_mapping = convert_rules_to_networkx_graphs(rules_base)
    augment_graphs, augment_nwx_mapping = convert_rules_to_networkx_graphs(rules_augment)
    networkx_to_rule_mapping = {**base_nwx_mapping, **augment_nwx_mapping}

    base_bucket_type = create_bucket_by_type(base_graphs)
    augment_bucket_type = create_bucket_by_type(augment_graphs)

    return base_bucket_type, augment_bucket_type, networkx_to_rule_mapping


def results_by_type(model_name, dataset_name, mat_type, relations, base_bucket, augment_bucket,
                    networkx_to_rule_mapping, additional="type"):
    folder_to_write = f"D:\PhD\Work\EmbeddingInterpretibility\RulePatterns\data\Experiments\Results\\{mat_type}\\{dataset_name}\\{model_name}\\"
    agg_score_by_type, rule_match_by_type = aggregate_score(base_rule_bucket=base_bucket,
                                                            augment_rule_bucket=augment_bucket,
                                                            networkx_to_rule_mapping=networkx_to_rule_mapping)
    write_aggregated_score(dataset_name=dataset_name, model_name=model_name, mat_type=mat_type,
                           folder_to_write=folder_to_write, agg_score=agg_score_by_type, bucket=base_bucket,
                           networkx_to_rule_mapping=networkx_to_rule_mapping, additional=additional)
    write_rule_matches(dataset_name=dataset_name, model_name=model_name, mat_type=mat_type,
                       folder_to_write=folder_to_write, rule_matches=rule_match_by_type, additional=additional)


def results_by_head(model_name, dataset_name, mat_type, relations, base_bucket, augment_bucket,
                    networkx_to_rule_mapping, additional="head"):
    folder_to_write = f"D:\PhD\Work\EmbeddingInterpretibility\RulePatterns\data\Experiments\Results\\{mat_type}\\{dataset_name}\\{model_name}\\"
    base_bucket_head = create_bucket_by_head(bucket=base_bucket, networkx_to_rule_mapping=networkx_to_rule_mapping)
    augment_bucket_head = create_bucket_by_head(bucket=augment_bucket,
                                                networkx_to_rule_mapping=networkx_to_rule_mapping)
    agg_score_by_head, rule_match_by_head = aggregate_score(base_rule_bucket=base_bucket_head,
                                                            augment_rule_bucket=augment_bucket_head,
                                                            networkx_to_rule_mapping=networkx_to_rule_mapping)
    write_aggregated_score(dataset_name=dataset_name, model_name=model_name, mat_type=mat_type,
                           folder_to_write=folder_to_write, agg_score=agg_score_by_head, bucket=base_bucket_head,
                           networkx_to_rule_mapping=networkx_to_rule_mapping, additional=additional,
                           relations=relations)
    write_rule_matches(dataset_name=dataset_name, model_name=model_name, mat_type=mat_type,
                       folder_to_write=folder_to_write, rule_matches=rule_match_by_head, additional=additional)


def get_results(model_name, dataset_name, mat_type, relations, additional=""):
    global universal_node_ids
    folder_to_write = f"D:\PhD\Work\EmbeddingInterpretibility\RulePatterns\data\Experiments\Results\\{mat_type}\\{dataset_name}\\{model_name}\\"
    folder_to_rules = "D:\PhD\Work\EmbeddingInterpretibility\RulePatterns\data\Experiments\Rules\\"
    rules1 = ParseRule(
        filename=f"{folder_to_rules}{mat_type}\\{dataset_name}\\{model_name}\\{dataset_name.lower()}_{model_name.lower()}_{mat_type.lower()}_base_rules.tsv",
        model_name=model_name, dataset_name=dataset_name)
    rules2 = ParseRule(
        filename=f"{folder_to_rules}{mat_type}\\{dataset_name}\\{model_name}\\{dataset_name.lower()}_{model_name.lower()}_{mat_type.lower()}_augment_rules.tsv",
        model_name=model_name, dataset_name=dataset_name)

    rules1.parse_rules_from_file()
    rules2.parse_rules_from_file()

    rules_base = rules1.rules
    rules_augment = rules2.rules

    universal_node_ids = get_universal_node_id_mapping([rules_base, rules_augment])
    base_graphs, base_nwx_mapping = convert_rules_to_networkx_graphs(rules_base)
    augment_graphs, augment_nwx_mapping = convert_rules_to_networkx_graphs(rules_augment)
    networkx_to_rule_mapping = {**base_nwx_mapping, **augment_nwx_mapping}

    base_bucket_type = create_bucket_by_type(base_graphs)
    augment_bucket_type = create_bucket_by_type(augment_graphs)
    agg_score_by_type, rule_match_by_type = aggregate_score(base_rule_bucket=base_bucket_type,
                                                            augment_rule_bucket=augment_bucket_type,
                                                            networkx_to_rule_mapping=networkx_to_rule_mapping)
    write_aggregated_score(dataset_name=dataset_name, model_name=model_name, mat_type=mat_type,
                           folder_to_write=folder_to_write, agg_score=agg_score_by_type, bucket=base_bucket_type,
                           networkx_to_rule_mapping=networkx_to_rule_mapping, additional=additional)
    write_rule_matches(dataset_name=dataset_name, model_name=model_name, mat_type=mat_type,
                       folder_to_write=folder_to_write, rule_matches=rule_match_by_type, additional=additional)

    base_bucket_head = create_bucket_by_head(bucket=base_bucket_type, networkx_to_rule_mapping=networkx_to_rule_mapping)
    augment_bucket_head = create_bucket_by_head(bucket=augment_bucket_type,
                                                networkx_to_rule_mapping=networkx_to_rule_mapping)
    agg_score_by_head, rule_match_by_head = aggregate_score(base_rule_bucket=base_bucket_head,
                                                            augment_rule_bucket=augment_bucket_head,
                                                            networkx_to_rule_mapping=networkx_to_rule_mapping)
    write_aggregated_score(dataset_name=dataset_name, model_name=model_name, mat_type=mat_type,
                           folder_to_write=folder_to_write, agg_score=agg_score_by_head, bucket=base_bucket_head,
                           networkx_to_rule_mapping=networkx_to_rule_mapping, additional=additional,
                           relations=relations)
    write_rule_matches(dataset_name=dataset_name, model_name=model_name, mat_type=mat_type,
                       folder_to_write=folder_to_write, rule_matches=rule_match_by_head, additional=additional)


def t():
    dataset_name = "WN18"
    mat_type = "Materialized"
    n_relations = 18
    relations = [i for i in range(n_relations)]

    model_name = "ComplEx"
    base_bucket, augment_bucket, networkx_to_rule_mapping = get_bucket(model_name=model_name, dataset_name=dataset_name,
                                                                       mat_type=mat_type, relations=relations)
    results_by_type(model_name=model_name, dataset_name=dataset_name, mat_type=mat_type, relations=relations,
                    base_bucket=base_bucket, augment_bucket=augment_bucket,
                    networkx_to_rule_mapping=networkx_to_rule_mapping)
    results_by_head(model_name=model_name, dataset_name=dataset_name, mat_type=mat_type, relations=relations,
                    base_bucket=base_bucket, augment_bucket=augment_bucket,
                    networkx_to_rule_mapping=networkx_to_rule_mapping)

    model_name = "TransE"
    base_bucket, augment_bucket, networkx_to_rule_mapping = get_bucket(model_name=model_name, dataset_name=dataset_name,
                                                                       mat_type=mat_type, relations=relations)
    results_by_type(model_name=model_name, dataset_name=dataset_name, mat_type=mat_type, relations=relations,
                    base_bucket=base_bucket, augment_bucket=augment_bucket,
                    networkx_to_rule_mapping=networkx_to_rule_mapping)
    results_by_head(model_name=model_name, dataset_name=dataset_name, mat_type=mat_type, relations=relations,
                    base_bucket=base_bucket, augment_bucket=augment_bucket,
                    networkx_to_rule_mapping=networkx_to_rule_mapping)


if __name__ == "__main__":
    t()
