import os
import sys

import networkx

from Isomorphism import convert_rules_to_networkx_graphs, node_match, \
    get_universal_node_id_mapping, \
    create_bucket_by_type
from ParseRules import Atom, Rule
import Isomorphism
import LatexUtils

global universal_node_id_mapping


def parse_files_for_rules(rule_filename):
    """
        Takes filename as input and parses file for rules
        Args:
            rule_filename: File name of rules file
        Returns:
            rules: List of Rule
    """

    rules = []
    with open(rule_filename, "r") as rule_file:
        rule_file.readline()
        for line in rule_file:
            if line == "\n" or "NULL" in line:
                continue

            line_splits = line.strip().split(",")
            first_index = line.index(",") + 1

            second_index = len(line) - line[::-1].index(")")
            rule_str_splits = line[first_index:second_index].split(" ")
            selec = round(float(line_splits[-1]), 4)
            pca = round(float(line_splits[-2]), 4)
            hc = round(float(line_splits[-3]), 4)
            body_atoms = []
            for atom in rule_str_splits[:-1]:
                relation = int(atom[0:atom.index("(")])
                variable1 = atom[atom.index("(") + 1: atom.index(",")]
                variable2 = atom[atom.index(",") + 1: atom.index(")")]
                body_atoms.append(Atom(relation, variable1, variable2, ""))

            atom = rule_str_splits[-1]
            relation = int(atom[atom.index("==>") + 3:atom.index("(")])
            variable1 = atom[atom.index("(") + 1: atom.index(",")]
            variable2 = atom[atom.index(",") + 1: atom.index(")")]
            head_atom = Atom(relation, variable1, variable2, "")
            rule = Rule(head_atom, body_atoms, hc, pca)
            rule.selectivity = selec
            rules.append(rule)
    return rules


def get_model_name(file_name):
    """
        Return model_name from file_name
        Args:
            file_name: The file name
        Returns:
            model_name: The name of the model
    """

    splits = file_name.split("/")
    return splits[-1].split("_")[1]


def get_matching_nwx_graph(current_nwx_graph, candidates):
    """
        Returns the matching Networkx.DiGraph object

        Args:
            current_nwx_graph: NetworkX.DiGraph of the type of rule to match
            candidates (List[networkx.DiGraph]): Candidates for matching

        Returns:
            matching_nwx_graph (networkx.DiGraph): Matching DiGraph
    """
    matching_nwx_graph = None
    for candidate in candidates:
        candidate_nwx_graph = candidate
        if networkx.is_isomorphic(current_nwx_graph, candidate_nwx_graph, node_match=node_match):
            matching_nwx_graph = candidate_nwx_graph
            break

    return matching_nwx_graph


def write_file_for_head(results, folder_to_write, dataset_name, additional, k):
    data_hc = {}
    data_pca = {}
    data_selec = {}

    for relation in results:
        data_hc[relation] = {}
        data_pca[relation] = {}
        data_selec[relation] = {}

        for model_name in results[relation]:
            hc, pca, selec = results[relation][model_name]

            data_hc[relation][model_name] = hc
            data_pca[relation][model_name] = pca
            data_selec[relation][model_name] = selec

    table_hc = LatexUtils.create_regular_table(data_hc, "Head relationship")
    table_pca = LatexUtils.create_regular_table(data_pca, "Head relationship")
    table_selec = LatexUtils.create_regular_table(data_selec, "Head relationship")

    table_hc.to_csv(f"{folder_to_write}/{dataset_name}/{dataset_name}_{additional}_k_{k}_compared_hc.csv", index=False,
                    index_label=False)
    table_pca.to_csv(f"{folder_to_write}/{dataset_name}/{dataset_name}_{additional}_k_{k}_compared_pca.csv",
                     index=False, index_label=False)
    table_selec.to_csv(f"{folder_to_write}/{dataset_name}/{dataset_name}_{additional}_k_{k}_compared_selec.csv",
                       index=False, index_label=False)


def write_file_for_type(results, folder_to_write, dataset_name, additional, k):
    """
        Write results to file

        Args:
            results (Dict): Dictionary containing results
    """

    columns = results[list(results.keys())[0]]

    data_hc = {}
    data_pca = {}
    data_selec = {}

    for type_key in results:
        data_hc[type_key.id_print()] = {}
        data_pca[type_key.id_print()] = {}
        data_selec[type_key.id_print()] = {}

    for type_key in results:
        for column in columns:
            hc, pca, selec = results[type_key][column]
            data_hc[type_key.id_print()][column] = round(hc, 4)
            data_pca[type_key.id_print()][column] = round(pca, 4)
            data_selec[type_key.id_print()][column] = round(selec, 4)

    table_hc = LatexUtils.create_regular_table(data_hc, "Rule types")
    table_pca = LatexUtils.create_regular_table(data_pca, "Rule types")
    table_selec = LatexUtils.create_regular_table(data_selec, "Rule types")

    table_hc.to_csv(f"{folder_to_write}/{dataset_name}/{dataset_name}_{additional}_k_{k}_compared_hc.csv", index=False,
                    index_label=False)
    table_pca.to_csv(f"{folder_to_write}/{dataset_name}/{dataset_name}_{additional}_k_{k}_compared_pca.csv",
                     index=False, index_label=False)
    table_selec.to_csv(f"{folder_to_write}/{dataset_name}/{dataset_name}_{additional}_k_{k}_compared_selec.csv",
                       index=False, index_label=False)


def match_head_files(folder_to_aggregated_rules, folder_to_write, dataset_name, additional, k, relation_list):
    models = ["boxe", "complex", "hake", "hole", "quate", "rotate", "rotpro", "toruse", "transe", "tucker"]
    global universal_node_id_mapping

    rules_by_file_name = {}
    all_rules = []
    files = [
        f"{folder_to_aggregated_rules}/{dataset_name}/{model_name}/{dataset_name}_{model_name}_k_{k}_aggregated_{additional}.csv"
        for model_name in models]
    # For each file, open the file, extract rules from file, store rules by key
    for file_name in files:
        if not os.path.exists(file_name):
            continue
        rules = parse_files_for_rules(file_name)
        rules_by_file_name[file_name] = rules
        all_rules.extend(rules)

    results = {}

    for relation in relation_list:
        results[relation] = {}
        for file_name in files:
            model_name = get_model_name(file_name)
            rules = rules_by_file_name[file_name]
            matching_rule = None

            for rule in rules:
                if rule.head_atom.relationship == relation:
                    matching_rule = rule
                    break

            if matching_rule is not None:
                results[relation][model_name] = [matching_rule.head_coverage, matching_rule.pca_confidence,
                                                 matching_rule.selectivity]

            else:
                results[relation][model_name] = [-2.0, -2.0, -2.0]

    write_file_for_head(results, folder_to_write, dataset_name, additional, k)



def match_type_files(folder_to_aggregated_rules, folder_to_write, dataset_name, additional, k):
    """
        Takes list of files as input and prints rule in latex form based on matching by type of rule
    """

    models = ["boxe", "complex", "hake", "hole", "quate", "rotate", "rotpro", "toruse", "transe", "tucker"]
    global universal_node_id_mapping

    rules_by_file_name = {}
    all_rules = []
    files = [
        f"{folder_to_aggregated_rules}/{dataset_name}/{model_name}/{dataset_name}_{model_name}_k_{k}_aggregated_{additional}.csv"
        for model_name in models]
    # For each file, open the file, extract rules from file, store rules by key
    for file_name in files:
        if not os.path.exists(file_name):
            continue
        rules = parse_files_for_rules(file_name)
        rules_by_file_name[file_name] = rules
        all_rules.extend(rules)

    # Get universal node id mapping
    universal_node_id_mapping = get_universal_node_id_mapping([all_rules])
    Isomorphism.universal_node_ids = universal_node_id_mapping
    nwx_mapping = {}
    nwx_graph_by_file_name = {}
    nwx_rules = []
    for file_name in files:
        file_graph, file_mapping = convert_rules_to_networkx_graphs(rules_by_file_name[file_name])
        nwx_mapping = {**nwx_mapping, **file_mapping}
        nwx_graph_by_file_name[file_name] = file_graph
        nwx_rules.extend(file_graph)

    bucket_by_type = create_bucket_by_type(nwx_rules)

    results = {}
    for type_key in bucket_by_type:
        current_nwx_graph = bucket_by_type[type_key][0]
        current_rule = nwx_mapping[current_nwx_graph]
        results[current_rule] = {}
        for file_name in files:
            model_name = get_model_name(file_name)
            results[current_rule][model_name] = []
            matching_nwx_graph = get_matching_nwx_graph(current_nwx_graph, nwx_graph_by_file_name[file_name])

            hc = -2.0
            pca = -2.0
            selec = -2.0

            if matching_nwx_graph is not None:
                rule = nwx_mapping[matching_nwx_graph]
                hc = rule.head_coverage
                pca = rule.pca_confidence
                selec = rule.selectivity

            results[current_rule][model_name].extend([hc, pca, selec])

    write_file_for_type(results, folder_to_write, dataset_name, additional, k)


if __name__ == "__main__":
    # folder_to_result = sys.argv[1]
    # folder_to_aggregated_rules = sys.argv[2]
    # dataset_name = sys.argv[3]
    # k = int(sys.argv[4])

    dataset_name = "WN18"
    folder_to_write = "D:/PhD/Work/UnderstandingLP/data/ComparedRules"
    folder_to_aggregated_rules = "D:/PhD/Work/UnderstandingLP/data/AggregatedRules"
    folder_to_dataset = "D:/PhD/Work/UnderstandingLP/data/Datasets"
    k = 5

    for dataset_name in ["WN18", "WN18RR"]:
        match_type_files(dataset_name=dataset_name, folder_to_write=folder_to_write,
                         folder_to_aggregated_rules=folder_to_aggregated_rules, k=5, additional="unique_type")

        match_type_files(dataset_name=dataset_name, folder_to_write=folder_to_write,
                         folder_to_aggregated_rules=folder_to_aggregated_rules, k=5, additional="repeating_type")

        n_relations = Isomorphism.get_relation_list(folder_to_dataset, dataset_name)
        match_head_files(dataset_name=dataset_name, folder_to_write=folder_to_write,
                         folder_to_aggregated_rules=folder_to_aggregated_rules, k=5, additional="unique_head",
                         relation_list=n_relations)

        match_head_files(dataset_name=dataset_name, folder_to_write=folder_to_write,
                         folder_to_aggregated_rules=folder_to_aggregated_rules, k=5, additional="repeating_head",
                         relation_list=n_relations)
