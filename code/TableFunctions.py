import networkx

from Isomorphism_old import convert_rules_to_networkx_graphs, get_networkx_representation, node_match, edge_match, \
    get_universal_node_id_mapping, \
    create_bucket_by_type
from ParseRules import Atom, Rule
import Isomorphism_old

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
            if line == "\n":
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

    splits = file_name.split("\\")
    return splits[-2] if "new" not in file_name else splits[-2]+"_new"


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


def write_file_for_type(results, folder_to_write, dataset_name, additional):
    """
        Write results to file

        Args:
            results (Dict): Dictionary containing results
    """

    f = open(f"{folder_to_write}{dataset_name.lower()}_{additional}type_compared.csv", "w+")
    columns = results[list(results.keys())[0]]

    for type_key in results:
        f.write(f"Rule type:{type_key.id_print()}\n")
        for column in columns:
            hc, pca, selec = results[type_key][column]
            f.write(f"\t{column}\n")
            f.write(f"\t\tHead coverage: {hc}\n")
            f.write(f"\t\tPCA confidence: {pca}\n")
            f.write(f"\t\tSelectivity: {selec}\n")
        f.write("\n")

    f.close()


def match_type_files(files, folder_to_write, dataset_name, additional=""):
    """
        Takes list of files as input and prints rule in latex form based on matching by type of rule
    """
    global universal_node_id_mapping

    rules_by_file_name = {}
    all_rules = []

    # For each file, open the file, extract rules from file, store rules by key
    for file_name in files:
        rules = parse_files_for_rules(file_name)
        rules_by_file_name[file_name] = rules
        all_rules.extend(rules)

    # Get universal node id mapping
    universal_node_id_mapping = get_universal_node_id_mapping([all_rules])
    Isomorphism_old.universal_node_ids = universal_node_id_mapping
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

    write_file_for_type(results, folder_to_write, dataset_name, additional)


if __name__ == "__main__":
    folder_to_write = "D:\PhD\Work\EmbeddingInterpretibility\RulePatterns\data\Experiments\Tables\\"
    match_type_files([
                         "D:\PhD\Work\EmbeddingInterpretibility\RulePatterns\data\Experiments\Results\Materialized\WN18\ComplEx\wn18_complex_materialized_type.csv",
                         "D:\PhD\Work\EmbeddingInterpretibility\RulePatterns\data\Experiments\Results\Materialized\WN18\TransE\wn18_transe_materialized_type.csv"],
                     folder_to_write, dataset_name="WN18")
