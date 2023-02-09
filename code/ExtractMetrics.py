from ParseRules import Rule, Atom


class ProcessedRule(Rule):

    def __init__(self, head_atom, body_atoms, old_head_coverage, old_pca_confidence, new_head_coverage,
                 new_pca_confidence, functional_variable="", beta=1):
        super().__init__(head_atom, body_atoms, old_head_coverage, old_pca_confidence, functional_variable, beta)
        self.new_head_coverage = new_head_coverage
        self.new_pca_confidence = new_pca_confidence


def extract_rule(rule_str):
    input_list = rule_str.split(' ')
    output_list = []
    print(input_list)
    for item in input_list:
        if '=>' in item:
            continue

        num, variable1, variable2 = item.replace(')','').replace('(','').split('?')
        output_list.append([int(num), variable1.replace(",", ''), variable2.replace(',', '')])

    return output_list


def extract_metrics(processed_rule_file_path):
    """
    This function takes in a file path for a processed rule file and returns a list of processed rules.

    The processed rule file contains information on rules that have undergone some processing,
    each line in the file corresponds to a single rule. The line is expected to be tab-separated and contains the following information:

    Rule string representation
    Original rule head coverage
    Original rule PCA
    New rule head coverage
    New rule PCA
    This function reads the lines of the file, extracts the information, and constructs a list of ProcessedRule objects.

    Args:
    - processed_rule_file_path (str): The path to the processed rule file.

    Returns:
    - list of ProcessedRule objects: A list of processed rule objects, with each rule's metrics extracted from the input file.
    """
    rules = []
    with open(processed_rule_file_path, "r") as f:
        for line in f:
            line = line.strip()
            splits = line.split("\t")
            rule_str = splits[0]
            o_hc = float(splits[1])
            o_pca = float(splits[2])
            n_hc = float(splits[3])
            n_pca = float(splits[4])

            body_atoms = []
            all_atoms = extract_rule(rule_str)

            for atom_structure in all_atoms[:-1]:
                atom = Atom(atom_structure[0], "?" + atom_structure[1], "?" + atom_structure[2], "")
                body_atoms.append(atom)

            head_atom = Atom(all_atoms[-1][0], "?" + all_atoms[-1][1], "?" + all_atoms[-1][2], "")

            rule = ProcessedRule(head_atom, body_atoms, o_hc, o_pca, n_hc, n_pca)
            print(rule.id_print())
            rules.append(rule)

    return rules
