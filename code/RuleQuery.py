from neo4j import GraphDatabase
from datetime import datetime
from ParseRules import ParseRule

def filter_rules(rules, hc=None, pca=None, selectivity=None):
    """
    Filter rules by head coverage, pca confidence or selectivity
    :param rules: List of Rules
    :param hc: Head coverage
    :param pca: Pca confidence
    :param selectivity: Selectivity
    :return: filtered_rules
    """

    filtered_rules = []

    for rule in rules:
        pca_flag = False
        hc_flag = False
        selec_flag = False

        if pca is not None:
            if rule.pca_confidence >= pca:
                pca_flag = True

        if hc is not None:
            if rule.head_coverage >= hc:
                hc_flag = True

        if selectivity is not None:
            if rule.selectivity >= selectivity:
                selec_flag = True

        add_flag = True

        if pca is not None:
            if pca_flag:
                add_flag &= True
            else:
                add_flag &= False

        if hc is not None:
            if hc_flag:
                add_flag &= True
            else:
                add_flag &= False

        if selectivity is not None:
            if selec_flag:
                add_flag &= True
            else:
                add_flag = False

        if add_flag:
            filtered_rules.append(rule)

    return filtered_rules


def get_pca_and_hc(rule, driver, dataset, is_mispredicted=False):
    """
        Computes the Head Coverage (HC) and PCA of a given association rule.

        :param is_mispredicted: Flag indicating if we are including mispredicted triples
        :param dataset: Name of emo4j database
        :type database: str
        :param rule: The association rule for which HC and PCA should be computed.
        :type rule: Rule
        :param driver: The Neo4j driver to run the queries.
        :type driver: neo4j.Driver
        :return: A tuple of two values representing HC and PCA.
        :rtype: tuple
    """
    query = ""
    res = None

    # Find non functional variable
    non_func_var = "b" if rule.functional_variable == "a" else "a"

    # Build query
    relations_in_query = 0
    for atom in rule.body_atoms:
        query += " MATCH " + atom.neo4j_print().replace("[", f"[r{relations_in_query}")
        relations_in_query += 1

    with driver.session(database=dataset) as session:
        # Define hashmap
        support = set()
        pca = set()
        total_heads = 0
        body_pairs = {}

        #print(f"{datetime.now()} -- Running body query")

        # Run body query
        query += " WHERE "
        for rel in range(relations_in_query):
            if is_mispredicted:
                query += f"r{rel}.triple_type<>\"2\" AND "
            else:
                query += f"r{rel}.triple_type=\"1\" AND "


        query = query[:-4]
        result = session.run(query + " RETURN id(a) AS a, id(b) AS b")
        for record in result:
            # Get functional and non functional variable
            fv = record[rule.functional_variable]
            nfv = record[non_func_var]

            # If nv does not exist, add nv using fv as key
            if fv not in body_pairs:
                body_pairs[fv] = set()

            body_pairs[fv].add(nfv)

        #print(f"{datetime.now()} -- Running head query")

        # Define hash set for support and pca

        # Define set for functional variables
        all_fvs = set()

        # Define query for head atom
        if is_mispredicted:
            result = session.run("MATCH " + rule.head_atom.neo4j_print().replace("[",
                                                                             "[r0") + " WHERE r0.triple_type<>\"2\" RETURN id(a) AS a, id(b) AS b")
        else:
            result = session.run("MATCH " + rule.head_atom.neo4j_print().replace("[",
                                                                             "[r0") + " WHERE r0.triple_type=\"1\" RETURN id(a) AS a, id(b) AS b")

        for record in result:
            # Get fv and nfv
            fv = record[rule.functional_variable]
            nfv = record[non_func_var]

            # Add fv to fv set
            all_fvs.add(fv)

            # If bodypairs contains fv and the value is the nfv, add to support
            if fv in body_pairs and nfv in body_pairs[fv]:
                support.add(f"{fv},{nfv}")

            total_heads += 1

        #print(f"{datetime.now()} -- Computing PCA")

        # Compute pca
        # Iterate through all fvs
        for fv in all_fvs:
            # If bodyPairs contains the fv
            if fv in body_pairs:
                # For all values in key of body pairs, add to pca
                for other in body_pairs[fv]:
                    pca.add(f"{fv},{other}")

        #print("Body pairs size: " + str(len(body_pairs)))
        #print("Head size: " + str(total_heads))
        #print("Support size: " + str(len(support)))
        #print("PCA size: " + str(len(pca)))
        hc = round(1.0 * len(support) / total_heads, 4)
        pca = round(1.0 * len(support) / len(pca), 4)

        return hc, pca


def a1():
    models = [ "TransE"]
    datasets = ["WN18RR"]

    driver = GraphDatabase.driver("neo4j://localhost:7687",
                                  auth=("neo4j", "password"))
    for model in models:
        for dataset in datasets:
            mismatch = 0
            rule_path = r"D:\PhD\Work\EmbeddingInterpretibility\Interpretibility\Results\MinedRules"
            dataset_folder = r"D:\PhD\Work\UnderstandingLP\data\Datasets"
            rp = ParseRule(filename=fr"{rule_path}\{dataset}\{model}_mispredicted.tsv", model_name=model,
                           dataset_name=dataset, dataset_folder=fr"{dataset_folder}\\")
            rp.parse_rules_from_file()

            database = f"{dataset.lower()}{model.lower()}"
            print(f"Dataset: {dataset}, Model: {model}")
            for rule in rp.rules[:25]:
                hc, pca = get_pca_and_hc(rule, driver=driver, dataset=database, is_mispredicted=True)
                rule_hc = round(rule.head_coverage, 4)
                rule_pca = round(rule.pca_confidence, 4)

                print(f"Rule PCA: {rule_pca}, Comp PCA: {pca}, Rule HC: {rule_hc}, Comp HC: {hc}")
                if rule_hc != hc or rule_pca != pca:
                    mismatch += 1

            print(f"Dataset: {dataset}, Model: {model}, Mismatch: {mismatch}")
            if mismatch != 0:
                print("mismatch")
                exit()


if __name__ == "__main__":
    a1()
