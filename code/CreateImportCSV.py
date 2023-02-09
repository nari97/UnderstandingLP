def create_import_csv(model_name, dataset_name, mispredicted_triples, folder_to_dataset, folder_to_import):
    """
    Creates the import CSV taking in the triples that have been materialized and appends to existing dataset CSV
    :param model_name: Name of the embedding model
    :param dataset_name: Name of the dataset
    :param mispredicted_triples: Dictionary indicating which triples are mispredicted
    :param folder_to_dataset: Path to folder that contains the CSV files for the dataset
    :param folder_to_import: Path to folder that contains CSV files for final database import
    :return:
    """

    path_to_model = f"{folder_to_import}\\{dataset_name}\\{model_name}\\"
    path_to_dataset = f"{folder_to_dataset}\\{dataset_name}\\"

    path_to_model_nodes = f"{path_to_model}\\{dataset_name}_{model_name}_nodes.csv"
    path_to_model_relationships = f"{path_to_model}\\{dataset_name}_{model_name}_relationships.csv"

    path_to_dataset_nodes = f"{path_to_dataset}\\{dataset_name}_nodes.csv"
    path_to_dataset_relationships = f"{path_to_dataset}\\{dataset_name}_relationships.csv"

    file_model_nodes = open(path_to_model_nodes, "w+")
    file_model_relationships = open(path_to_model_relationships, "w+")

    file_dataset_nodes = open(path_to_dataset_nodes, "r")
    file_dataset_relationships = open(path_to_dataset_relationships, "r")

    for line in file_dataset_nodes:
        file_model_nodes.write(line)

    for line in file_dataset_relationships:
        file_model_relationships.write(line)

    '''
        Nodes file has the headers nodeId:ID,:LABEL
        Relationship file has the headers :START_ID,r_type,:END_ID,:TYPE
    '''

    for triple_key in mispredicted_triples:

        r_type = mispredicted_triples[triple_key]
        s = triple_key[0]
        p = triple_key[1]
        o = triple_key[2]
        if r_type:
            file_model_relationships.write(f"{s},0,{o},{p}\n")
        else:
            file_model_relationships.write(f"{s},1,{o},{p}\n")

    file_dataset_relationships.close()
    file_dataset_nodes.close()
    file_model_relationships.close()
    file_dataset_relationships.close()


def create_dataset_csv(dataset_name, folder_to_dataset) -> None:
    """
    Function to create the nodes.csv and relationships.csv for a dataset using the train, validation and test splits
    :param dataset_name: Name of dataset
    :param folder_to_dataset: Path to folder that contains the CSV files for the dataset
    :return:
    """

    path_to_dataset = f"{folder_to_dataset}\\{dataset_name}\\"
    file_dataset_nodes = open(f"{path_to_dataset}\\{dataset_name}_nodes.csv", "w+")
    file_dataset_relationships = open(f"{path_to_dataset}\\{dataset_name}_relationships.csv", "w+")

    file_dataset_entities_csv = open(f"{path_to_dataset}\\entity2id.txt", "r")
    file_dataset_relationships_alltriples = open(f"{path_to_dataset}\\alltriples.tsv", "r")

    file_dataset_nodes.write(f"nodeId:ID,:LABEL\n")
    file_dataset_entities_csv.readline()
    for line in file_dataset_entities_csv:
        splits = line.strip().split("\t")
        if len(splits) == 1:
            splits = line.strip().split(" ")

        entity_id = int(splits[1])
        file_dataset_nodes.write(f"{entity_id},NODE\n")

    file_dataset_relationships.write(f":START_ID,triple_type,:END_ID,:TYPE\n")
    for line in file_dataset_relationships_alltriples:
        line = line.strip().split("\t")
        file_dataset_relationships.write(f"{line[0]},2,{line[2]},{line[1]}\n")

    file_dataset_nodes.close()
    file_dataset_relationships.close()
    file_dataset_entities_csv.close()
    file_dataset_relationships_alltriples.close()


def get_mispredicted_flagged_triples(folder_to_dataset, model_name):
    """
    This function finds which of the entire set of materialized triples are mispredictions
    :param folder_to_dataset: Folder to materializations dataset
    :param model_name: Name of the model
    :return:
    """

    materialized_triples_file_path = f"{folder_to_dataset}\\{model_name}_materialized.tsv"
    mispredicted_triples_file_path = f"{folder_to_dataset}\\{model_name}_mispredicted.tsv"

    mispredicted_triples = {}

    materialized_file = open(materialized_triples_file_path, "r")
    mispredicted_file = open(mispredicted_triples_file_path, "r")

    materialized_ctr = 0
    mispredicted_ctr = 0
    for line in mispredicted_file:
        if line == "\n":
            continue
        line = line.strip()
        s, p, o = line.split("\t")
        mispredicted_triples[(s, p, o)] = True
        mispredicted_ctr += 1

    for line in materialized_file:
        if line == "\n":
            continue
        line = line.strip()
        s, p, o = line.split("\t")
        mispredicted_triples[(s, p, o)] = False
        materialized_ctr += 1

    print("Number of materialized triples: ", materialized_ctr)
    print("Number of mispredicted triples: ", mispredicted_ctr)
    materialized_file.close()
    mispredicted_file.close()
    return mispredicted_triples


def reconcile_materialization_and_import(dataset_name, model_name):
    """
    This function computes the mispredicted triples and writes the files
    :param dataset_name: Name of the dataset
    :param model_name: Name of the model
    :return:
    """
    folder_to_dataset = "D:\\PhD\\Work\\UnderstandingLP\\data\\Datasets"
    folder_to_import = "D:\\PhD\\Work\\UnderstandingLP\\data\\Imports"
    folder_to_materialization = "D:\\PhD\\Work\\EmbeddingInterpretibility\\Interpretibility\\Results\\Materializations"

    mispredicted_flagged_triples = get_mispredicted_flagged_triples(
        folder_to_dataset=f"{folder_to_materialization}\\{dataset_name}\\", model_name=model_name)

    create_dataset_csv(dataset_name=dataset_name, folder_to_dataset=folder_to_dataset)
    create_import_csv(model_name=model_name, dataset_name=dataset_name,
                      mispredicted_triples=mispredicted_flagged_triples, folder_to_dataset=folder_to_dataset,
                      folder_to_import=folder_to_import)

if __name__ == "__main__":

    dataset_name = "WN18RR"
    model_name = "ComplEx"

    print("Dataset name: ", dataset_name)
    print("Model name: ", model_name)
    reconcile_materialization_and_import(dataset_name=dataset_name, model_name=model_name)
