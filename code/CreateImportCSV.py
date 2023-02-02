def create_import_csv(model_name, dataset_name, triples, folder_to_dataset, folder_to_import):
    """
    Creates the import CSV taking in the triples that have been materialized and appends to existing dataset CSV
    :param model_name: Name of the embedding model
    :param dataset_name: Name of the dataset
    :param triples: List of lists containing 4 values [s,p,o,t] where s is the subject, p is predicate, o is object
                    and t is the r_type, 1 for ~M and 2 for M
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

    for triple in triples:
        subject = triple[0]
        predicate = triple[1]
        object = triple[2]
        r_type = triple[3]

        file_model_relationships.write(f"{subject},{r_type},{object},{predicate}\n")

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

    file_dataset_relationships.write(f":START_ID,r_type,:END_ID,:TYPE\n")
    for line in file_dataset_relationships_alltriples:
        line = line.strip().split("\t")
        file_dataset_relationships.write(f"{line[0]},0,{line[2]},{line[1]}\n")

    file_dataset_nodes.close()
    file_dataset_relationships.close()
    file_dataset_entities_csv.close()
    file_dataset_relationships_alltriples.close()


if __name__ == "__main__":
    dataset = "WN18"
    create_dataset_csv(dataset_name=dataset, folder_to_dataset="D:\\PhD\\Work\\UnderstandingLP\\data\\Datasets")
    model_name = "TransE"
    triples = [[0,1,2,1], [1,1,2,2], [3,2,2,1], [0,1,3,2], [0,2,2,1]]
    create_import_csv(model_name=model_name, dataset_name=dataset, folder_to_dataset="D:\\PhD\\Work\\UnderstandingLP\\data\\Datasets", folder_to_import="D:\\PhD\\Work\\UnderstandingLP\\data\\Imports", triples=triples)