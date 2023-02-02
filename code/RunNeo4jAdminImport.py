import os
from subprocess import Popen
from neo4j import GraphDatabase


def create_neo4j_database(model_name, dataset_name, path_to_imports="D:\\PhD\\Work\\UnderstandingLP\\data\\Imports",
                          path_to_database="C:\\Users\\nk1581\\.Neo4jDesktop\\relate-data\\dbmss\\dbms-433e1738-75d6-4a65-9a5c-d8993328df46"):
    """
    Function to create neo4j database using neo4j-admin
    :param path_to_imports: Path to Imports folder
    :param model_name: Name of model
    :param dataset_name: Name of database
    :param path_to_database: Path to neo4j database
    :return:
    """

    # First copy _nodes.csv and _relationships.csv to the import folder in the neo4j database

    base_file_name = f"{dataset_name}_{model_name}"
    database_name = base_file_name.replace("_", "").lower()
    path_to_import_nodes = f"{path_to_imports}\\{dataset_name}\\{model_name}\\{base_file_name}_nodes.csv"
    path_to_relationships_nodes = f"{path_to_imports}\\{dataset_name}\\{model_name}\\{base_file_name}_relationships.csv"

    path_to_neo4j_import = f"{path_to_database}\\import\\"

    os.system(f"copy {path_to_import_nodes} {path_to_neo4j_import}")
    os.system(f"copy {path_to_relationships_nodes} {path_to_neo4j_import}")
    print(
        f"{path_to_database}\\bin\\neo4j-admin database import full --nodes=import/{base_file_name}_nodes.csv --relationships=import/{base_file_name}_relationships.csv \"{database_name}\"")

    p = Popen(
        f"neo4j-admin.bat database import full --nodes=import/{base_file_name}_nodes.csv --relationships=import/{base_file_name}_relationships.csv \"{database_name}\"",
        cwd=f"{path_to_database}\\bin\\", shell=True)
    stdout, stderr = p.communicate()

    driver = GraphDatabase.driver("neo4j://localhost:7687",
                                  auth=("neo4j", "password"))

    with driver.session(database="system") as session:
        session.execute_write(create_db, database_name)

    driver.close()


def create_db(tx, db_name):
    tx.run(f"CREATE DATABASE {db_name}")


if __name__ == "__main__":
    dataset = "WN18"
    model = "TransE"
    create_neo4j_database(model_name=model, dataset_name=dataset)
