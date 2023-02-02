import subprocess
from ParseRules import ParseRule
import os.path
import os


def combine_predictions(predicate, dataset, model, mat_type):
    alltriples = open(f"../data/Datasets/{dataset}/alltriples.tsv")
    materializations = open(f"../data/Materializations/{dataset}/{model}_{mat_type.lower()}.tsv")
    output = open(f"../data/Experiments/Triples/{mat_type}/{dataset}/{model}/{dataset.lower()}_{model.lower()}_{mat_type.lower()}_predicate_{predicate}_triples.tsv", "w+")

    for line in alltriples:
        output.write(line)

    for line in materializations:
        if line == "\n":
            continue
        splits = line.strip().split("\t")
        if int(splits[1]) == predicate:
            output.write(line)

    alltriples.close()
    output.close()
    materializations.close()


def run_amie(predicates, dataset, model, mat_type, folder):
    #folder=D:/PhD/Work/EmbeddingInterpretibility/RulePatterns/
    print("Running AMIE on base file")

    folder_to_dataset = f"{folder}data/Datasets/{dataset}/"
    folder_to_rules = f"{folder}data/Experiments/Rules/{mat_type}/{dataset}/{model}/"
    folder_to_triples = f"{folder}data/Experiments/Triples/{mat_type}/{dataset}/{model}/"

    dataset_model_mat_filename = f"{dataset.lower()}_{model.lower()}_{mat_type.lower()}"

    base_file_read = f"{folder_to_dataset}alltriples.tsv"
    base_file_write = f"{folder_to_rules}{dataset_model_mat_filename}_base_rules.tsv"
    base_file_subprocess_call = f"java -jar  \"{folder}amie-dev.jar\" \"{base_file_read}\" --datalog > \"{base_file_write}\""
    print(base_file_subprocess_call)
    subprocess.call(base_file_subprocess_call, shell=True)
    print("AMIE run on base files")

    for predicate in range(0, predicates):
        print(f"Combining predicate {predicate} with triples from graph")
        combine_predictions(predicate, dataset, model, mat_type)
        print(f"Running AMIE on predicate {predicate}")
        predicate_file_read = f"{folder_to_triples}{dataset_model_mat_filename}_predicate_{predicate}_triples.tsv"
        predicate_file_write = f"{folder_to_rules}{dataset_model_mat_filename}_predicate_{predicate}_rules.tsv"
        predicate_file_subprocess_call = f"java -jar -Xmx20g  \"{folder}amie-dev.jar\" \"{predicate_file_read}\" -htr {predicate} --datalog > \"{predicate_file_write}\""
        print(predicate_file_subprocess_call)
        subprocess.call(predicate_file_subprocess_call, shell=True)
        print(f"Finished running AMIE on predicate {predicate}")

    f_result = open(f"{folder_to_rules}{dataset_model_mat_filename}_augment_rules.tsv", "w+")
    for ctr in range(0,15):
        f_result.write("Placeholder\n")
    for predicate in range(0, predicates):
        # Collect rules from each predicate file, and then combine them into one file
        # Optional delete individual files

        predicate_file = open(f"{folder_to_rules}{dataset_model_mat_filename}_predicate_{predicate}_rules.tsv")
        num_lines = sum(1 for line in open(f"{folder_to_rules}{dataset_model_mat_filename}_predicate_{predicate}_rules.tsv", "r"))
        for ctr in range(0, 15):
            predicate_file.readline()

        for ctr in range(15, num_lines-3):
            f_result.write(predicate_file.readline())

        predicate_file.close()
        os.remove(f"{folder_to_rules}{dataset_model_mat_filename}_predicate_{predicate}_rules.tsv")

    f_result.write("Placeholder\nPlaceholder\nPlaceholder")
    f_result.close()



if __name__ == "__main__":
    predicates = 18
    model = "ComplEx"
    dataset = "WN18"
    mat_type = "Mispredicted"
    folder = "D:\PhD\Work\EmbeddingInterpretibility\RulePatterns\\"

    run_amie(predicates, dataset, model, mat_type, folder)

    #check_rules(predicates, dataset, model)
