
def combine_predictions(predicate, dataset, model):
    alltriples = open(f"../data/Datasets/{dataset}/alltriples.tsv")
    materializations = open(f"../data/Materializations/{dataset}/{model}_materialized.tsv")
    output = open(f"../data/Experiments/{dataset.lower()}_{model.lower()}_alltriples_predicate_{predicate}.tsv", "w+")

    for line in alltriples:
        output.write(line)

    for line in materializations:
        splits = line.strip().split("\t")
        if int(splits[1]) == predicate:
            output.write(line)

    alltriples.close()
    output.close()
    materializations.close()

if __name__ == "__main__":
    predicate = 0
    dataset = "WN18RR"
    model = "TransE"
    combine_predictions(predicate, dataset, model)