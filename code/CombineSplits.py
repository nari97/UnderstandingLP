
def combine_splits():
    datasets = ["FB15K", "FB15K-237", "WN18", "WN18RR"]

    for dataset in datasets:
        combined = open(f"../data/Datasets/{dataset}/alltriples.tsv", "w+")
        train = open(f"../data/Datasets/{dataset}/train2id.txt")
        valid = open(f"../data/Datasets/{dataset}/valid2id.txt")
        test = open(f"../data/Datasets/{dataset}/test2id.txt")

        train.readline()
        valid.readline()
        test.readline()
        print(dataset)
        for split in [train, valid, test]:
            for line in split:

                splits = line.strip().split(" ")

                combined.write(f"{splits[0]}\t{splits[2]}\t{splits[1]}\n")

        train.close()
        valid.close()
        test.close()
        combined.close()

if __name__ == "__main__":
    combine_splits()