import csv


def write_duplicates_to_csv(duplicates, csvpath):
    """
    Append a list of duplicates to a csv file
    """
    with open(csvpath, "a") as fout:
        writer = csv.writer(fout)
        writer.writerows(duplicates)
