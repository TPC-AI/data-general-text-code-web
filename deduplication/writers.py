import csv
import os


def write_duplicates_to_csv(duplicates, csvpath, corpus_name, header=None):
    """
    Append a list of duplicates to a csv file
    """
    with open(csvpath, "a+") as fout:
        # if file is empty write header row
        if header is not None and os.stat(csvpath).st_size == 0:
            writer.writerow(header)
        writer = csv.writer(fout)
        writer.writerows((corpus_name,) + duplicates)
