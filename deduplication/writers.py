import csv
import os


def write_duplicates_to_csv(duplicates, csvpath, corpus_name, header=None):
    """
    Append a list of duplicates to a csv file
    """
    # just in case, make output dir
    dirname = os.path.dirname(csvpath)
    os.makedirs(dirname, exist_ok=True)
    # prepend corpus name to each csv row for organization
    dups = [(corpus_name,) + d for d in duplicates]
    with open(csvpath, "a+") as fout:
        writer = csv.writer(fout)
        # if file is empty write header row
        if header is not None and os.stat(csvpath).st_size == 0:
            writer.writerow(header)
        writer.writerows(dups)
    print(f"Wrote {len(dups)} duplicates to {csvpath}")
