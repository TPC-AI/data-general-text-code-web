import csv

"""
Write out list of duplicates to a csv file
"""
def write_duplicates_to_csv(duplicates, csvpath):
    with open(csvpath, 'w') as fout:
        writer = csv.writer(fout)
        writer.writerows(duplicates)