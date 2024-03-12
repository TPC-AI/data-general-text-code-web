from datasketch import MinHash, MinHashLSH, MinHashLSHForest
from tqdm.autonotebook import tqdm
from multiprocessing import Pool
from glob import glob
import argparse
import pickle
import json
import csv
import sys
import numpy as np


def byteslist_to_hashvalues(byteslist):
    hashvalues = np.array([], dtype=np.uint64)
    for item in byteslist:
        # unswap the bytes, as their representation is flipped during storage
        hv_segment = np.frombuffer(item, dtype=np.uint64).byteswap()
        hashvalues = np.append(hashvalues, hv_segment)

    return hashvalues


if __name__ == "__main__":
    lsh = MinHashLSHForest(num_perm=128, l=8)

    indir = "/eagle/argonne_tpc/hongz/minhash/arxiv"
    mh_key = None
    mh_orig = None
    for minhashfile in glob(f"{indir}/*.pkl"):
        with open(minhashfile, "rb") as fin:
            minhash_list = pickle.load(fin)
            for item in minhash_list:
                key, minhash = item
                mh_key = key
                mh_orig = minhash
                lsh.add(key, minhash)
                break
            break

    hv = byteslist_to_hashvalues(lsh.keys[mh_key])
    mh_reconstructed = MinHash(hashvalues=hv)

    # ensure hashvalues are the same
    for i, item in enumerate(mh_reconstructed.hashvalues):
        assert item == mh_orig.hashvalues[i]

    assert mh_orig == mh_reconstructed

    # print(mh_orig)
    # print(mh_reconstructed)

    print("JACCARD SELF SIM ESTIMATE:", mh_orig.jaccard(mh_reconstructed))

    # print(vars(mh_orig))
    for k in vars(mh_orig).keys():
        attr = getattr(mh_orig, k)
        r_attr = getattr(mh_reconstructed, k)
        if not (isinstance(attr, np.ndarray) or isinstance(attr, list)):
            assert (
                attr == r_attr
            ), f"Error, items differ in field {k}:\nOriginal: {attr}\nReconstructed: {r_attr}"
        else:
            assert len(attr) == len(r_attr)
            # for i in range(len(attr)):
            #     assert attr[i] == r_attr[i]
