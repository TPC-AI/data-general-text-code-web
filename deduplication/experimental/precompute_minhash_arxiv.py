from tqdm.autonotebook import tqdm
from multiprocessing import Pool
from datasketch import MinHash
from typing import Optional
from glob import glob
import pickle
import json


def compute_minhash(t: tuple) -> Optional[tuple]:
    lineNo, line = t
    lineNo += 1
    line = json.loads(line)
    line = line.get("text", "")
    s = set(line.split())
    if not s:
        return None
    m = MinHash(num_perm=128)
    for d in s:
        m.update(d.encode("utf8"))
    key = f"{fname}-{lineNo}"
    return (key, m)


if __name__ == "__main__":
    n = 50000  # (roughly) the number of articles stored in each input file
    for infile in glob("/eagle/tpc/hongz/arxiv_jsonl/*.jsonl"):
        fname = infile.split("/")[-1]
        with open(infile) as fin, Pool(32) as p, tqdm(total=n, desc=fname) as pbar:
            minhash_list = list()
            for result in p.imap_unordered(compute_minhash, enumerate(fin)):
                if result:
                    minhash_list.append(result)
                    pbar.update()
            with open(f"/eagle/tpc/hongz/minhash/arxiv/{fname[:-6]}.pkl", "wb") as fp:
                pickle.dump(minhash_list, fp)
            print(
                f"Generated MinHash for {len(minhash_list):,} scientific articles in {fname}"
            )
