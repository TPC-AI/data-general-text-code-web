from tqdm.autonotebook import tqdm
from multiprocessing import Pool
from datasketch import MinHash
from typing import Optional
from glob import glob
import pickle


def compute_minhash(t: tuple) -> Optional[tuple]:
    lineNo, doc = t
    s = set(doc.split())
    if not s:
        return None
    m = MinHash(num_perm=128)
    for d in s:
        m.update(d.encode('utf8'))
    key = f"{fname}-{lineNo}"
    return (key, m)


def prd_doc_generator(fin):
    doc = list()
    starting_lineNo = -1
    for lineNo, line in enumerate(fin):
        if line.strip():
            if starting_lineNo == -1:
                starting_lineNo = lineNo + 1
            doc.append(line)
        elif starting_lineNo > 0:
            yield (starting_lineNo, ''.join(doc))
            doc = list()
            starting_lineNo = -1


if __name__ == '__main__':
    n = 1000000    # (roughly) the number of articles stored in each input file
    for infile in glob('/grand/SuperBERT/hongz/PRD/BB/sentences_*.txt'):
        fname = infile.split('/')[-1]
        with open(infile) as fin, Pool(32) as p, tqdm(total=n, desc=fname) as pbar:
            minhash_list = list()
            for result in p.imap_unordered(compute_minhash, prd_doc_generator(fin)):
                if result:
                    minhash_list.append(result)
                    pbar.update()
            with open(f"/eagle/tpc/hongz/minhash/prd/{fname[:-4]}.pkl", 'wb') as fp:
                pickle.dump(minhash_list, fp)
