# Arxiv dataset description and processing methods

## Description

Arxiv provides access to more than 2M preprints from Arxiv.org. All are [available for download](https://info.arxiv.org/help/bulk_data/index.html), but license terms vary.

As of Aug 12, 2023, Arxiv has 2328073 papers, of which 1700092 have properly formed Latex, 191617 have PDF only, 420637 have a GZip file (usually containing Tex, it seems), 9800 have no Tex file, and 5927 have a Tex file without a dataclass declaration.

Several groups have prepared Arxiv-based datasets for use in LLM training:

* [The Pile](https://pile.eleuther.ai) includes, as pile_set_name ArXiv, 2,377,741 Arxiv documents in JSON format. The [Jan 13 2022 datasheet](https://arxiv.org/pdf/2201.07311.pdf) states "We downloaded the TEX sources of all papers on arXiv up to the July 2020 dump (the last file included in our data is arXiv src 2007 068.tar) via arXiv’s S3 Bulk Source File Access8, and used pandoc 1.19.2.4 to convert these source files to Markdown, discarding any papers which had errors during the conversion process." An initial self-deduplication suggests quite a few duplicates.
  
* [RedPajama-1}(https://huggingface.co/datasets/togethercomputer/RedPajama-Data-1T) includes as arxiv a set of jsonl files with 1,558,306 Arxiv papers with 10,531,941,643 words and 93,794,825,396 characters. They state, "We only keep latex source files and remove preambles, comments, macros and bibliographies."

Uncertainties:

* Why the Pile has so many more documents than RP1, despite them both supposedly working with the TeX source.
* How much difference there is between Pile and RP1
* Whether this is value running a new extraction pipeline on Arxiv sourcex.

