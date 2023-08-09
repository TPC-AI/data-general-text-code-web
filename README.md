# data-general-text-code-web
Material relating to data-general-text-code-web

The **data-general-text-code-web** group in TPC 

## Scientific articles

We want to assemble a de-duplicated collection of scientific articles, with for each the extracted text. (Ultimately figures and other content may also be wanted.) Our initial plan is to maintain these data on the Eagle file system at ALCF, but of course others may want to create copies elsewhere as well. [Note that not all sources allow the creation of derivative copies.]

Potential sources for articles include the Pile, Arxiv, DOE OSTI, PubMed, BioArxiv, etc. Different sources may have data in different formats and duplicates. Not all record DOIs or have accurate metadata. 

We want to create a database that details something like, (DOI-if-available, metadata, source, location(s)-on-tpc-storage, info about duplicates).

To support this work, we are engaged in the following activities:

1. Development of a [curated list of potential data sources](https://docs.google.com/spreadsheets/d/1cGTAsrWMd2pLtYEi8W432SODt6RVM14YJPEsPhvq6uA/edit#gid=0) with details on how to access eacb.
2. Assembly of articles from different sources, placing them at ALCF at `/lus/eagle/projects/tpc/Text`. So far we have:
  * [The Pile](https://pile.eleuther.ai), in JSONL format, at `/lus/eagle/projects/tpc/Text/jsonl_pile` [there is a binary format version elsewhere). Note that only part of this is scientific articles: see below.
  * [Biorxiv](https://www.biorxiv.org/tdm): download in progress, to be at `/lus/eagle/projects/tpc/Text/biorxiv`
  * ...
3. Extraction of text from PDF articles, when not available in other formats. We are working with two methods that we should compare:
  * [Grobid](https://grobid.readthedocs.io/en/latest/) to produce XML, and then simple extraction of text from XML. (Ian has code.)
  * Andrew McNaughton has been using [PyPDF2](https://pypi.org/project/PyPDF2/)
4. Evaluation of similarity
  * We are experimenting with [MinhashLSH](https://ekzhu.com/datasketch/lsh.html) from the [Python Datasketch library](https://github.com/ekzhu/datasketch), as used by the Pile team. 
5. Construction of a database as outlined above.
  * Nothing done yet


## Pile contents

Total count: 210607728. Which contain "scientific articles"? Certainly those labeled YES.

| Source | Count | Articles? |
| --- | --- | --- |
| ArXiv             | 2377741 | Yes |
| BookCorpus2       | 25355 | |
| Books3            | 277655 | |
| DM Mathematics    | 1918535 | |
| Enron Emails      | 926132 | |
| EuroParl          | 131723 | |
| FreeLaw           | 5069088 | |
| Github            | 18044218 | |
| Gutenberg (PG-19) | 66981 | |
| HackerNews        | 1571968 | |
| NIH ExPorter      | 1777926 | |
| OpenSubtitles     | 632485 | |
| OpenWebText2      | 32333654 | |
| PhilPapers        | 63875 | |
| Pile-CC           | 52441354 | |
| PubMed Abstracts  | 29329202 | |
| PubMed Central    | 5679903 | Yes |
| StackExchange     | 29529008 | |
| USPTO Backgrounds | 11123325 | |
| Ubuntu IRC        | 20067 | |
| Wikipedia (en)    | 16939503 | |
| YoutubeSubtitles  | 328030 | |
