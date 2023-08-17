# TPC data-general-text-code-web group

![Image showing a lot of books](books.png)

## A proposed initial goal (see also [slides from August hackathon](https://anl.app.box.com/s/qryy4sdsvd5joytulvkivetro19s72hy/file/1276828321799))

Assemble a large, de-duplicated collection of scientific articles for use in LLM training, with for each the extracted text (initially; later, also figures, entities, citation linkages, etc.), each with descriptive metadata.

Initially, we may maintain these data on the Eagle file system at ALCF. (Of course, others may want to create copies elsewhere as well--but note that not all sources allow the creation of derivative copies.)

Document contents of this collection in a database that details something like, (DOI-if-available, metadata, source, location(s)-on-tpc-storage, info about duplicates).

Potential sources for articles include the Pile, Arxiv, DOE OSTI, PubMed, BioArxiv, etc. Different sources may have data in different formats and duplicates. Not all record DOIs or have accurate metadata. 

## Some potential first steps

To support this work, we are engaged in the following activities:

1. Development of a [curated list of potential data sources](https://docs.google.com/spreadsheets/d/1cGTAsrWMd2pLtYEi8W432SODt6RVM14YJPEsPhvq6uA/edit#gid=0) with details on how to access eacb.
2. Assembly of articles from different sources, placing them at ALCF at `/lus/eagle/projects/tpc/Text`. So far we have:
    * [The Pile](https://pile.eleuther.ai), in JSONL format, at `/lus/eagle/projects/tpc/Text/jsonl_pile` [there is a binary format version elsewhere). Note that only part of this is scientific articles: see below.
    * [Biorxiv](https://www.biorxiv.org/tdm): download in progress, to be at `/lus/eagle/projects/tpc/Text/biorxiv`
    * [Medrxiv](https://www.medrxiv.org/tdm): download in progress to `/lus/eagle/projects/tpc/Text/medrxiv`
3. Extraction of text from PDF articles, when not available in other formats. We are working with two methods that we should compare:
    * [Grobid](https://grobid.readthedocs.io/en/latest/) to produce XML, and then simple extraction of text from XML. (Ian has code.)
    * Andrew McNaughton has been using [PyPDF2](https://pypi.org/project/PyPDF2/) and PyMuPDF (https://pymupdf.readthedocs.io/en/latest/)
4. Evaluation of similarity/de-duplication
    * We are experimenting with [MinhashLSH](https://ekzhu.com/datasketch/lsh.html) from the [Python Datasketch library](https://github.com/ekzhu/datasketch), as used by the Pile team.
    * See also the [RefinedWeb pipeline](https://arxiv.org/pdf/2306.01116.pdf), which uses MinHash but also a variety of other methods.
5. Construction of a database as outlined above.
    * Nothing done yet


## Pile contents

Total count: 210607728. Which contain "scientific articles"? Certainly those labeled YES. Others?

<center>

| Source | Count | Articles? | Notes |
| --- | --- | --- | --- |
| ArXiv             | 2377741 | Yes | Mostly math, CS, physics |
| BookCorpus2       | 25355 | | |
| Books3            | 277655 | | |
| DM Mathematics    | 1918535 | | |
| Enron Emails      | 926132 | | |
| EuroParl          | 131723 | | |
| FreeLaw           | 5069088 | | Legal opinions |
| Github            | 18044218 | | |
| Gutenberg (PG-19) | 66981 | | |
| HackerNews        | 1571968 | | |
| NIH ExPorter      | 1777926 | | NIH awarded applications, 1985- present |
| OpenSubtitles     | 632485 | | |
| OpenWebText2      | 32333654 | | |
| PhilPapers        | 63875 | | Open-access philosophy publications |
| Pile-CC           | 52441354 | | |
| PubMed Abstracts  | 29329202 | | 30M pubmed abstracts |
| PubMed Central    | 5679903 | Yes | According to [NCBI web site](https://www.ncbi.nlm.nih.gov/pmc/about/intro/), PMC had 8,386,512 full-text articles as of 2022 |
| StackExchange     | 29529008 | | |
| USPTO Backgrounds | 11123325 | | Background sections of US patents |
| Ubuntu IRC        | 20067 | | |
| Wikipedia (en)    | 16939503 | | |
| YoutubeSubtitles  | 328030 | | |

</center>
