# data-general-text-code-web
Material relating to data-general-text-code-web

The **data-general-text-code-web** group in TPC 

## Scientific articles

We want to assemble a de-duplicated collection of scientific articles, with for each the extracted text. (Ultimately figures and other content may also be wanted.) Our initial plan is to maintain these data on the Eagle file system at ALCF, but of course others may want to create copies elsewhere as well. [Note that not all sources allow the creation of derivative copies.]

Potential sources for articles include the Pile, Arxiv, DOE OSTI, PubMed, BioArxiv, etc. Different sources may have data in different formats and duplicates. Not all record DOIs or have accurate metadata. 

We want to create a database that details something like, (DOI-if-available, metadata, source, location(s)-on-tpc-storage, info about duplicates).

To support this work, we are engaged in the following activities:

* Development of a [curated list of potential data sources](https://docs.google.com/spreadsheets/d/1cGTAsrWMd2pLtYEi8W432SODt6RVM14YJPEsPhvq6uA/edit#gid=0) with details on how to access eacb.
* Assembly of articles from different sources, placing them at ALCF. So far we have:
  * The Pile, in JSONL format, at /lus/eagle/projects/tpc/Text/jsonl_pile [there is a binary format version elsewhere)
  * Biorxiv: download in progress, to be at /lus/eagle/projects/tpc/Text/biorxiv
* Extraction of text from PDF articles, when not available in other formats.

* 
