# TPC data-general-text-code-web group

![Image showing a lot of books](books.png)

## A proposed initial goal (see also [slides from August hackathon](https://anl.app.box.com/s/qryy4sdsvd5joytulvkivetro19s72hy/file/1276828321799))

Assemble a large, de-duplicated collection of scientific articles for use in LLM training, with for each the extracted text (initially; later, also figures, entities, citation linkages, etc.), each with descriptive metadata.

Document contents of this collection in a database that details something like, (DOI-if-available, metadata, source, location(s)-on-tpc-storage, info about duplicates).

Potential sources for articles include the Pile, Arxiv, DOE OSTI, PubMed, BioArxiv, etc. Different sources may have data in different formats and duplicates. Not all record DOIs or have accurate metadata. Many but not all of these datasets can be retrieved from public sources, but generally they cannot be redistribited.

## Some potential first steps

To support this work, we are engaged in the following activities:

1. Development of a [curated list of potential data sources](https://docs.google.com/spreadsheets/d/1cGTAsrWMd2pLtYEi8W432SODt6RVM14YJPEsPhvq6uA/edit#gid=0) with details on how to access eacb.
2. Assembly of articles from different sources. E.g., the Argonne group is assembling some datasets at ALCF.
3. Extraction of text from PDF articles, when not available in other formats. The following are some methods that can be considered.
    * [Grobid](https://grobid.readthedocs.io/en/latest/) to produce XML, and then simple extraction of text from XML. 
    * Andrew McNaughton has been using [PyPDF2](https://pypi.org/project/PyPDF2/) and PyMuPDF (https://pymupdf.readthedocs.io/en/latest/)
    * Marker for extraction as markdown.
4. Evaluation of similarity/de-duplication
    * We are experimenting with [MinhashLSH](https://ekzhu.com/datasketch/lsh.html) from the [Python Datasketch library](https://github.com/ekzhu/datasketch), as used by the Pile team: see [code assembled by Hong Zhi](https://github.com/TPC-AI/data-general-text-code-web/tree/main/deduplication).
    * See also the [RefinedWeb pipeline](https://arxiv.org/pdf/2306.01116.pdf), which uses MinHash but also a variety of other methods.
