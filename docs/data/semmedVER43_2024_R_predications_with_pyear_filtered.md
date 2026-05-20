# Variable Dictionary

| Variable | Type | Description |
|---|---|---|
| PREDICATION_ID | string | Auto-generated primary key for each unique predication. |
| SENTENCE_ID | string | Foreign key to the SENTENCE table. |
| PMID | string | PubMed identifier of the citation to which the predication belongs. |
| PREDICATE | string | String representation of the predicate, for example `TREATS` or `PROCESS_OF`. |
| SUBJECT_CUI | string | Source CUI field for the subject of the predication. Values may contain multiple pipe-delimited CUIs. |
| SUBJECT_NAME | string | Preferred name of the subject of the predication. |
| SUBJECT_SEMTYPE | string | Semantic type of the subject of the predication. |
| SUBJECT_NOVELTY | numeric | Novelty of the subject of the predication. Filtered to non-missing and non-zero values. |
| OBJECT_CUI | string | Source CUI field for the object of the predication. Values may contain multiple pipe-delimited CUIs. |
| OBJECT_NAME | string | Preferred name of the object of the predication. |
| OBJECT_SEMTYPE | string | Semantic type of the object of the predication. |
| OBJECT_NOVELTY | numeric | Novelty of the object of the predication. Filtered to non-missing and non-zero values. |
| PYEAR | string | Completion year metadata from the CITATIONS table, appended by matching on `PMID`. |
| subject_cui_primary | string | First CUI listed in `SUBJECT_CUI`, split at the pipe delimiter. |
| object_cui_primary | string | First CUI listed in `OBJECT_CUI`, split at the pipe delimiter. |
