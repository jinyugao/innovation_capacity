"""Build a one-PMID/one-Work regression master from validated paper-level inputs."""
from __future__ import annotations

import argparse
import csv
import json
import os
from pathlib import Path
import tempfile

import duckdb

PROJECT = Path(os.getenv('IC_PROJECT_ROOT', '/xdisk/sebratt/jinyugao/research/projects/innovation_capacity'))
METRICS = {
    'annotation': ('edge_annotation_metrics', 'pmid_edge_annotation_metrics_1980_2023.parquet'),
    'pattern': ('edge_annotation_metrics_pattern', 'pmid_edge_annotation_metrics_pattern_1980_2023.parquet'),
    'new_knowledge': ('edge_annotation_new_knowledge_metrics/pmid', 'pmid_edge_annotation_new_knowledge_metrics_1980_2023.parquet'),
    'adoption': ('edge_adoption_metrics/pmid', 'pmid_edge_adoption_metrics_1980_2018.parquet'),
    'development': ('future_development_metrics/pmid', 'pmid_future_development_metrics_1980_2018.parquet'),
    'decomposition': ('edge_adoption_decomposition/pmid', 'pmid_edge_adoption_decomposition_1980_2018.parquet'),
}
EXPECTED_METRICS = {'adoption': ['PMID', 'PYEAR', 'n_triples', 'n_new_node', 'n_new_combination', 'n_new_relation',
              'n_repeated_triple', 'n_new_triples', 'n_new_node_adopted', 'n_new_combination_adopted',
              'n_new_relation_adopted', 'n_new_triples_adopted', 'n_repeated_triple_continued',
              'n_triples_reappeared', 'share_new_node_adopted', 'share_new_combination_adopted',
              'share_new_relation_adopted', 'share_new_triples_adopted',
              'share_repeated_triple_continued', 'share_triples_reappeared',
              'share_adopted_new_triples_in_all', 'has_new_node_adopted', 'has_new_combination_adopted',
              'has_new_relation_adopted', 'has_new_triples_adopted', 'has_repeated_triple_continued',
              'has_triples_reappeared'],
 'annotation': ['PMID', 'PYEAR', 'n_triples', 'n_new_node', 'n_new_combination', 'n_new_relation',
                'n_repeated_triple', 'n_new_triples', 'share_new_node', 'share_new_combination',
                'share_new_relation', 'share_repeated_triple', 'share_new_triples',
                'share_new_node_among_new', 'share_new_combination_among_new',
                'share_new_relation_among_new'],
 'decomposition': ['PMID', 'PYEAR', 'n_triples', 'n_new_triples', 'n_new_triples_adopted',
                   'share_new_triples', 'share_new_triples_adopted',
                   'share_adopted_new_triples_in_all'],
 'development': ['PMID', 'PYEAR', 'n_new_node', 'n_new_combination', 'n_unique_new_combination_pairs',
                 'n_nn_future_nc', 'share_nn_future_nc', 'has_nn_future_nc', 'n_nn_future_nr',
                 'share_nn_future_nr', 'has_nn_future_nr', 'n_nn_future_any', 'share_nn_future_any',
                 'has_nn_future_any', 'n_nc_future_nr', 'share_nc_future_nr', 'has_nc_future_nr',
                 'n_nn_dev_neither', 'share_nn_dev_neither', 'n_nn_dev_nc_only', 'share_nn_dev_nc_only',
                 'n_nn_dev_nr_only', 'share_nn_dev_nr_only', 'n_nn_dev_both', 'share_nn_dev_both',
                 'n_nn_neither_adopted_nor_developed', 'share_nn_neither_adopted_nor_developed',
                 'n_nn_adopted_only', 'share_nn_adopted_only', 'n_nn_developed_only',
                 'share_nn_developed_only', 'n_nn_adopted_and_developed',
                 'share_nn_adopted_and_developed', 'n_nc_neither_adopted_nor_developed',
                 'share_nc_neither_adopted_nor_developed', 'n_nc_adopted_only', 'share_nc_adopted_only',
                 'n_nc_developed_only', 'share_nc_developed_only', 'n_nc_adopted_and_developed',
                 'share_nc_adopted_and_developed', 'n_nc_pairs_future_nr', 'share_nc_pairs_future_nr',
                 'has_nc_pairs_future_nr'],
 'new_knowledge': ['PMID', 'PYEAR', 'n_unique_concepts', 'n_unique_new_concepts',
                   'share_unique_new_concepts', 'n_unique_concept_pairs',
                   'n_unique_new_combination_pairs', 'share_unique_new_combination_pairs', 'n_triples',
                   'n_new_node', 'n_new_combination', 'n_new_relation', 'n_repeated_triple',
                   'n_new_triples', 'share_new_node', 'share_new_combination', 'share_new_relation',
                   'share_repeated_triple', 'share_new_triples'],
 'pattern': ['PMID', 'PYEAR', 'has_new_node', 'has_new_combination', 'has_new_relation',
             'has_new_triples', 'n_new_knowledge_types', 'knowledge_type_pattern']}
SCI_REQUIRED = '''paperid doi year date doctype cited_by_count is_retracted reference_count
citation_count C3 C5 C10 disruption Atyp_Median_Z Atyp_10pct_Z Atyp_Pairs WSB_mu WSB_sigma
WSB_Cinf SB_B SB_T team_size institution_count patent_count newsfeed_count nct_count nih_count nsf_count PMID'''.split()


def q(value):
    return "'" + str(value).replace("'", "''") + "'"


def ident(value):
    return '"' + value.replace('"', '""') + '"'


def pmid(expr):
    value = f"nullif(regexp_extract(trim(CAST({expr} AS VARCHAR)), '^(?:https?://(?:www[.])?ncbi[.]nlm[.]nih[.]gov/pubmed/|https?://pubmed[.]ncbi[.]nlm[.]nih[.]gov/)?([0-9]+)/?$', 1), '')"
    return f"CASE WHEN regexp_matches({value}, '[1-9]') THEN ltrim({value}, '0') END"


def work(expr):
    return f"nullif(regexp_extract(trim(CAST({expr} AS VARCHAR)), '^(?:https?://openalex[.]org/)?(W[0-9]+)/?$', 1), '')"


def signature(path):
    stat = path.stat()
    return {'bytes': stat.st_size, 'mtime_ns': stat.st_mtime_ns}


def metric_definition(column):
    """English definitions for the existing upstream metrics; no renaming of concepts."""
    counts = {
        'n_triples': 'Distinct directed subject-predicate-object triples within the PMID',
        'n_new_node': 'New Node triples within the PMID',
        'n_new_combination': 'New Combination triples within the PMID',
        'n_new_relation': 'New Relation triples within the PMID',
        'n_repeated_triple': 'Repeated Triple triples within the PMID',
        'n_new_triples': 'New Node + New Combination + New Relation triple counts',
        'n_unique_concepts': 'Distinct endpoint concepts within the PMID',
        'n_unique_new_concepts': 'Distinct endpoint concepts absent from the preceding five-year reference window',
        'n_unique_concept_pairs': 'Distinct undirected concept pairs within the PMID, ignoring predicate',
        'n_unique_new_combination_pairs': 'Distinct undirected pairs classified as New Combination within the PMID',
        'n_new_knowledge_types': 'Number of New Node, New Combination and New Relation types present (0-3)',
        'n_new_node_adopted': 'Focal New Node triples recurring exactly in the following five years',
        'n_new_combination_adopted': 'Focal New Combination triples recurring exactly in the following five years',
        'n_new_relation_adopted': 'Focal New Relation triples recurring exactly in the following five years',
        'n_new_triples_adopted': 'Focal new triples recurring exactly in the following five years',
        'n_repeated_triple_continued': 'Focal Repeated Triple triples recurring exactly in the following five years',
        'n_triples_reappeared': 'All focal triples recurring exactly in the following five years',
        'n_nn_future_nc': 'Focal New Node triples with at least one new endpoint in future New Combination records',
        'n_nn_future_nr': 'Focal New Node triples with at least one new endpoint in future New Relation records',
        'n_nn_future_any': 'Focal New Node triples meeting either future development condition (union)',
        'n_nc_future_nr': 'Focal New Combination triples whose undirected pair appears in future New Relation records',
        'n_nc_pairs_future_nr': 'Distinct focal New Combination undirected pairs appearing in future New Relation records',
    }
    for prefix, label in [('nn', 'New Node'), ('nc', 'New Combination')]:
        for suffix, text in {
            'neither_adopted_nor_developed': 'neither recurring exactly nor meeting a future development condition',
            'adopted_only': 'recurring exactly without future development',
            'developed_only': 'meeting a future development condition without exact recurrence',
            'adopted_and_developed': 'both recurring exactly and meeting a future development condition',
        }.items():
            counts[f'n_{prefix}_{suffix}'] = f'Focal {label} triples {text} in the following five years'
    for suffix, text in {
        'neither': 'neither New Combination nor New Relation development',
        'nc_only': 'New Combination development only',
        'nr_only': 'New Relation development only',
        'both': 'both development paths (possibly involving different new endpoints)',
    }.items():
        counts['n_nn_dev_' + suffix] = 'Focal New Node triples with ' + text + ' in the following five years'
    ratios = {
        'share_unique_new_concepts': ('n_unique_new_concepts', 'n_unique_concepts'),
        'share_unique_new_combination_pairs': ('n_unique_new_combination_pairs', 'n_unique_concept_pairs'),
        'share_adopted_new_triples_in_all': ('n_new_triples_adopted', 'n_triples'),
    }
    for stem in ('new_node', 'new_combination', 'new_relation', 'repeated_triple', 'new_triples'):
        ratios['share_' + stem] = ('n_' + stem, 'n_triples')
    for stem in ('new_node', 'new_combination', 'new_relation'):
        ratios['share_' + stem + '_among_new'] = ('n_' + stem, 'n_new_triples')
    for stem in ('new_node', 'new_combination', 'new_relation', 'new_triples'):
        ratios['share_' + stem + '_adopted'] = ('n_' + stem + '_adopted', 'n_' + stem)
    ratios['share_repeated_triple_continued'] = ('n_repeated_triple_continued', 'n_repeated_triple')
    ratios['share_triples_reappeared'] = ('n_triples_reappeared', 'n_triples')
    for key in counts:
        if key.startswith(('n_nn_', 'n_nc_')):
            denominator = 'n_new_node' if key.startswith('n_nn_') else 'n_unique_new_combination_pairs' if key == 'n_nc_pairs_future_nr' else 'n_new_combination'
            ratios['share_' + key[2:]] = (key, denominator)
    if column in ratios:
        numerator, denominator = ratios[column]
        return f'{numerator} / {denominator}', denominator
    if column in counts:
        return counts[column], ''
    if column.startswith('has_') and 'n_' + column[4:] in counts:
        return '0/1 indicator: ' + 'n_' + column[4:] + ' > 0', ''
    if column == 'knowledge_type_pattern':
        return 'Nominal type pattern: Repeated_Only, N, C, R, N+C, N+R, C+R or N+C+R; repeated triples may accompany new types', ''
    return 'Additional upstream metric; consult source module README: ' + column, ''


def run(args):
    paths = {tag: getattr(args, tag) for tag in METRICS}
    paths.update({tag: getattr(args, tag) for tag in ('pmid_summary', 'work_details', 'audit_validation', 'sciscinet')})
    for path in paths.values():
        if not path.is_file():
            raise FileNotFoundError(path)
    if args.citation_window_end_year is not None and not 1900 <= args.citation_window_end_year <= 2100:
        raise ValueError('Invalid citation-window-end-year')
    audit = json.loads(args.audit_validation.read_text())
    if audit.get('all_checks_passed') is not True:
        raise ValueError('OpenAlex mapping audit did not pass')
    signatures = {str(p.resolve()): signature(p) for p in paths.values()}
    if args.output.exists():
        raise FileExistsError(f'Output already exists: {args.output}; choose a new directory for a new version')
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix='pmid_analysis_', dir=args.output.parent) as temp:
        stage = Path(temp) / 'results'
        stage.mkdir()
        with duckdb.connect(str(Path(temp) / 'build.duckdb')) as con:
            con.execute(f"SET threads={int(os.getenv('IC_DUCKDB_THREADS', '4'))}")
            con.execute(f"SET memory_limit={q(os.getenv('IC_DUCKDB_MEMORY_LIMIT', '48GB'))}")
            con.execute(f"SET temp_directory={q(Path(temp) / 'spill')}")
            con.execute('SET preserve_insertion_order=false')
            def n(sql):
                return con.execute(sql).fetchone()[0]
            def reject(sql, message):
                bad = n(sql)
                if bad:
                    raise ValueError(f'{message}: {bad:,}')
            def schema(table):
                return dict((r[0], r[1]) for r in con.execute(f'DESCRIBE {table}').fetchall())
            def require(table, columns):
                missing = set(columns) - schema(table).keys()
                if missing:
                    raise ValueError(f'{table} missing columns: {sorted(missing)}')
            def export(sql, filename):
                fmt = 'PARQUET, COMPRESSION ZSTD' if filename.endswith('.parquet') else 'CSV, HEADER TRUE'
                con.execute(f'COPY ({sql}) TO {q(stage / filename)} (FORMAT {fmt})')
            def unique(table, key):
                reject(f'SELECT count(*) FROM {table} WHERE {ident(key)} IS NULL', f'{table} null {key}')
                reject(f'SELECT count(*) FROM (SELECT {ident(key)} FROM {table} GROUP BY 1 HAVING count(*)>1)', f'{table} duplicate {key}')

            print('1/5: checking six metric tables and cross-table definitions', flush=True)
            metric_schemas = {}
            for tag in METRICS:
                con.execute(f'CREATE VIEW raw_{tag} AS SELECT * FROM read_parquet({q(paths[tag])})')
                require('raw_' + tag, EXPECTED_METRICS[tag])
                con.execute(f'''CREATE TABLE {tag} AS SELECT {pmid('PMID')} pmid,
                    CASE WHEN regexp_full_match(trim(CAST(PYEAR AS VARCHAR)), '[12][0-9]{{3}}') THEN try_cast(PYEAR AS INTEGER) END pyear, * EXCLUDE(PMID,PYEAR) FROM raw_{tag}''')
                unique(tag, 'pmid')
                end = 2018 if tag in ('adoption', 'development', 'decomposition') else 2023
                reject(f'SELECT count(*) FROM {tag} WHERE pyear IS NULL OR pyear<1980 OR pyear>{end}', f'{tag} invalid year')
                metric_schemas[tag] = schema(tag)
                conditions = []
                for col in metric_schemas[tag]:
                    value = ident(col)
                    if col.startswith('n_'):
                        conditions.append(f'{value} IS NULL OR NOT isfinite({value}) OR {value}<0 OR {value}<>floor({value})')
                    elif col.startswith('share_'):
                        conditions.append(f'({value} IS NOT NULL AND (NOT isfinite({value}) OR {value}<0 OR {value}>1))')
                    elif col.startswith('has_'):
                        conditions.append(f'{value} IS NULL OR {value} NOT IN (0,1)')
                if conditions:
                    reject(f'SELECT count(*) FROM {tag} WHERE ' + ' OR '.join(conditions), f'{tag} invalid metric values')
                if tag != 'annotation':
                    reject(f'SELECT count(*) FROM {tag} t LEFT JOIN annotation a USING(pmid) WHERE a.pmid IS NULL OR t.pyear<>a.pyear', f'{tag} PMID/year outside annotation')
            if not n('SELECT count(*) FROM annotation'):
                raise ValueError('Empty annotation cohort')
            owners = {}
            duplicate_checks = []
            comparison_groups = {}
            for tag, columns in metric_schemas.items():
                for col, dtype in columns.items():
                    if col in ('pmid', 'pyear'):
                        continue
                    if col not in owners:
                        owners[col] = tag
                        continue
                    previous = owners[col]
                    numeric = any(x in (dtype + metric_schemas[previous][col]) for x in ('DOUBLE', 'FLOAT', 'DECIMAL'))
                    a, b = f'a.{ident(col)}', f'b.{ident(col)}'
                    compare = f'(({a} IS NULL)<>({b} IS NULL) OR ({a} IS NOT NULL AND {b} IS NOT NULL AND (NOT isfinite({a}) OR NOT isfinite({b}) OR abs({a}-{b})>1e-9)))' if numeric else f'{a} IS DISTINCT FROM {b}'
                    comparison_groups.setdefault((previous, tag), []).append((col, compare))
                    duplicate_checks.append({'column': col, 'retained_source': previous, 'checked_source': tag})

            # Compare all shared fields for each source pair in one join, rather
            # than rebuilding a full PMID hash join for every individual field.
            for (previous, tag), checks in comparison_groups.items():
                condition = ' OR '.join('(' + expr + ')' for _, expr in checks)
                reject(f'SELECT count(*) FROM {previous} a JOIN {tag} b USING(pmid) WHERE {condition}',
                       f'Duplicate metric differs: {previous}/{tag} ({", ".join(c for c, _ in checks)})')

            print('2/5: selecting globally one-to-one PMID/Work mappings', flush=True)
            con.execute(f'CREATE VIEW audit_raw AS SELECT * FROM read_parquet({q(args.pmid_summary)})')
            require('audit_raw', ['pmid', 'analysis_year', 'n_works', 'has_work_shared_with_other_pmids'])
            con.execute(f'''CREATE TABLE audit AS SELECT {pmid('pmid')} pmid,analysis_year,n_works,
                has_work_shared_with_other_pmids FROM audit_raw''')
            unique('audit', 'pmid')
            reject('SELECT count(*) FROM audit WHERE n_works IS NULL OR n_works<0 OR has_work_shared_with_other_pmids IS NULL', 'Invalid audit cardinalities')
            reject('SELECT count(*) FROM annotation a FULL JOIN audit m USING(pmid) WHERE a.pmid IS NULL OR m.pmid IS NULL OR a.pyear IS DISTINCT FROM m.analysis_year', 'Audit and annotation cohorts/years differ; rerun audit')
            if audit.get('analysis_pmids') != n('SELECT count(*) FROM audit'):
                raise ValueError('Audit validation count differs from audit table')
            con.execute('CREATE TABLE eligible AS SELECT a.pmid,a.pyear FROM annotation a JOIN audit m USING(pmid) WHERE m.n_works=1 AND NOT m.has_work_shared_with_other_pmids')
            con.execute(f'CREATE VIEW details_raw AS SELECT * FROM read_parquet({q(args.work_details)})')
            require('details_raw', ['pmid', 'work_id', 'n_pmids_for_work', 'analysis_year', 'openalex_year'])
            con.execute(f'''CREATE TABLE mapping AS SELECT {pmid('d.pmid')} pmid,{work('d.work_id')} workid,
                d.analysis_year,d.openalex_year,d.n_pmids_for_work FROM details_raw d
                SEMI JOIN eligible e ON {pmid('d.pmid')}=e.pmid''')
            reject("SELECT count(*) FROM mapping WHERE openalex_year IS NOT NULL AND NOT regexp_full_match(trim(CAST(openalex_year AS VARCHAR)), '[12][0-9]{3}')", 'Invalid OpenAlex year')
            unique('mapping', 'pmid')
            unique('mapping', 'workid')
            reject('SELECT count(*) FROM mapping m JOIN eligible e USING(pmid) WHERE m.analysis_year IS DISTINCT FROM e.pyear OR m.n_pmids_for_work IS DISTINCT FROM 1', 'Inconsistent one-to-one detail')
            reject('SELECT count(*) FROM eligible e ANTI JOIN mapping m USING(pmid)', 'Eligible PMID missing mapping detail')
            export('''SELECT a.pmid,a.pyear,m.n_works,
                CASE WHEN m.n_works=0 THEN 'no_openalex_mapping'
                WHEN m.n_works<>1 THEN 'multiple_workids'
                ELSE 'workid_shared_with_other_pmids' END reason
                FROM annotation a JOIN audit m USING(pmid)
                WHERE m.n_works<>1 OR m.has_work_shared_with_other_pmids''', 'excluded_mapping_pmids.parquet')

            print('3/5: matching SciSciNet by Work ID', flush=True)
            con.execute(f'CREATE VIEW sci_raw AS SELECT * FROM read_parquet({q(args.sciscinet)})')
            require('sci_raw', SCI_REQUIRED)
            sci_fields = [c for c in schema('sci_raw') if c.lower() not in ('paperid', 'pmid')]
            # Read only selected works, but retain every source field. Duplicate records fail,
            # including identical duplicates: never select or average an arbitrary row.
            con.execute(f'''CREATE TABLE sci AS SELECT {work('s.paperid')} workid,
                {pmid('s.PMID')} mapped_pmid, s.* EXCLUDE(paperid,PMID)
                FROM sci_raw s SEMI JOIN mapping m ON {work('s.paperid')}=m.workid''')
            unique('sci', 'workid')
            reject('SELECT count(*) FROM sci s JOIN mapping m USING(workid) WHERE s.mapped_pmid IS DISTINCT FROM m.pmid', 'SciSciNet PMID disagrees with audited mapping')
            reject("SELECT count(*) FROM sci WHERE year IS NOT NULL AND NOT regexp_full_match(trim(CAST(year AS VARCHAR)), '[12][0-9]{3}')", 'Invalid SciSciNet year')
            export('SELECT m.pmid,m.workid,m.analysis_year FROM mapping m ANTI JOIN sci s USING(workid)', 'excluded_missing_sciscinet.parquet')
            con.execute('CREATE TABLE selected AS SELECT m.* FROM mapping m JOIN sci s USING(workid)')
            if not n('SELECT count(*) FROM selected'):
                raise ValueError('No matched one-to-one papers')

            print('4/5: writing master and variable dictionary', flush=True)
            fields = ['a.pmid', 'm.workid', 'a.pyear', 'm.openalex_year']
            dictionary = []
            def add(name, source, definition, missing='NULL means unavailable; zero is retained as observed.', denominator='', coverage='1980-2023 research years'):
                dictionary.append(dict(variable=name, source=source, definition=definition, denominator=denominator, coverage=coverage, missing_rule=missing))
            for name, definition in [('pmid', 'Canonical PMID, unique in this table'), ('workid', 'Canonical OpenAlex W identifier, globally one-to-one with PMID'), ('pyear', 'Research year used by the six metrics tables'), ('openalex_year', 'Publication year from audited OpenAlex year file')]:
                add(name, 'annotation / OpenAlex audit', definition)
            for col in sci_fields:
                name = 'sciscinet_year' if col == 'year' else 'sciscinet_date' if col == 'date' else 'sci_' + col
                fields.append(f's.{ident(col)} AS {ident(name)}')
                add(name, 'SciSciNet: ' + col, 'Original SciSciNet field ' + col + '; source values retained without imputation. See SciSciNet documentation for metric definitions.')
            for col, tag in owners.items():
                fields.append(f'{tag}.{ident(col)} AS {ident(col)}')
                sources = [t for t, cols in metric_schemas.items() if col in cols]
                definition, denominator = metric_definition(col)
                coverage = '1980-2018 research years' if tag in ('adoption', 'development', 'decomposition') else '1980-2023 research years'
                add(col, '; '.join(sources), definition, 'NULL preserves upstream undefined/missing values, including zero-denominator ratios; NULL also indicates an unmatched source table. See has_*_metrics flags.', denominator, coverage)
            extra = {
                'year_match_semmed_openalex': 'a.pyear=m.openalex_year',
                'year_match_semmed_sciscinet': 'a.pyear=s.year',
                'year_match_openalex_sciscinet': 'm.openalex_year=s.year',
                'year_diff_openalex_semmed': 'm.openalex_year-a.pyear',
                'year_diff_sciscinet_semmed': 's.year-a.pyear',
                'eligible_adoption_5y': 'a.pyear BETWEEN 1980 AND 2018',
                'has_new_triples': 'a.n_new_triples>0',
            }
            # has_new_triples already exists upstream as 0/1; retain its original type.
            if 'has_new_triples' in owners:
                del extra['has_new_triples']
            for tag in METRICS:
                extra[f'has_{tag}_metrics'] = f'{tag}.pmid IS NOT NULL'
            for window in (3, 5, 10):
                extra[f'eligible_c{window}'] = 'NULL::BOOLEAN' if args.citation_window_end_year is None else f's.year+{window}<={args.citation_window_end_year}'
            for name, expr in extra.items():
                fields.append(f'{expr} AS {ident(name)}')
                if name.startswith('eligible_c'):
                    meaning = f'Full citation-window availability based on SciSciNet year and explicitly supplied cutoff {args.citation_window_end_year}; not a test of nonmissing citation count.'
                else:
                    meaning = 'Derived SQL definition: ' + expr
                add(name, 'analysis build', meaning, 'NULL if a required year is unavailable; citation eligibility is NULL for all rows when cutoff is unspecified. Source-presence flags are always true/false.')
            names = [d['variable'].lower() for d in dictionary]
            if len(names) != len(set(names)):
                raise ValueError('Output column naming collision')
            # annotation is alias a; other tables retain their tag as alias.
            fields = [f.replace('annotation.', 'a.') for f in fields]
            joins = ' '.join(f'LEFT JOIN {tag} ON a.pmid={tag}.pmid AND a.pyear={tag}.pyear' for tag in METRICS if tag != 'annotation')
            query = 'SELECT ' + ','.join(fields) + ' FROM selected m JOIN annotation a USING(pmid) JOIN sci s ON m.workid=s.workid ' + joins
            export(query, 'pmid_analysis_table_1980_2023.parquet')
            con.execute(f'CREATE VIEW master AS SELECT * FROM read_parquet({q(stage / "pmid_analysis_table_1980_2023.parquet")})')
            unique('master', 'pmid')
            unique('master', 'workid')
            if n('SELECT count(*) FROM master') != n('SELECT count(*) FROM selected'):
                raise ValueError('Join changed selected paper count')
            master_schema = schema('master')
            for entry in dictionary:
                entry['dtype'] = master_schema[entry['variable']]
            with (stage / 'variable_dictionary.csv').open('w', newline='') as handle:
                writer = csv.DictWriter(handle, fieldnames=list(dictionary[0]))
                writer.writeheader()
                writer.writerows(dictionary)
            # Batch to avoid rescanning a wide Parquet once per column.
            miss_expr = ','.join(f'count(*) FILTER(WHERE {ident(c)} IS NULL) AS {ident(c)}' for c in schema('master'))
            missing_values = con.execute('SELECT ' + miss_expr + ' FROM master').fetchone()
            with (stage / 'missingness.csv').open('w', newline='') as handle:
                writer = csv.writer(handle)
                writer.writerow(['variable', 'n_rows', 'n_missing'])
                rows = n('SELECT count(*) FROM master')
                writer.writerows((c, rows, val) for c, val in zip(schema('master'), missing_values))
            export('''SELECT pyear,count(*) n_pmids,
                count(*) FILTER(WHERE year_match_semmed_openalex) n_openalex_year_equal,
                count(*) FILTER(WHERE year_match_semmed_sciscinet) n_sciscinet_year_equal,
                count(*) FILTER(WHERE has_adoption_metrics) n_adoption_rows,
                count(*) FILTER(WHERE has_development_metrics) n_development_rows,
                count(*) FILTER(WHERE sci_C3 IS NOT NULL) n_nonmissing_c3,
                count(*) FILTER(WHERE sci_C5 IS NOT NULL) n_nonmissing_c5,
                count(*) FILTER(WHERE sci_C10 IS NOT NULL) n_nonmissing_c10
                FROM master GROUP BY pyear ORDER BY pyear''', 'summary_by_year.csv')
            print('5/5: final row counts, input stability and publication', flush=True)
            counts = {
                'n_annotation_pmids': n('SELECT count(*) FROM annotation'),
                'n_one_to_one_pmids': n('SELECT count(*) FROM eligible'),
                'n_excluded_mapping': n('SELECT count(*) FROM annotation') - n('SELECT count(*) FROM eligible'),
                'n_excluded_missing_sciscinet': n('SELECT count(*) FROM mapping') - n('SELECT count(*) FROM selected'),
                'n_output_pmids': rows,
                'n_output_columns': len(schema('master')),
            }
            for path in paths.values():
                if signature(path) != signatures[str(path.resolve())]:
                    raise ValueError(f'Input changed while building: {path}')
            report = dict(**counts, citation_window_end_year=args.citation_window_end_year,
                citation_eligibility_year_basis='sciscinet_year', adoption_window_end_year=2023,
                year_mismatches_retained=True, duplicate_metric_checks=duplicate_checks,
                inputs={k: str(v) for k, v in paths.items()}, input_signatures=signatures,
                all_checks_passed=True)
            (stage / 'validation_summary.json').write_text(json.dumps(report, indent=2) + '\n')
            (stage / 'sample_flow.json').write_text(json.dumps(counts, indent=2) + '\n')
        # Publish the complete directory only after all validations and input checks pass.
        stage.replace(args.output)
    print(json.dumps(counts, indent=2), flush=True)
    print(f'All checks passed. Output: {args.output}', flush=True)
    return report


def parser():
    p = argparse.ArgumentParser(description=__doc__)
    base = PROJECT / 'data/processed/knowledge_combination'
    for tag, (subdir, filename) in METRICS.items():
        p.add_argument('--' + tag.replace('_', '-'), type=Path, default=base / subdir / filename)
    audit = PROJECT / 'data/processed/data_validation/semmeddb_openalex'
    p.add_argument('--pmid-summary', type=Path, default=audit / 'pmid_summary.parquet')
    p.add_argument('--work-details', type=Path, default=audit / 'pmid_work_year_details.parquet')
    p.add_argument('--audit-validation', type=Path, default=audit / 'validation_summary.json')
    p.add_argument('--sciscinet', type=Path, default=PROJECT / 'data/processed/sciscinet_v2/sciscinet_papers_pmid.parquet')
    p.add_argument('--citation-window-end-year', type=int, default=None,
                   help='Verified last fully covered citation year; omit to retain NULL eligibility flags')
    p.add_argument('--output', type=Path, default=PROJECT / 'data/regression/pmid_analysis_table')
    return p


if __name__ == '__main__':
    run(parser().parse_args())
