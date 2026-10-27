"""Synthetic integration tests; no production data or HPC access required."""
import json
import tempfile
import unittest
from pathlib import Path

import duckdb

from build_pmid_analysis_table import METRICS, EXPECTED_METRICS, SCI_REQUIRED, parser, run


class MasterTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.args = parser().parse_args([])
        self.args.output = self.root / 'output'
        self.args.citation_window_end_year = 2023
        self.con = duckdb.connect()
        self.years = {1: 1980, 2: 2018, 3: 2020, 4: 2023, 5: 1981, 6: 1982, 7: 1983, 8: 1984}
        for tag in METRICS:
            names = EXPECTED_METRICS[tag]
            cols = ','.join(c + (' VARCHAR' if c in ('PMID', 'knowledge_type_pattern') else ' DOUBLE' if c.startswith('share_') else ' BIGINT') for c in names)
            self.con.execute(f'CREATE TABLE {tag}({cols})')
            for i, yr in self.years.items():
                if tag in ('adoption', 'decomposition', 'development') and yr > 2018:
                    continue
                if tag == 'pattern' and i == 2:
                    continue
                new = 0 if i == 1 else 1
                values = {c: None if c.startswith('share_') else 0 for c in names}
                values.update(PMID=str(i), PYEAR=yr)
                known = dict(n_triples=2, n_new_triples=new, n_new_node=new,
                    n_repeated_triple=2-new, share_new_triples=new/2,
                    share_new_node=new/2, share_new_combination=0.0,
                    share_new_relation=0.0, share_repeated_triple=(2-new)/2,
                    n_unique_concepts=2, n_unique_concept_pairs=1,
                    n_unique_new_concepts=new, n_unique_new_combination_pairs=0,
                    share_unique_new_concepts=new/2, share_unique_new_combination_pairs=0.0,
                    share_new_node_among_new=1.0 if new else None,
                    share_new_combination_among_new=0.0 if new else None,
                    share_new_relation_among_new=0.0 if new else None,
                    n_new_triples_adopted=new, n_new_node_adopted=new,
                    share_new_triples_adopted=1.0 if new else None,
                    share_new_node_adopted=1.0 if new else None,
                    has_new_triples=new, has_new_node=new, n_new_knowledge_types=new,
                    knowledge_type_pattern='N' if new else 'Repeated_Only')
                values.update({c: v for c,v in known.items() if c in values})
                row = [values[c] for c in names]
                self.con.execute(f'INSERT INTO {tag} VALUES ({",".join("?" for _ in row)})', row)
            path = self.root / (tag + '.parquet')
            setattr(self.args, tag, path)
        self.con.execute('CREATE TABLE audit(pmid VARCHAR,analysis_year INTEGER,n_works INTEGER,has_work_shared_with_other_pmids BOOLEAN)')
        self.con.executemany('INSERT INTO audit VALUES (?,?,?,?)', [(str(i), yr, 2 if i == 5 else 0 if i == 6 else 1, i == 7) for i, yr in self.years.items()])
        self.con.execute('CREATE TABLE details(pmid VARCHAR,work_id VARCHAR,analysis_year INTEGER,openalex_year INTEGER,n_pmids_for_work INTEGER)')
        self.con.executemany('INSERT INTO details VALUES (?,?,?,?,?)', [(str(i), f'https://openalex.org/W{i}', yr, yr + (i == 2), 2 if i == 7 else 1) for i, yr in self.years.items() if i != 6])
        fields = []
        text = {'paperid', 'PMID', 'doi', 'date', 'doctype'}
        for c in SCI_REQUIRED:
            dtype = 'VARCHAR' if c in text else 'BOOLEAN' if c == 'is_retracted' else 'DOUBLE' if c in ('disruption', 'Atyp_Median_Z', 'Atyp_10pct_Z', 'WSB_mu', 'WSB_sigma', 'WSB_Cinf', 'SB_B') else 'BIGINT'
            fields.append(f'"{c}" {dtype}')
        self.con.execute('CREATE TABLE sci(' + ','.join(fields) + ')')
        for i, yr in self.years.items():
            if i == 8:
                continue
            values = {c: None for c in SCI_REQUIRED}
            values.update(paperid=f'W{i}', PMID=str(i), year=yr + (i == 2), C3=0, C5=2, C10=3, team_size=4, is_retracted=False)
            self.con.execute(f'INSERT INTO sci VALUES ({",".join("?" for _ in values)})', list(values.values()))
        self.args.pmid_summary = self.root / 'audit.parquet'
        self.args.work_details = self.root / 'details.parquet'
        self.args.sciscinet = self.root / 'sci.parquet'
        self.args.audit_validation = self.root / 'validation.json'
        self.args.audit_validation.write_text(json.dumps({'all_checks_passed': True, 'analysis_pmids': len(self.years)}))

    def tearDown(self):
        self.con.close()
        self.temp.cleanup()

    def save(self):
        files = {tag: getattr(self.args, tag) for tag in METRICS}
        files.update(audit=self.args.pmid_summary, details=self.args.work_details, sci=self.args.sciscinet)
        for table, path in files.items():
            self.con.execute(f"COPY {table} TO '{path}' (FORMAT PARQUET)")

    def build(self):
        self.save()
        return run(self.args)

    def output(self):
        return f"read_parquet('{self.args.output / 'pmid_analysis_table_1980_2023.parquet'}')"

    def test_master_filters_and_preserves_years_and_missingness(self):
        report = self.build()
        self.assertEqual(report['n_output_pmids'], 4)
        self.assertEqual(report['n_excluded_mapping'], 3)
        self.assertEqual(report['n_excluded_missing_sciscinet'], 1)
        rows = self.con.execute('SELECT pmid,workid FROM ' + self.output() + ' ORDER BY pmid').fetchall()
        self.assertEqual(rows, [('1', 'W1'), ('2', 'W2'), ('3', 'W3'), ('4', 'W4')])
        row = self.con.execute("SELECT pyear,openalex_year,sciscinet_year,year_match_semmed_sciscinet,eligible_c5,has_pattern_metrics FROM " + self.output() + " WHERE pmid='2'").fetchone()
        self.assertEqual(row, (2018, 2019, 2019, False, False, False))
        row = self.con.execute("SELECT share_new_triples_adopted,n_new_triples_adopted FROM " + self.output() + " WHERE pmid='1'").fetchone()
        self.assertEqual(row, (None, 0))
        row = self.con.execute("SELECT has_adoption_metrics,eligible_adoption_5y,n_new_triples_adopted FROM " + self.output() + " WHERE pmid='3'").fetchone()
        self.assertEqual(row, (False, False, None))
        self.assertTrue((self.args.output / 'variable_dictionary.csv').is_file())
        self.assertTrue((self.args.output / 'excluded_mapping_pmids.parquet').is_file())

    def test_unspecified_citation_cutoff_is_unknown(self):
        self.args.citation_window_end_year = None
        self.build()
        self.assertEqual(self.con.execute('SELECT count(*) FROM ' + self.output() + ' WHERE eligible_c3 IS NOT NULL OR eligible_c5 IS NOT NULL OR eligible_c10 IS NOT NULL').fetchone()[0], 0)

    def test_duplicate_metric_difference_fails(self):
        self.con.execute("UPDATE adoption SET n_triples=9 WHERE PMID='2'")
        with self.assertRaisesRegex(ValueError, 'Duplicate metric differs'):
            self.build()
        self.assertFalse(self.args.output.exists())

    def test_duplicate_pmid_after_normalization_fails(self):
        self.con.execute("INSERT INTO pattern SELECT '0001', * EXCLUDE(PMID) FROM pattern WHERE PMID='1'")
        with self.assertRaisesRegex(ValueError, 'duplicate pmid'):
            self.build()

    def test_sciscinet_duplicate_work_fails(self):
        self.con.execute("INSERT INTO sci SELECT * FROM sci WHERE paperid='W1'")
        with self.assertRaisesRegex(ValueError, 'sci duplicate workid'):
            self.build()

    def test_sciscinet_wrong_pmid_fails(self):
        self.con.execute("UPDATE sci SET PMID='999' WHERE paperid='W1'")
        with self.assertRaisesRegex(ValueError, 'SciSciNet PMID disagrees'):
            self.build()

    def test_mapping_reverse_cardinality_fails(self):
        self.con.execute("UPDATE details SET n_pmids_for_work=2 WHERE pmid='1'")
        with self.assertRaisesRegex(ValueError, 'Inconsistent one-to-one detail'):
            self.build()

    def test_stale_audit_year_fails(self):
        self.con.execute("UPDATE audit SET analysis_year=1981 WHERE pmid='1'")
        with self.assertRaisesRegex(ValueError, 'cohorts/years differ'):
            self.build()

    def test_missing_metric_year_fails(self):
        self.con.execute("UPDATE development SET PYEAR=1981 WHERE PMID='1'")
        with self.assertRaisesRegex(ValueError, 'PMID/year outside annotation'):
            self.build()

    def test_existing_output_protected(self):
        self.args.output.mkdir()
        (self.args.output / 'keep.txt').write_text('keep')
        with self.assertRaises(FileExistsError):
            self.build()
        self.assertEqual((self.args.output / 'keep.txt').read_text(), 'keep')

    def test_out_of_range_ratio_fails(self):
        self.con.execute("UPDATE development SET share_nn_future_nc=1.2 WHERE PMID='2'")
        with self.assertRaisesRegex(ValueError, 'invalid metric values'):
            self.build()

    def test_nonfinite_ratio_fails(self):
        self.con.execute("UPDATE development SET share_nn_future_nc='NaN'::DOUBLE WHERE PMID='2'")
        with self.assertRaisesRegex(ValueError, 'invalid metric values'):
            self.build()

    def test_negative_count_fails(self):
        self.con.execute("UPDATE development SET n_nn_future_nc=-1 WHERE PMID='2'")
        with self.assertRaisesRegex(ValueError, 'invalid metric values'):
            self.build()

    def test_fractional_sciscinet_year_fails(self):
        self.con.execute('ALTER TABLE sci ALTER COLUMN year TYPE DOUBLE')
        self.con.execute("UPDATE sci SET year=2018.5 WHERE paperid='W2'")
        with self.assertRaisesRegex(ValueError, 'Invalid SciSciNet year'):
            self.build()

    def test_missing_mapping_detail_fails(self):
        self.con.execute("DELETE FROM details WHERE pmid='1'")
        with self.assertRaisesRegex(ValueError, 'missing mapping detail'):
            self.build()

    def test_duplicate_work_across_pmids_fails(self):
        self.con.execute("UPDATE details SET work_id='W1' WHERE pmid='2'")
        with self.assertRaisesRegex(ValueError, 'duplicate workid'):
            self.build()

    def test_citation_window_boundaries(self):
        self.build()
        values = self.con.execute('SELECT pmid,eligible_c3,eligible_c5,eligible_c10 FROM ' + self.output() + ' ORDER BY pmid').fetchall()
        self.assertEqual(values, [('1',True,True,True), ('2',True,False,False), ('3',True,False,False), ('4',False,False,False)])

    def test_invalid_audit_not_accepted(self):
        self.args.audit_validation.write_text('{"all_checks_passed": false}')
        with self.assertRaisesRegex(ValueError, 'audit did not pass'):
            self.build()


if __name__ == '__main__':
    unittest.main()
