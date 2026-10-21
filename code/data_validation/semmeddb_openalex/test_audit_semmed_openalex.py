import csv
import gzip
import importlib.util
import tempfile
from pathlib import Path
import unittest
import duckdb
spec=importlib.util.spec_from_file_location('audit',Path(__file__).with_name('audit_semmed_openalex.py'))
audit=importlib.util.module_from_spec(spec);spec.loader.exec_module(audit)

class AuditTest(unittest.TestCase):
    def fixture(self,root,header=False):
        con=duckdb.connect()
        con.execute('CREATE TABLE a(PMID VARCHAR,PYEAR VARCHAR)')
        con.executemany('INSERT INTO a VALUES (?,?)',[(str(i),'2001') for i in range(1,13)]+[('1','2001'),('12','2002')])
        con.execute(f"COPY a TO '{root/'analysis.parquet'}' (FORMAT PARQUET)");con.close()
        with gzip.open(root/'citations.csv.gz','wt',newline='') as h:
            w=csv.writer(h)
            if header:w.writerow(['PMID','ISSN','DP','EDAT','PYEAR'])
            w.writerows([(str(i),'ISSN','2001 Dec','2002-1-1','2001') for i in range(1,13)])
            w.writerow(['11','ISSN','2000','2001','2000']) # conflicting SemMed year
        # 1=single/equal; 2=single/different; 3=multi/all; 4=multi/some;
        # 5=multi/none; 6=multi/incomplete; 7=unmatched; 8=missing year;
        # 9=work mapped to another PMID too; 10=invalid OA year;
        # 11=conflicting SemMed year; 12=conflicting analysis year.
        ids=[(f'W{i}',str(i),'https://doi.org/same,doi') for i in range(1,13) if i!=7]
        ids += [('W33','3','https://doi.org/same,doi'),('W44','4','different'),('W55','5','same'),('W66','6','same'),('W9','99','same'),('W1','0001','https://doi.org/same,doi'),('bad','1',''),('W100','000','')]
        with gzip.open(root/'ids.csv.gz','wt',newline='') as h:
            w=csv.writer(h);w.writerow(['work_id','pmid','doi'])
            for work,p,doi in ids:w.writerow(['https://openalex.org/'+work,'https://pubmed.ncbi.nlm.nih.gov/'+p+'/',doi])
        years=[(f'W{i}','2000' if i in [2,5] else '2001.8' if i==10 else '2001') for i in range(1,13) if i not in [7,8]]
        years += [('W33','2001'),('W44','2000'),('W55','2002'),('W66','bad'),('W1','2001')]
        with gzip.open(root/'years.csv.gz','wt',newline='') as h:
            w=csv.writer(h);w.writerow(['work_id','publication_year']);w.writerows(years)
        argv=['--aggregation-buckets','3','--analysis',str(root/'analysis.parquet'),'--citations',str(root/'citations.csv.gz'),'--ids',str(root/'ids.csv.gz'),'--years',str(root/'years.csv.gz'),'--output',str(root/'out')]
        if header:argv+=['--citations-header']
        return audit.parser().parse_args(argv)

    def test_partitions_and_counts(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);args=self.fixture(root);audit.run(args)
            c=duckdb.connect()
            rows={r[0]:r[1:] for r in c.execute(f"SELECT pmid,mapping_group,year_status,n_works FROM '{root/'out/pmid_summary.parquet'}'").fetchall()}
            self.assertEqual(len(rows),12)
            expected={'1':('single_work','all_match',1),'2':('single_work','none_match',1),'3':('multiple_works','all_match',2),'4':('multiple_works','some_match',2),'5':('multiple_works','none_match',2),'6':('multiple_works','incomplete_openalex_years',2),'7':('unmatched','unmatched',0),'8':('single_work','incomplete_openalex_years',1),'9':('single_work','all_match',1),'10':('single_work','incomplete_openalex_years',1),'11':('single_work','semmed_year_unavailable',1),'12':('single_work','all_match',1)}
            self.assertEqual(rows,expected)
            path=str(root/'out/pmid_summary.parquet')
            self.assertEqual(c.execute(f"SELECT n_matching_works,n_works_with_unavailable_year FROM '{path}' WHERE pmid='6'").fetchone(),(1,1))
            self.assertTrue(c.execute(f"SELECT has_work_shared_with_other_pmids FROM '{path}' WHERE pmid='9'").fetchone()[0])
            self.assertEqual(c.execute(f"SELECT dp_year,edat_year FROM '{path}' WHERE pmid='1'").fetchone(),(2001,2002))
            self.assertIsNone(c.execute(f"SELECT analysis_year FROM '{path}' WHERE pmid='12'").fetchone()[0])
            self.assertEqual(c.execute(f"SELECT sum(n_pmids) FROM read_csv_auto('{root/'out/mapping_year_summary.csv'}')").fetchone()[0],12)
            self.assertEqual(c.execute(f"SELECT count(*) FROM '{root/'out/pmid_work_year_details.parquet'}'").fetchone()[0],16)
            self.assertEqual(c.execute(f"SELECT n_distinct_usable_dois FROM '{path}' WHERE pmid='3'").fetchone()[0],1)
            with self.assertRaises(FileExistsError):audit.run(args)
            c.close()

    def test_headered_citations(self):
        with tempfile.TemporaryDirectory() as tmp:
            args=self.fixture(Path(tmp),header=True);audit.run(args)
            self.assertTrue((args.output/'validation_summary.json').exists())

    def test_bad_header_setting(self):
        with tempfile.TemporaryDirectory() as tmp:
            args=self.fixture(Path(tmp),header=True);args.citations_header=False
            with self.assertRaisesRegex(ValueError,'header'):audit.run(args)
            self.assertFalse(args.output.exists())

    def test_invalid_analysis(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);args=self.fixture(root)
            c=duckdb.connect();c.execute(f"COPY (SELECT '1' PMID,'2e3' PYEAR) TO '{root/'invalid.parquet'}' (FORMAT PARQUET)");c.close()
            args.analysis=root/'invalid.parquet'
            with self.assertRaisesRegex(ValueError,'invalid analysis'):audit.run(args)
            self.assertFalse(args.output.exists())

    def test_empty_mapping_tables(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);args=self.fixture(root)
            for path,columns in [(args.ids,['work_id','pmid','doi']),(args.years,['work_id','publication_year'])]:
                with gzip.open(path,'wt',newline='') as h:csv.writer(h).writerow(columns)
            audit.run(args)
            c=duckdb.connect()
            self.assertEqual(c.execute(f"SELECT count(*) FROM '{root/'out/pmid_summary.parquet'}' WHERE mapping_group='unmatched'").fetchone()[0],12)
            c.close()

    def test_missing_semmed_records(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);args=self.fixture(root)
            with gzip.open(args.citations,'wt',newline='') as h:csv.writer(h).writerow(['99','','2001','2001','2001'])
            audit.run(args)
            c=duckdb.connect()
            self.assertEqual(c.execute(f"SELECT count(*) FROM '{root/'out/pmid_summary.parquet'}' WHERE year_status='semmed_year_unavailable'").fetchone()[0],11)
            c.close()

    def test_partition_equivalence(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);args=self.fixture(root)
            args.aggregation_buckets=1
            audit.run(args)
            args.output=root/'partitioned';args.aggregation_buckets=7
            audit.run(args)
            c=duckdb.connect()
            for file in ['pmid_summary.parquet','pmid_work_year_details.parquet']:
                left=str(root/'out'/file);right=str(root/'partitioned'/file)
                self.assertEqual(c.execute(f"SELECT count(*) FROM ((SELECT * FROM '{left}' EXCEPT ALL SELECT * FROM '{right}') UNION ALL (SELECT * FROM '{right}' EXCEPT ALL SELECT * FROM '{left}'))").fetchone()[0],0)
            c.close()

if __name__=='__main__':unittest.main()
