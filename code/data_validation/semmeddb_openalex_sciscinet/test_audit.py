import csv
import gzip
import importlib.util
from pathlib import Path
import tempfile
import unittest
import duckdb
spec=importlib.util.spec_from_file_location('audit',Path(__file__).with_name('audit_semmed_openalex_sciscinet.py'))
audit=importlib.util.module_from_spec(spec);spec.loader.exec_module(audit)

class AuditTest(unittest.TestCase):
 def test_conflicts_and_years(self):
  with tempfile.TemporaryDirectory() as tmp:
   root=Path(tmp);c=duckdb.connect()
   c.execute(f"COPY (SELECT CAST(i AS VARCHAR) PMID,2001 PYEAR FROM range(1,10) t(i)) TO '{root/'analysis.parquet'}' (FORMAT PARQUET)")
   with (root/'citations.csv').open('w') as h:
    w=csv.writer(h);w.writerows([[str(i),'ISSN','2001 Dec','2002-01-01','2001'] for i in range(1,10)])
   with gzip.open(root/'ids.csv.gz','wt',newline='') as h:
    w=csv.writer(h);w.writerow(['work_id','pmid','doi'])
    w.writerows([[f'https://openalex.org/W{i}',f'https://pubmed.ncbi.nlm.nih.gov/{i}/','https://doi.org/a,b'] for i in range(1,9)])
    w.writerows([['W22','2',''],['W3','99',''],['bad','4',''],['W55','000','']])
   with gzip.open(root/'years.csv.gz','wt',newline='') as h:
    w=csv.writer(h);w.writerow(['work_id','publication_year'])
    w.writerows([[f'W{i}',2000 if i==4 else 2001] for i in range(1,9)])
    w.writerows([['W22',2001],['W5',2002],['W6','bad']])
   c.execute(f"COPY (SELECT 'W'||i paperid,CASE WHEN i=4 THEN 2000 WHEN i=7 THEN 2002 ELSE 2001 END AS \"year\" FROM range(1,8) t(i) UNION ALL SELECT 'W22',2001) TO '{root/'sci.parquet'}' (FORMAT PARQUET)")
   args=audit.parser().parse_args(['--analysis',str(root/'analysis.parquet'),'--citations',str(root/'citations.csv'),'--ids',str(root/'ids.csv.gz'),'--years',str(root/'years.csv.gz'),'--sciscinet',str(root/'sci.parquet'),'--output',str(root/'out')])
   audit.run(args)
   rows=c.execute(f"SELECT pmid,match_status FROM read_parquet('{root/'out/pmid_work_year_audit.parquet'}')").fetchall()
   statuses={p:s for p,s in rows}
   self.assertEqual(len(rows),10)
   self.assertEqual(statuses['1'],'all_equal')
   self.assertEqual(statuses['2'],'ambiguous_mapping')
   self.assertEqual(statuses['3'],'ambiguous_mapping')
   self.assertEqual(statuses['4'],'openalex_sciscinet_equal')
   self.assertEqual(statuses['5'],'missing_or_conflicting_year')
   self.assertEqual(statuses['6'],'missing_or_conflicting_year')
   self.assertEqual(statuses['7'],'semmed_openalex_equal')
   self.assertEqual(statuses['8'],'missing_or_conflicting_year')
   self.assertEqual(statuses['9'],'no_openalex_mapping')
   self.assertEqual(c.execute(f"SELECT delta_sciscinet_semmed FROM read_parquet('{root/'out/pmid_work_year_audit.parquet'}') WHERE pmid='4'").fetchone()[0],-1)
   with self.assertRaises(FileExistsError):audit.run(args)
   c.close()

 def fixture(self,root,analysis,citations,ids,years,sci,header=False):
  c=duckdb.connect()
  c.execute('CREATE TABLE a(PMID VARCHAR,PYEAR VARCHAR)')
  if analysis:c.executemany('INSERT INTO a VALUES (?,?)',analysis)
  c.execute(f"COPY a TO '{root/'analysis.parquet'}' (FORMAT PARQUET)")
  c.execute('CREATE TABLE s(paperid VARCHAR,year VARCHAR)')
  if sci:c.executemany('INSERT INTO s VALUES (?,?)',sci)
  c.execute(f"COPY s TO '{root/'sci.parquet'}' (FORMAT PARQUET)");c.close()
  for name,columns,rows in [('citations',['PMID','ISSN','DP','EDAT','PYEAR'],citations),('ids',['work_id','pmid','doi'],ids),('years',['work_id','publication_year'],years)]:
   with (root/f'{name}.csv').open('w',newline='') as h:
    w=csv.writer(h)
    if name!='citations' or header:w.writerow(columns)
    w.writerows(rows)
  argv=['--analysis',str(root/'analysis.parquet'),'--citations',str(root/'citations.csv'),'--ids',str(root/'ids.csv'),'--years',str(root/'years.csv'),'--sciscinet',str(root/'sci.parquet'),'--output',str(root/'out')]
  if header:argv+=['--citations-header']
  return audit.parser().parse_args(argv)

 def test_strict_years_and_other_statuses(self):
  with tempfile.TemporaryDirectory() as tmp:
   root=Path(tmp)
   analysis=[(str(i),'2001') for i in range(1,10)]+[('1','2001')]
   citations=[(str(i),'','2001 Dec','2002-1-1','2000' if i==5 else '2001') for i in range(1,10)]
   ids=[(f'W{i}','0001' if i==1 else str(i),'') for i in range(1,9)]
   years=[(f'W{i}','2000' if i in [2,3] else '2001.8' if i==4 else '2001') for i in range(1,9)]
   sci=[(f'W{i}','2002' if i==3 else '2e3' if i==6 else '2001') for i in range(1,9)]
   years += [('W1','2001'),('W8',None)]
   sci += [('W1','2001')]
   args=self.fixture(root,analysis,citations,ids,years,sci,header=True)
   audit.run(args)
   c=duckdb.connect();rows=dict(c.execute(f"SELECT pmid,match_status FROM '{root/'out/pmid_work_year_audit.parquet'}'").fetchall())
   self.assertEqual(rows['1'],'all_equal')
   self.assertEqual(rows['2'],'semmed_sciscinet_equal')
   self.assertEqual(rows['3'],'all_different')
   self.assertEqual(rows['4'],'missing_or_conflicting_year')
   self.assertEqual(rows['5'],'analysis_semmed_year_mismatch')
   self.assertEqual(rows['6'],'missing_or_conflicting_year')
   self.assertEqual(rows['8'],'missing_or_conflicting_year')
   summary=c.execute(f"SELECT sum(n_pmids) FROM read_csv_auto('{root/'out/year_difference_semmed_analysis.csv'}')").fetchone()[0]
   self.assertEqual(summary,9) # Includes PMID 9 with no OpenAlex match; no duplication for PMID 1.
   c.close()

 def test_empty_analysis_fails(self):
  with tempfile.TemporaryDirectory() as tmp:
   args=self.fixture(Path(tmp),[],[],[],[],[])
   with self.assertRaisesRegex(ValueError,'empty'):audit.run(args)
   self.assertFalse(args.output.exists())

 def test_invalid_analysis_year_fails(self):
  with tempfile.TemporaryDirectory() as tmp:
   args=self.fixture(Path(tmp),[('1','2001.8')],[],[],[],[])
   with self.assertRaisesRegex(ValueError,'Invalid analysis'):audit.run(args)
   self.assertFalse(args.output.exists())

 def test_no_mappings_and_empty_sources(self):
  with tempfile.TemporaryDirectory() as tmp:
   root=Path(tmp)
   args=self.fixture(root,[('1','2001')],[('99','','2001','2001','2001')],[],[],[])
   audit.run(args)
   c=duckdb.connect()
   row=c.execute(f"SELECT pmid,match_status FROM '{root/'out/pmid_work_year_audit.parquet'}'").fetchone()
   self.assertEqual(row,('1','no_openalex_mapping'));c.close()

 def test_header_flag_mismatch_fails(self):
  with tempfile.TemporaryDirectory() as tmp:
   args=self.fixture(Path(tmp),[('1','2001')],[('1','','2001','2001','2001')],[],[],[],header=True)
   args.citations_header=False
   with self.assertRaisesRegex(ValueError,'header'):audit.run(args)
   self.assertFalse(args.output.exists())

if __name__=='__main__':unittest.main()
