import contextlib
import io
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import duckdb
from network_common import *
from build_pmid_adoption_network import build
from combine_pmid_adoption_network import combine

class NetworkTest(unittest.TestCase):
    def fixture(self,root,empty=False):
        c=duckdb.connect()
        c.execute('''CREATE TABLE a(PMID VARCHAR,PYEAR INTEGER,subject_cui_primary VARCHAR,PREDICATE VARCHAR,object_cui_primary VARCHAR,edge_annotation VARCHAR)''')
        rows=[('1',1980,'X','P','Y','New_Node'),('1',1980,'A','R','B','New_Combination'),('1',1980,'C','R','D','New_Relation'),('2',1980,'X','P','Y','New_Node'),('3',1980,'E','P','F','Repeated_Triple'),('4',1980,'G','P','H','New_Combination')]
        if empty:rows=[]
        if rows:c.executemany('INSERT INTO a VALUES (?,?,?,?,?,?)',rows)
        focal=annotation(root,1980);focal.parent.mkdir(parents=True)
        parquet(c,'SELECT * FROM a',focal)
        t=transition(root,1980);t.parent.mkdir(parents=True)
        parquet(c,"SELECT *, PMID IN ('1','2') future_exact_triple_seen,CASE WHEN PMID IN ('1','2') THEN 'Adopted' ELSE 'Disappeared' END future_five_year_transition FROM a",t)
        future={1981:[('5',1981,'X','P','Y','Repeated_Triple'),('5',1981,'A','R','B','Repeated_Triple')],1982:[('6',1982,'X','P','Y','Repeated_Triple'),('6',1982,'C','R','D','Repeated_Triple')],1983:[('7',1983,'I','P','J','Repeated_Triple')],1984:[],1985:[]}
        for y,rs in future.items():
            c.execute('DELETE FROM a')
            if rs:c.executemany('INSERT INTO a VALUES (?,?,?,?,?,?)',rs)
            parquet(c,'SELECT * FROM a',annotation(root,y))
        c.execute('''CREATE TABLE m(PMID VARCHAR,PYEAR INTEGER,n_triples BIGINT,n_new_node BIGINT,n_new_combination BIGINT,n_new_relation BIGINT,n_new_triples BIGINT,n_new_node_adopted BIGINT,n_new_combination_adopted BIGINT,n_new_relation_adopted BIGINT,n_new_triples_adopted BIGINT,has_new_triples_adopted INTEGER)''')
        if not empty:c.executemany('INSERT INTO m VALUES (?,?,?,?,?,?,?,?,?,?,?,?)',[
            ('1',1980,3,1,1,1,3,1,1,1,3,1),('2',1980,1,1,0,0,1,1,0,0,1,1),('3',1980,1,0,0,0,0,0,0,0,0,0),('4',1980,1,0,1,0,1,0,0,0,0,0)])
        m=metric(root,1980);m.parent.mkdir(parents=True);parquet(c,'SELECT * FROM m',m);c.close()

    def quiet(self,fn,*args,**kwargs):
        with contextlib.redirect_stdout(io.StringIO()):return fn(*args,**kwargs)

    def test_edges_nodes_trace(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);self.fixture(root)
            s=self.quiet(build,root,1980,buckets=3)
            self.assertEqual(s['n_triple_paper_pair_occurrences'],6)
            self.assertEqual(s['n_edges'],4)
            c=duckdb.connect();ep=annual(root,1980)['edges']
            edges=c.execute(f'SELECT adopter_pmid,focal_pmid,n_shared_new_triples FROM read_parquet({q(ep)}) ORDER BY 1,2').fetchall()
            self.assertEqual(edges,[('5','1',2),('5','2',1),('6','1',2),('6','2',1)])
            self.assertEqual(scalar(c,f"SELECT count(*) FROM read_parquet({q(annual(root,1980)['trace'])})"),9)
            report=self.quiet(combine,root,1980,1980)
            self.assertEqual(report['n_nodes'],7);self.assertEqual(report['n_zero_degree_nodes'],3)
            nodes=root/OUT/'nodes/pmid_adoption_network_nodes_1980_1985.parquet'
            self.assertEqual(c.execute(f"SELECT in_degree,in_strength,n_new_triples_adopted FROM read_parquet({q(nodes)}) WHERE PMID='1'").fetchone(),(2,4,3))
            self.assertEqual(c.execute(f"SELECT focal_status,in_degree,out_degree FROM read_parquet({q(nodes)}) WHERE PMID='4'").fetchone(),('new_triples_not_adopted',0,0))
            self.assertIsNone(c.execute(f"SELECT n_new_triples_adopted FROM read_parquet({q(nodes)}) WHERE PMID='5'").fetchone()[0])
            with self.assertRaises(FileExistsError):build(root,1980)
            c.close()

    def test_profile_and_partition_equivalence(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);self.fixture(root)
            self.quiet(build,root,1980,profile_only=True)
            self.assertFalse(annual(root,1980)['edges'].exists())
            self.quiet(build,root,1980,buckets=1)
            c=duckdb.connect();c.execute(f"CREATE TABLE old AS SELECT * FROM read_parquet({q(annual(root,1980)['edges'])})")
            self.quiet(build,root,1980,buckets=7,overwrite=True)
            self.assertEqual(scalar(c,f"SELECT count(*) FROM ((SELECT * FROM old EXCEPT ALL SELECT * FROM read_parquet({q(annual(root,1980)['edges'])})) UNION ALL (SELECT * FROM read_parquet({q(annual(root,1980)['edges'])}) EXCEPT ALL SELECT * FROM old))"),0)
            c.close()

    def test_empty_focal(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);self.fixture(root,empty=True)
            s=self.quiet(build,root,1980,buckets=2)
            self.assertEqual(s['n_edges'],0)
            report=self.quiet(combine,root,1980,1980)
            self.assertEqual(report['n_nodes'],3)

    def test_duplicate_future_fails(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);self.fixture(root)
            c=duckdb.connect();path=annotation(root,1981)
            c.execute(f'CREATE TABLE a AS SELECT * FROM read_parquet({q(path)})')
            parquet(c,'SELECT * FROM a UNION ALL SELECT * FROM a',path);c.close()
            with self.assertRaisesRegex(ValueError,'duplicate PMID triples'):self.quiet(build,root,1980)
            self.assertFalse(annual(root,1980)['validation'].exists())

    def test_previous_metrics_disagreement(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);self.fixture(root)
            c=duckdb.connect();path=metric(root,1980)
            c.execute(f'CREATE TABLE m AS SELECT * FROM read_parquet({q(path)})');c.execute("UPDATE m SET n_new_triples_adopted=2 WHERE PMID='1'")
            parquet(c,'SELECT * FROM m',path);c.close()
            with self.assertRaisesRegex(ValueError,'Existing PMID adoption'):self.quiet(build,root,1980)

    def test_stale_output_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);self.fixture(root);self.quiet(build,root,1980,buckets=2)
            path=annual(root,1980)['trace'];path.touch()
            with self.assertRaisesRegex(ValueError,'output changed'):self.quiet(combine,root,1980,1980)

    def test_direction_is_exact_and_missing_year_fails(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);self.fixture(root)
            c=duckdb.connect();path=annotation(root,1981)
            c.execute(f'CREATE TABLE a AS SELECT * FROM read_parquet({q(path)})')
            c.execute("UPDATE a SET subject_cui_primary='B',object_cui_primary='A' WHERE PREDICATE='R'")
            parquet(c,'SELECT * FROM a',path);c.close()
            with self.assertRaisesRegex(ValueError,'Reconstructed five-year adoption'):self.quiet(build,root,1980)
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);self.fixture(root);annotation(root,1985).unlink()
            with self.assertRaises(FileNotFoundError):self.quiet(build,root,1980)

    def test_two_year_combine_and_empty_edge_year(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);self.fixture(root)
            c=duckdb.connect()
            path=transition(root,1981)
            parquet(c,f"SELECT *,PREDICATE='P' future_exact_triple_seen,CASE WHEN PREDICATE='P' THEN 'Continued' ELSE 'Disappeared' END future_five_year_transition FROM read_parquet({q(annotation(root,1981))})",path)
            parquet(c,"SELECT '5'::VARCHAR PMID,1981::INTEGER PYEAR,2::BIGINT n_triples,0::BIGINT n_new_node,0::BIGINT n_new_combination,0::BIGINT n_new_relation,0::BIGINT n_new_triples,0::BIGINT n_new_node_adopted,0::BIGINT n_new_combination_adopted,0::BIGINT n_new_relation_adopted,0::BIGINT n_new_triples_adopted,0::INTEGER has_new_triples_adopted",metric(root,1981))
            parquet(c,f'SELECT * FROM read_parquet({q(annotation(root,1985))})',annotation(root,1986));c.close()
            self.quiet(build,root,1980,buckets=2);self.quiet(build,root,1981,buckets=2)
            r=self.quiet(combine,root,1980,1981)
            self.assertEqual(r['n_edges'],4)
            c=duckdb.connect()
            rows=c.execute(f"SELECT focal_year,n_edges FROM read_csv_auto({q(root/OUT/'summary/pmid_adoption_network_summary_1980_1981.csv')}) ORDER BY focal_year").fetchall()
            self.assertEqual(rows,[(1980,4),(1981,0)])
            nodes=root/OUT/'nodes/pmid_adoption_network_nodes_1980_1986.parquet'
            self.assertEqual(c.execute(f"SELECT focal_status,out_degree FROM read_parquet({q(nodes)}) WHERE PMID='5'").fetchone(),('no_new_triples',2));c.close()

    def test_fanout_under_memory_cap(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);self.fixture(root,empty=True)
            c=duckdb.connect()
            c.execute("CREATE TABLE a AS SELECT CAST(i AS VARCHAR) PMID,1980::INTEGER PYEAR,'X'::VARCHAR subject_cui_primary,'P'::VARCHAR PREDICATE,'Y'::VARCHAR object_cui_primary,'New_Node'::VARCHAR edge_annotation FROM range(1,201) t(i)")
            parquet(c,'SELECT * FROM a',annotation(root,1980))
            parquet(c,"SELECT *,true future_exact_triple_seen,'Adopted'::VARCHAR future_five_year_transition FROM a",transition(root,1980))
            for y in range(1981,1986):parquet(c,'SELECT * FROM a WHERE false',annotation(root,y))
            parquet(c,"SELECT CAST(i AS VARCHAR) PMID,1981::INTEGER PYEAR,'X'::VARCHAR subject_cui_primary,'P'::VARCHAR PREDICATE,'Y'::VARCHAR object_cui_primary,'Repeated_Triple'::VARCHAR edge_annotation FROM range(1001,1501) t(i)",annotation(root,1981))
            parquet(c,"SELECT PMID,PYEAR,1::BIGINT n_triples,1::BIGINT n_new_node,0::BIGINT n_new_combination,0::BIGINT n_new_relation,1::BIGINT n_new_triples,1::BIGINT n_new_node_adopted,0::BIGINT n_new_combination_adopted,0::BIGINT n_new_relation_adopted,1::BIGINT n_new_triples_adopted,1::INTEGER has_new_triples_adopted FROM a",metric(root,1980));c.close()
            with patch.dict('os.environ',{'IC_DUCKDB_MEMORY_LIMIT':'256MB','IC_DUCKDB_THREADS':'1'}):
                s=self.quiet(build,root,1980,buckets=8)
            self.assertEqual(s['n_edges'],100000)
            self.assertEqual(s['n_triple_paper_pair_occurrences'],100000)

    def test_five_year_boundary(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);self.fixture(root)
            c=duckdb.connect()
            c.execute(f'CREATE TABLE late AS SELECT * FROM read_parquet({q(annotation(root,1982))})')
            c.execute('UPDATE late SET PYEAR=1985')
            parquet(c,'SELECT * FROM late',annotation(root,1985))
            parquet(c,'SELECT * FROM late WHERE false',annotation(root,1982))
            parquet(c,"SELECT '8'::VARCHAR PMID,1986::INTEGER PYEAR,'G'::VARCHAR subject_cui_primary,'P'::VARCHAR PREDICATE,'H'::VARCHAR object_cui_primary,'Repeated_Triple'::VARCHAR edge_annotation",annotation(root,1986))
            c.close();s=self.quiet(build,root,1980,buckets=2)
            self.assertEqual(s['n_edges'],4)
            c=duckdb.connect();path=annual(root,1980)['edges']
            self.assertEqual(c.execute(f"SELECT min(year_gap),max(year_gap) FROM read_parquet({q(path)})").fetchone(),(1,5))
            self.assertEqual(scalar(c,f"SELECT count(*) FROM read_parquet({q(path)}) WHERE focal_pmid='4'"),0);c.close()

    def test_every_year_empty(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);self.fixture(root,empty=True)
            c=duckdb.connect()
            for y in range(1981,1986):
                path=annotation(root,y)
                c.execute(f'CREATE OR REPLACE TABLE empty AS SELECT * FROM read_parquet({q(path)}) WHERE false')
                parquet(c,'SELECT * FROM empty',path)
            c.close();self.quiet(build,root,1980,buckets=2)
            r=self.quiet(combine,root,1980,1980)
            self.assertEqual((r['n_nodes'],r['n_edges']),(0,0))

    def test_stale_source_and_cross_year_pmid(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);self.fixture(root);self.quiet(build,root,1980,buckets=2)
            metric(root,1980).touch()
            with self.assertRaisesRegex(ValueError,'Stale source'):self.quiet(combine,root,1980,1980)
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);self.fixture(root)
            c=duckdb.connect();path=annotation(root,1983)
            c.execute(f'CREATE TABLE a AS SELECT * FROM read_parquet({q(path)})')
            c.execute("UPDATE a SET PMID='5'");parquet(c,'SELECT * FROM a',path);c.close()
            with self.assertRaisesRegex(ValueError,'Future PMID has multiple years'):self.quiet(build,root,1980)

if __name__=='__main__':unittest.main()
