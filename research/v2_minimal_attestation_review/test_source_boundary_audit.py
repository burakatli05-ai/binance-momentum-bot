"""Supplemental S01-S10 feasibility checks; not T001-T092 acceptance tests."""
import ast
import hashlib
import json
from pathlib import Path
import sys
import unittest
sys.dont_write_bytecode = True
sys.path.insert(0,str(Path(__file__).resolve().parent))
import source_boundary_audit as audit

class BoundaryChecks(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.sources=audit.read_sources()
        cls.result=audit.audit()

    def test_S01_exact_source_SHA(self):
        self.assertEqual(len(self.sources),4)
        for name,digest in audit.EXPECTED.items():
            self.assertEqual(hashlib.sha256((audit.APP/name).read_bytes()).hexdigest(),digest)

    def test_S02_gate_inline_no_retained_operand(self):
        gate=audit.function(self.sources['bot.py'][1],'early_notify_pass')
        self.assertEqual(audit.name_reads(gate,'EARLY_NOTIFY_MAX_BUY30'),[5558])
        self.assertEqual(audit.direct_local_captures(gate,'EARLY_NOTIFY_MAX_BUY30'),[])
        self.assertIsInstance(gate.body[-1],ast.Return)
        comparison=next(n for n in ast.walk(gate) if isinstance(n,ast.Compare) and any(isinstance(c,ast.Name) and c.id=='EARLY_NOTIFY_MAX_BUY30' for c in n.comparators))
        self.assertEqual(ast.unparse(comparison),'EARLY_NOTIFY_MIN_BUY30 <= m[\'buy30\'] <= EARLY_NOTIFY_MAX_BUY30')

    def test_S03_label_inline_thresholds(self):
        scorer=audit.function(self.sources['bot.py'][1],'ignition_shadow_score')
        for name in ('FAST_EARLY_V2_SCORE','IGNITION_V2_MIN_SCORE'):
            self.assertEqual(audit.name_reads(scorer,name),[4272])
            self.assertEqual(audit.direct_local_captures(scorer,name),[])
        self.assertEqual(ast.unparse(scorer.body[-1]),'return (pts, label, reasons)')

    def test_S04_qualification_and_SQL_are_separate_reads(self):
        selector=audit.function(self.sources['early_autotrader_v2.py'][1],'record_selector','Pilot')
        self.assertEqual(audit.attr_reads(selector,'self.cfg.min_score'),[133,149])
        self.assertEqual(audit.direct_local_captures(selector,'self.cfg.min_score'),[])
        self.assertEqual(ast.unparse(selector.body[-1]),'return qualified')

    def test_S05_publication_precedes_generation(self):
        fn=audit.function(self.sources['early_autotrader_v2.py'][1],'configure','Pilot')
        lines={n.lineno:ast.unparse(n) for n in fn.body}
        self.assertEqual(lines[125],'self.setting(key, value)')
        self.assertEqual(lines[126],'self.cfg = config')
        self.assertEqual(lines[127],'self.generation += 1')
        self.assertFalse(any(isinstance(n,ast.Call) and ast.unparse(n.func)=='self.event' for n in ast.walk(fn)))

    def test_S06_gate_precedes_arm_without_cap_argument(self):
        tree=self.sources['bot.py'][1]
        calls=[n for n in ast.walk(tree) if isinstance(n,ast.Call)]
        gate=next(n for n in calls if n.lineno==6022 and ast.unparse(n.func)=='early_notify_pass')
        arm=next(n for n in calls if n.lineno==6042 and ast.unparse(n.func)=='early_v2.arm')
        self.assertLess(gate.lineno,arm.lineno)
        self.assertEqual([ast.unparse(a) for a in arm.args],['st.active_radar_id','symbol','m','score'])

    def test_S07_existing_clock_and_branch_order(self):
        fn=audit.function(self.sources['early_v2_adapter.py'][1],'arm','Integration')
        calls={(n.lineno,ast.unparse(n.func)) for n in ast.walk(fn) if isinstance(n,ast.Call)}
        for line,callee in [(347,'self.pilot.clock'),(352,'self.step_lock.arm'),(357,"self.b['ignition_shadow_score']"),(365,'self.pilot.record_selector'),(369,'self.pilot.event'),(381,'self._queue_notify'),(397,'self.queue.put_nowait')]:
            self.assertIn((line,callee),calls)
        self.assertEqual([n.lineno for n in ast.walk(fn) if isinstance(n,ast.Call) and ast.unparse(n.func)=='self.pilot.clock'],[347])

    def test_S08_defaults_and_publications_are_distinct(self):
        pubs={(x['file'],x['line'],x['target']) for x in self.result['publications']}
        for line,name in [(135,'EARLY_NOTIFY_MAX_BUY30'),(222,'IGNITION_V2_MIN_SCORE'),(223,'FAST_EARLY_V2_SCORE')]:
            self.assertIn(('bot.py',line,name),pubs)
        self.assertIn(('early_autotrader_v2.py',98,'self.cfg'),pubs)
        self.assertIn(('early_autotrader_v2.py',126,'self.cfg'),pubs)
        self.assertEqual(len([x for x in pubs if x[2]=='self.generation']),5)

    def test_S09_ten_fresh_source_files_unchanged(self):
        identity=json.loads((audit.HERE/'Source_Base_Identity.json').read_text(encoding='utf8'))
        self.assertEqual(len(identity['files']),10)
        for row in identity['files']:
            raw=(audit.ROOT/row['repo_path']).read_bytes()
            self.assertEqual(len(raw),row['size'])
            self.assertEqual(hashlib.sha256(raw).hexdigest(),row['sha256'])

    def test_S10_same_result_cannot_identify_threshold(self):
        # Pure logical counterexamples only: no selector replay, fitting or market outcome.
        self.assertEqual(0.7<=0.8,0.7<=0.9)
        self.assertEqual(95>=85,95>=90)
        self.assertEqual(95>=90,95>=94)
        self.assertNotEqual(0.8,0.9)
        before,consumed,after=0.8,0.9,0.8
        self.assertEqual(before,after)
        self.assertNotEqual(consumed,after)

if __name__=='__main__':
    unittest.main(verbosity=2)
