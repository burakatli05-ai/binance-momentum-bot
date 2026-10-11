"""Exactly one final invocation; no test or benchmark CLI escape hatches."""
import pathlib,json,hashlib,datetime,sys,traceback
HERE=pathlib.Path(__file__).resolve().parent
def save(path,value):
    path.write_text(json.dumps(value,ensure_ascii=False,indent=2)+'\n',encoding='utf-8',newline='\n')
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def main():
    manifest=json.loads((HERE/'Final_Disposition_Freeze_Manifest.json').read_text(encoding='utf-8'))
    with (HERE/'FINAL_RUN_STARTED.json').open('x',encoding='utf-8') as f:
        json.dump(dict(started_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),
            freeze_sha256=sha(HERE/'Final_Disposition_Freeze_Manifest.json'),one_run_only=True),f,indent=2)
    output=HERE/'final_run';output.mkdir(exist_ok=False)
    try:
        for name,expected in manifest['frozen_files'].items():
            assert sha(HERE/name)==expected,'FREEZE_MISMATCH '+name
        from Final_Harness import run_tests,benchmark
        result,traces=run_tests(output)
        perf=benchmark(output)
        comparisons=dict(unique_scenarios=result['unique_tests'],
            all_strategy_comparisons_equal=all(all(c.values()) for r in result['results'] for c in r['comparisons']),
            rows=[dict(id=r['id'],comparisons=r['comparisons']) for r in result['results']],
            local_fixture_only=True,production_noninterference_proven=False)
        save(output/'Final_Transcript_Comparison.json',comparisons)
        relevant=[t for t in traces if t['id'] in ('M14','M33','M34','M35','M36')]
        save(output/'Final_Serialization_Completeness.json',dict(
            serialized_gate_cap=next(r for r in result['results'] if r['id']=='M33')['status'],
            serialized_runtime_identity=next(r for r in result['results'] if r['id']=='M34')['status'],
            gate_episode_observed=False,durable_receipt_identity=False,
            complete_exact_receipt=False,witnesses=[dict(id=t['id'],subcase=t['subcase'],witness=t['candidate']['witness']) for t in relevant]))
        for name,expected in manifest['frozen_files'].items():
            assert sha(HERE/name)==expected,'FREEZE_CHANGED_DURING_RUN '+name
        required=['Final_Test_Results.json','Final_Raw_Transcripts.json','Final_Adversarial_Read_Results.csv',
            'Final_Transcript_Comparison.json','Final_Serialization_Completeness.json',
            'Final_Performance_Result.json','Microbenchmark_Raw.csv','Bytecode_Comparison.json']
        assert all((output/n).is_file() for n in required),'MISSING_ARTIFACT'
        passed=result['unique_tests']==36 and result['passed']==36 and perf['status']=='PASS' and comparisons['all_strategy_comparisons_equal']
        disposition='A' if passed else 'B'
        save(output/'FINAL_RUN_COMPLETED.json',dict(completed_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),
            result_class=disposition,unique_scenarios=36,passed=result['passed'],failed=result['failed'],
            performance=perf['status'],complete=True,behavior_runs=1,retests=0,
            frozen_files_unchanged=True,artifact_hashes={n:sha(output/n) for n in required}))
        print(json.dumps(dict(final_disposition=disposition,passed=result['passed'],failed=result['failed'],complete=True)))
        return 0 if passed else 1
    except BaseException as exc:
        save(output/'FINAL_RUN_ABORTED.json',dict(result_class='B',complete=False,error_type=type(exc).__name__,
            error=str(exc),traceback=traceback.format_exc(),retests=0))
        print(traceback.format_exc());return 2
if __name__=='__main__':sys.exit(main())
