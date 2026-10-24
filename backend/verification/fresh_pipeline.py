"""Run streaming verification with isolated Kafka/Mongo/checkpoint volumes.

Only resources owned by a new random Compose project are removed. Existing
EchoGuard containers, volumes, credentials and scenario files are untouched.
"""
import argparse
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import subprocess
import tempfile
from uuid import uuid4

from backend.ingest.scenario import ScenarioConfig, write_scenario

ROOT = Path(__file__).resolve().parents[2]


def isolated_config(config, project, scenario_dir):
    config['name'] = project
    for section in ('volumes', 'networks'):
        for value in config.get(section, {}).values():
            if value.get('external'):
                raise ValueError('External resources are not allowed in an isolated gate')
            value.pop('name', None)
    for service in config['services'].values():
        service.pop('container_name', None)
        # Gate runs inside Docker, so no host ports are needed.
        service.pop('ports', None)
        service.pop('restart', None)
    producer = config['services']['producer']
    producer['volumes'] = [{'type': 'bind', 'source': str(scenario_dir), 'target': '/app/data/scenarios'}]
    return config


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--report', type=Path, default=ROOT / 'data/scenarios/fresh-pipeline-report.json')
    args = parser.parse_args()
    project = 'echoguard-gate-' + uuid4().hex[:12]
    compose = ['docker', 'compose', '--profile', 'demo', '--profile', 'stream', '--profile', 'worker', '--profile', 'workflow']
    config = json.loads(subprocess.check_output(compose + ['config', '--format', 'json'], cwd=ROOT))
    with tempfile.TemporaryDirectory(prefix='echoguard-gate-') as temporary:
        directory = Path(temporary)
        scenarios = directory / 'scenarios'
        scenarios.mkdir()
        # A fresh project has empty watermark state; a single half-hour bucket
        # keeps this gate's expected two clusters independent of wall-clock minute.
        now = datetime.now(timezone.utc)
        start = now.replace(minute=(now.minute // 30) * 30, second=0, microsecond=0) - timedelta(minutes=30)
        manifest = write_scenario(ScenarioConfig(start_time=start), scenarios / 'payment_spike.jsonl',
                                  scenarios / 'payment_spike.manifest.json')
        path = directory / 'compose.json'
        path.write_text(json.dumps(isolated_config(config, project, scenarios)))
        command = ['docker', 'compose', '-p', project, '-f', str(path), '--profile', 'demo',
                   '--profile', 'stream', '--profile', 'worker', '--profile', 'workflow']
        def run(*arguments, check=True):
            return subprocess.run(command + list(arguments), cwd=ROOT, check=check)
        def run_json(*arguments):
            result = subprocess.run(command + list(arguments), cwd=ROOT, text=True,
                                    capture_output=True)
            if result.returncode:
                print(result.stdout, end='')
                print(result.stderr, end='')
                result.check_returncode()
            report = json.loads(result.stdout.strip().splitlines()[-1])
            print(json.dumps(report, sort_keys=True))
            return report
        passed = False
        try:
            run('build', 'producer', 'stream-job', 'case-writer', 'workflow-tool')
            run('up', '-d', 'stream-job', 'case-writer')
            run('run', '--rm', 'producer', 'publish', '--rate', '0')
            run('run', '--rm', 'producer', 'verify')
            run('exec', '-T', 'stream-job', 'python3', '-m', 'backend.stream.verify', '--timeout', '300')
            run('exec', '-T', 'case-writer', 'python', '-m', 'backend.worker.verify', '--timeout', '300')
            first_spark = run_json('exec', '-T', 'stream-job', 'python3',
                                   '-m', 'backend.verification.offsets', 'spark')
            first_worker = run_json('exec', '-T', 'case-writer', 'python',
                                    '-m', 'backend.verification.offsets', 'worker')
            first_ends = run_json('exec', '-T', 'case-writer', 'python',
                                  '-m', 'backend.verification.offsets', 'snapshot')['topic_end']
            # Replay keeps the same logical IDs but adds fresh physical Kafka
            # records. The offset boundary must advance after restarting.
            run('restart', 'stream-job', 'case-writer')
            run('run', '--rm', 'producer', 'publish', '--rate', '0')
            run('run', '--rm', 'producer', 'verify')
            second_ends = run_json('exec', '-T', 'case-writer', 'python',
                                   '-m', 'backend.verification.offsets', 'snapshot')['topic_end']
            before_raw, after_raw = first_ends['support.raw.v1'], second_ends['support.raw.v1']
            if before_raw.keys() != after_raw.keys():
                raise AssertionError('Raw topic partitions changed during replay')
            replay_records = sum(after_raw[partition] - before_raw[partition] for partition in before_raw)
            if replay_records != 263:
                raise AssertionError(f'Replay raw offsets did not advance by 263: {replay_records}')
            second_spark = run_json('exec', '-T', 'stream-job', 'python3',
                                    '-m', 'backend.verification.offsets', 'spark')
            second_worker = run_json('exec', '-T', 'case-writer', 'python',
                                     '-m', 'backend.verification.offsets', 'worker')
            run('exec', '-T', 'case-writer', 'python', '-m', 'backend.worker.verify', '--timeout', '300')
            run('run', '--rm', '--entrypoint', 'python', 'workflow-tool',
                '-m', 'backend.actions.verify')
            passed = True
        finally:
            if not passed:
                run('logs', '--tail=100', 'kafka-volume-init', 'stream-job', 'case-writer', 'mongo', 'kafka', check=False)
            run('down', '-v', '--remove-orphans', check=False)
        report = {'status': 'verified', 'project': project, 'isolated_initial_state': True,
                  'dataset_sha256': manifest['dataset_sha256'],
                  'replay_records': replay_records,
                  'first_spark': first_spark, 'first_worker': first_worker,
                  'second_spark': second_spark, 'second_worker': second_worker,
                  'verified_at': datetime.now(timezone.utc).isoformat()}
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(json.dumps(report, indent=2) + '\n')
        print(json.dumps(report))


if __name__ == '__main__':
    main()
