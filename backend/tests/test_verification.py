from pathlib import Path
import pytest
from backend.verification.fresh_pipeline import isolated_config


def test_gate_cannot_reuse_named_volumes_or_host_ports():
    config = {'name': 'echoguard', 'volumes': {'mongo_data': {'name': 'echoguard_mongo_data'}},
              'networks': {'default': {'name': 'echoguard_default'}},
              'services': {'producer': {'volumes': [{'source': '/existing/scenarios'}]},
                           'mongo': {'ports': [{'published': '27017'}], 'restart': 'unless-stopped',
                                     'container_name': 'existing-mongo'}}}
    result = isolated_config(config, 'gate-unique', Path('/private/gate/scenarios'))
    assert result['name'] == 'gate-unique'
    assert result['volumes']['mongo_data'] == {}
    assert result['networks']['default'] == {}
    assert not result['services']['mongo']
    assert result['services']['producer']['volumes'][0]['source'] == '/private/gate/scenarios'


def test_gate_rejects_external_resources():
    with pytest.raises(ValueError, match='External'):
        isolated_config({'volumes': {'shared': {'external': True}}}, 'gate', Path('/tmp'))


def test_rendered_gate_keeps_volume_initialization_dependency(tmp_path):
    """Exercise the real Compose config and isolation transform without a daemon."""
    import json
    import shutil
    import subprocess
    from backend.verification.fresh_pipeline import ROOT

    if shutil.which('docker') is None:
        pytest.skip('Compose CLI required for rendered configuration regression')
    config = json.loads(subprocess.check_output(
        ['docker', 'compose', '--profile', 'demo', '--profile', 'stream',
         '--profile', 'worker', '--profile', 'workflow', 'config', '--format', 'json'],
        cwd=ROOT,
    ))
    gate = isolated_config(config, 'echoguard-gate-test', tmp_path)
    broker = gate['services']['kafka']
    initializer = gate['services']['kafka-volume-init']
    assert broker['user'] == '1000:1000'
    assert initializer['user'] == '0:0'
    assert broker['depends_on']['kafka-volume-init']['condition'] == 'service_completed_successfully'
    for service in (broker, initializer):
        mount = next(v for v in service['volumes'] if v['target'] == '/tmp/kraft-combined-logs')
        assert mount['type'] == 'volume' and mount['source'] == 'kafka_data'
        assert not mount.get('read_only', False)
    assert 'name' not in gate['volumes']['kafka_data']
    script = next(v for v in initializer['volumes'] if v['target'].endswith('/init_volume.sh'))
    assert script['read_only']
    assert Path(script['source']).is_file()
    subprocess.run(['sh', '-n', script['source']], check=True)
    path = tmp_path / 'compose.json'
    path.write_text(json.dumps(gate))
    subprocess.run(['docker', 'compose', '-p', 'echoguard-gate-test',
                    '-f', str(path), 'config', '--quiet'], check=True)
