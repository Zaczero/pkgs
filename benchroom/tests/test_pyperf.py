import pyperf

from benchroom.pyperf import load


def test_readings_remain_grouped_by_worker(tmp_path):
    benchmark = pyperf.Benchmark([
        pyperf.Run([1.0, 3.0], metadata={'name': 'encode', 'unit': 'second'}),
        pyperf.Run([6.0, 10.0], metadata={'name': 'encode', 'unit': 'second'}),
    ])
    path = tmp_path / 'results.json'
    pyperf.BenchmarkSuite([benchmark]).dump(str(path))
    (series,) = load(path, name='Encoder')
    assert series.values == (2.0, 8.0)
    assert series.name == 'Encoder'
