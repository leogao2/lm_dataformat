import lm_dataformat as lmd

import shutil

def test_dat():
    archive = lmd.DatArchive('test_dir')
    blns = open('test/blns.txt').read()
    archive.add_data(blns)
    archive.add_data('testing 123')
    archive.add_data(blns)
    archive.add_data('testing 123456789')
    archive.commit()

    reader = lmd.Reader('test_dir')

    data = list(reader.stream_data())

    assert data[0] == blns
    assert data[1] == 'testing 123'
    assert data[2] == blns
    assert data[3] == 'testing 123456789'
    shutil.rmtree('test_dir')

def test_json():
    archive = lmd.JSONArchive('test_dir')
    blns = open('test/blns.txt').read()
    archive.add_data(blns)
    archive.add_data('testing 123')
    archive.add_data(blns)
    archive.add_data('testing 123456789')
    archive.commit()

    reader = lmd.Reader('test_dir')

    data = list(reader.stream_data())

    assert data[0] == blns
    assert data[1] == 'testing 123'
    assert data[2] == blns
    assert data[3] == 'testing 123456789'
    shutil.rmtree('test_dir')

def test_jsonl():
    archive = lmd.Archive('test_dir')
    blns = open('test/blns.txt').read()
    archive.add_data(blns)
    archive.add_data('testing 123', meta={'testing': 123})
    archive.add_data(blns, meta={'testing2': 456, 'testing': ['a','b']})
    archive.add_data('testing 123456789')
    archive.commit()

    reader = lmd.Reader('test_dir')

    data = list(reader.stream_data(get_meta=True))

    assert data[0] == (blns, {})
    assert data[1] == ('testing 123', {'testing': 123})
    assert data[2] == (blns, {'testing2': 456, 'testing': ['a','b']})
    assert data[3] == ('testing 123456789', {})
    shutil.rmtree('test_dir')

def test_jsonl_paras():
    archive = lmd.Archive('test_dir')
    blns = open('test/blns.txt').read()
    archive.add_data(blns)
    archive.add_data(['testing 123', 'testing 345'], meta={'testing': 123})
    archive.add_data(blns, meta={'testing2': 456, 'testing': ['a','b']})
    archive.add_data('testing 123456789')
    archive.commit()

    reader = lmd.Reader('test_dir')

    data = list(reader.stream_data(get_meta=True))

    assert data[0] == (blns, {})
    assert data[1] == ('testing 123\n\ntesting 345', {'testing': 123})
    assert data[2] == (blns, {'testing2': 456, 'testing': ['a','b']})
    assert data[3] == ('testing 123456789', {})
    shutil.rmtree('test_dir')