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

def test_jsonl_tar():
    blns = open('test/blns.txt').read()
    reader = lmd.Reader('test/blns.jsonl.zst.tar')

    data = list(reader.stream_data(get_meta=True))

    assert data[0] == (blns, {})
    assert data[1] == ('testing 123\n\ntesting 345', {'testing': 123})
    assert data[2] == (blns, {'testing2': 456, 'testing': ['a','b']})
    assert data[3] == ('testing 123456789', {})

    assert data[4] == (blns, {})
    assert data[5] == ('testing 123\n\ntesting 345', {'testing': 123})
    assert data[6] == (blns, {'testing2': 456, 'testing': ['a','b']})
    assert data[7] == ('testing 123456789', {})

def test_txt_read():
    reader = lmd.Reader('test/blns.txt')
    blns = open('test/blns.txt').read()

    data = list(reader.stream_data(get_meta=False))

    assert data[0] == blns
    assert len(data) == 1

def test_zip_read():
    reader = lmd.Reader('test/blns.txt.zip')
    blns = open('test/blns.txt').read()

    data = list(reader.stream_data(get_meta=False))

    assert data[0] == blns
    assert len(data) == 1

def test_tgz_read():
    reader = lmd.Reader('test/blns.txt.tar.gz')
    blns = open('test/blns.txt').read()

    data = list(reader.stream_data(get_meta=False))

    assert data[0] == blns
    assert len(data) == 1
