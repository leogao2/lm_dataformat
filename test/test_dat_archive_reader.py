import lm_dataformat as lmd

import shutil

def test_dat():
    archive = lmd.Archive('test_dir')
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