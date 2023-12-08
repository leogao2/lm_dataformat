# LM_Dataformat 

[![Build unstable](https://github.com/lfoppiano/lm_dataformat/actions/workflows/ci-build.yml/badge.svg)](https://github.com/lfoppiano/lm_dataformat/actions/workflows/ci-build.yml)

Utilities for storing data for LM training.

**NOTE**: The original project seems abandoned, I've continued the development to support ore data formats and integration with stackexchange_dataset


## Basic Usage

To write:

```
ar = Archive('output_dir')

for x in something():
  # do other stuff
  ar.add_data(somedocument, meta={
    'example': stuff,
    'someothermetadata': [othermetadata, otherrandomstuff],
    'otherotherstuff': True
  })

# remember to commit at the end!
ar.commit()
```

To read:

```
rdr = Reader('input_dir_or_file')

for doc in rdr.stream_data():
  # do something with the document
```
