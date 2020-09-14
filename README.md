# LM_Dataformat [![Build Status](https://travis-ci.org/leogao2/lm_dataformat.svg?branch=master)](https://travis-ci.org/leogao2/lm_dataformat) [![Coverage Status](https://coveralls.io/repos/github/leogao2/lm_dataformat/badge.svg?branch=master)](https://coveralls.io/github/leogao2/lm_dataformat?branch=master)

Utilities for storing data for LM training.


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
