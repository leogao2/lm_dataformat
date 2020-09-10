# LM_Dataformat

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

for doc in ar.stream_data():
  # do something with the document
```
