#Module for taking raw bulk json from stat

## CSV outputs are currently only working

###I think the main issue was that these days wasn't typical and had strange characters that caused things to failed. 

### Meaning, the json has to be read in to escape characters e.g. strict=False'
```python
data2 = json.loads(data.decode('utf-8'), strict=False) 
```

##All dependencies are located in the venv
### To Run Script:
```bash
~$ source venv/bin activate
(venv) ~$ python auto_loop.py
```

## Parquet file writes currently failing and will continue investigating, but think its:
###When the dataframe has an array for the 'ResultType' e.g sometimes it shows up as ['answers', 'list']

```bash
(venv) it-mbp-mattlivingston:stat_data_processing mattlivingston$ python test.py
Traceback (most recent call last):
  File "test.py", line 47, in <module>
    table = pa.Table.from_pandas(dataframe)
  File "pyarrow/table.pxi", line 875, in pyarrow.lib.Table.from_pandas (/Users/travis/build/BryanCutler/arrow-dist/arrow/python/build/temp.macosx-10.6-intel-3.6/lib.cxx:44927)
  File "/Users/mattlivingston/PycharmProjects/stat_data_processing/venv/lib/python3.6/site-packages/pyarrow/pandas_compat.py", line 356, in dataframe_to_arrays
    convert_types))
  File "/Users/mattlivingston/miniconda3/lib/python3.6/concurrent/futures/_base.py", line 586, in result_iterator
    yield fs.pop().result()
  File "/Users/mattlivingston/miniconda3/lib/python3.6/concurrent/futures/_base.py", line 425, in result
    return self.__get_result()
  File "/Users/mattlivingston/miniconda3/lib/python3.6/concurrent/futures/_base.py", line 384, in __get_result
    raise self._exception
  File "/Users/mattlivingston/miniconda3/lib/python3.6/concurrent/futures/thread.py", line 56, in run
    result = self.fn(*self.args, **self.kwargs)
  File "/Users/mattlivingston/PycharmProjects/stat_data_processing/venv/lib/python3.6/site-packages/pyarrow/pandas_compat.py", line 345, in convert_column
    return pa.array(col, from_pandas=True, type=ty)
  File "pyarrow/array.pxi", line 170, in pyarrow.lib.array (/Users/travis/build/BryanCutler/arrow-dist/arrow/python/build/temp.macosx-10.6-intel-3.6/lib.cxx:29224)
  File "pyarrow/array.pxi", line 70, in pyarrow.lib._ndarray_to_array (/Users/travis/build/BryanCutler/arrow-dist/arrow/python/build/temp.macosx-10.6-intel-3.6/lib.cxx:28465)
  File "pyarrow/error.pxi", line 87, in pyarrow.lib.check_status (/Users/travis/build/BryanCutler/arrow-dist/arrow/python/build/temp.macosx-10.6-intel-3.6/lib.cxx:8645)
pyarrow.lib.ArrowTypeError: Unsupported Python type for list items
(venv) it-mbp-mattlivingston:stat_data_processing mattlivingston$ 
```
