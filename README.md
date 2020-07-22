![Test](https://github.com/moj-analytical-services/fuzzyfinder/workflows/Test/badge.svg)

# fuzzyfinder

Given a large table of records, fuzzy search for a record, and return a table of potential matches, with a match score


First build a database of your records.  If you provide a path, the db is persisted to disk.  Otherwise it's in-memory:
```python
from fuzzyfinder.database import SearchDatabaseBuilder
db = SearchDatabaseBuilder()
```

Add some records:

```python
df = pd.read_csv('mytable.csv')

# Note you should supply the name of a unique_id column so they can be uniquely identified later
db.write_pandas_dataframe(df_to_write, unique_id_col='unique_id')
```

Once you've finished adding records, optimise the database for search:


```python
db.build_or_replace_stats_tables()
```

Now you can serach for potential matches

```python
search_dict = {"first_name": "john", "surname": "smith"}
db.find_potential_matches_as_pandas(search_dict)
```

