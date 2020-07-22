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
db.write_pandas_dataframe(df_to_write)
```

Once you've finished adding records, optimise the database for search:


```python
db.update_token_stats_tables()
```

Now you can serach for potential matches

```python
search_dict = {"first_name": "john", "surname": "smith"}
search_rec = Record(search_dict, db.conn)
inder = MatchFinder(search_dict, db.conn, return_records_limit=50)
finder.find_potential_matches()
finder.found_records_as_df
```

