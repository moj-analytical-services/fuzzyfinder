## Aim

- Given a record, we want to find a list of potentially-matching records in large table
- We want those records to be

- User should have the option of either:
  - Populating a on-disk SQLite db
  - Running everything in memory


## Stages

- Database build stage

- Lookup records

- Score records which have been looked up

## Abstractions

- A record.
    Main role is formatting the record, tokenising etc.
    Constain beta information about columns etc.
    - properties:
        - record as a dict
    - methods:
        - convert record into cleaned tokenised string for fts





- RecordComparison

    -properties
        The two records
    - methods
        Create a score for a record comparison




- RecordComparisonScorer
    Takes two records and computes a match score using probabilities

    - an additional function get_toke_probabilities(record, sqliteconnection)  so that record class is 'pure' (doesn't need to know about sqlite)

- MatchGetter
  - Give it a record and it gives you back potentially matching records


- SearchDatabase
  - methods

  - add record(Record)

  - add records from dataframe

  - prepare_for_matching()




## Interface


- establish database from dataframe. in mem option

- find potentially matching records from database with scores


conn = create_db(filename='mydb.db')

add_records_to_db(pddf)

prepare for matching()


match_getter = MatchGetter(record, SearchDatabase)


matches = get_scored__matches(record, SearchDatabase, as_pd = True)


## Places where need to check hitting cache:

-  comparison.get_token_proportion


## Timings:

`fake_1m.parquet` non parallel write db to `delete.db`, build stats optimise and vacuum:
0:02:19.547663

`fake_1m.parquet` parallel write db to `delete.db`
1:25.80

Takes about 17 seconds to turn coutners into single counter

To do next:

The genreator with executemany works well.

Repeat with the counters.

Need to write a map function for the counters that parallelises the reduce.

### Timings for real data

- first three files of real data, saved to disk as parquet

Baseline of main, commit 363fa05


faster_batch:

Option 1:  No primary key on df or token counts table.  Col counters saved across batches rather than written each batch:


INFO:fuzzyfinder.database:top of db
starting read
0:00:00.033590
starting batch write
0:00:02.857744
starting read
0:01:13.270006
starting batch write
0:01:16.152377
INFO:fuzzyfinder.database:starting to write all col counters
starting col counters write
0:02:27.330696
starting stats tables
0:02:34.124270
0:03:24.377918

token	token_count	token_proportion
0	JONES	13630	0.003968
1	JNS	14360	0.004180

Option 2:  Primary key on df and token counts table.  Col counters saved across batches rather than written each batch:

INFO:fuzzyfinder.database:top of db
starting read
0:00:00.019651

starting batch write
0:00:02.883229
starting read
0:01:24.239071
starting batch write
0:01:27.102970
INFO:fuzzyfinder.database:starting to write all col counters
starting col counters write
0:03:07.652419
starting stats tables
0:03:23.265033
0:04:18.627704



Option 3:  Primary key on df and token counts table.  Col counters written each batch:





main: