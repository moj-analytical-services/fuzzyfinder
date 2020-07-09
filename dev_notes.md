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

    - an additional function get_toke_probabilities(record, sqliteconnection)  so that record class is 'pure' (doesn't need to know about sqlite)



- RecordComparison
    
    -properties
        The two records
    - methods
        Create a score for a record comparison




- RecordComparisonScorer
    Takes two records and computes a match score using probabilities

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


