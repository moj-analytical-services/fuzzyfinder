import tempfile
import pytest
import os
import pandas as pd 

from fuzzyfinder.database import SearchDatabase
from fuzzyfinder.record import Record
cwd = os.path.dirname(os.path.abspath(__file__))

def test_1m():


    db_filename = tempfile.NamedTemporaryFile().name

    db = SearchDatabase(db_filename)
    
    path =  os.path.join(cwd, "data", "fake_1m.parquet")
    df = pd.read_parquet(path)
    df = df.reset_index()
    
    df = df.drop('group', axis=1)

    db.write_pandas_dataframe(df, 'index')
    
    db.build_or_replace_stats_tables()
    
    # Should be 1m records 
    sql = "select count(*) as c from df"
    r = db.conn.execute(sql).fetchall()
    assert r[0]["c"] == 1e6
    
    
    sql = "select count(*) as c from fts_target"
    r = db.conn.execute(sql).fetchall()
    assert r[0]["c"] == 1e6
    
    sql = "select * from first_name_token_counts where token = 'POPPY'" 
    r = db.conn.execute(sql).fetchall()
    assert r[0]["token_count"] == 4697
    


def test_two_dfs():


    db_filename = tempfile.NamedTemporaryFile().name

    db = SearchDatabase(db_filename)
    
    path =  os.path.join(cwd, "data", "fake_30000.parquet")
    df1 = pd.read_parquet(path)
    df1 = df1.reset_index()
    df1 = df1.drop('group', axis=1)
    
    path =  os.path.join(cwd, "data", "fake_300000.parquet")
    df2 = pd.read_parquet(path)
    df2 = df2.reset_index()
    df2['index'] = df2['index'] + 1e7
    df2 = df2.drop('group', axis=1)

    db.write_pandas_dataframe(df1, 'index', batch_size=8_765, write_column_counters=False)
    db.write_pandas_dataframe(df2, 'index')
    
    db.build_or_replace_stats_tables()
    
    # Should be 1m records 
    sql = "select count(*) as c from df"
    r = db.conn.execute(sql).fetchall()
    assert r[0]["c"] == 330_000
    
    sql = "select count(*) as c from fts_target"
    r = db.conn.execute(sql).fetchall()
    assert r[0]["c"] == 330_000
    
    sql = "select * from first_name_token_counts where token = 'POPPY'" 
    r = db.conn.execute(sql).fetchall()
    assert r[0]["token_count"] == 59 + 905
    
