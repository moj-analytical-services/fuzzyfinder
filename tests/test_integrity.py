import tempfile
import string

from fuzzyfinder.database import SearchDatabaseBuilder


def test_integrity():

    # Want to test database insert functionality to check that:
    # 1. It's not possible to add the same unique_id twice
    # 2. Token counts are computed correctly when you try and add the same unique_id twice
    db_filename = tempfile.NamedTemporaryFile().name
    db = SearchDatabaseBuilder(db_filename)
    records = []
    for char in list(string.ascii_lowercase):
        record = {"unique_id": char, "value": char}
        records.append(record)

    db.write_list_dicts_parallel(records, unique_id_col='unique_id',  batch_size=5)

    sql_df_count = """
    select count(*) as count from df
    """

    results = db.conn.execute(sql_df_count)
    results = results.fetchall()
    assert results[0]["count"] == 26

    db2 = SearchDatabaseBuilder(db_filename)
    db2.write_list_dicts_parallel(records, unique_id_col='unique_id', batch_size=10)

    results = db2.conn.execute(sql_df_count)
    results = results.fetchall()
    assert results[0]["count"] == 26

    # At the moment, all tokens should have a count of 1

    sql_tkn_count = """
    select
        max(token_count) as max,
        min(token_count) as min,
        count(*) as count
    from value_token_counts
    """

    results = db2.conn.execute(sql_tkn_count)
    results = results.fetchall()
    assert results[0]["max"] == 1
    assert results[0]["min"] == 1
    assert results[0]["count"] == 26

    # Note records deliberately includes 29 items now, we expect three new
    for char in ["a", "b", "c"]:
        record = {"unique_id": f"{char}_2", "value": char}
        records.append(record)

    db2.write_list_dicts_parallel(records, unique_id_col='unique_id', batch_size=10)

    results = db2.conn.execute(sql_df_count)
    results = results.fetchall()

    assert results[0]["count"] == 29

    results = db2.conn.execute(sql_tkn_count)
    results = results.fetchall()
    assert results[0]["max"] == 2
    assert results[0]["min"] == 1
    assert results[0]["count"] == 26

    sql_count_a = """
    select token_count
    from value_token_counts
    where token = 'A'
    """

    results = db.conn.execute(sql_count_a)
    results = results.fetchall()
    assert results[0]["token_count"] == 2
