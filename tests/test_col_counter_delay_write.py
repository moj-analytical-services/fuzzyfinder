import tempfile
import pytest


from fuzzyfinder.database import SearchDatabase


def test_integrity():


    # Want to test database insert functionality to check that:
    # 1. It's not possible to add the same unique_id twice
    # 2. Token counts are computed correctly when you try and add the same unique_id twice
   
    db_filename = tempfile.NamedTemporaryFile().name
    

    db = SearchDatabase(db_filename)

    rec_tokens = []
    rec_tokens.extend(["a"] * 1)
    rec_tokens.extend(["b"] * 2)
    rec_tokens.extend(["c"] * 3)
    rec_tokens.extend(["d"] * 4)

    records = []
    for rec_num, char in enumerate(rec_tokens):
        record = {"unique_id": rec_num, "value": char}
        records.append(record)

    db.write_list_dicts_parallel(records, unique_id_col='unique_id', batch_size=5, write_column_counters=False)

    # should be out of sync

    # Add another 10 As.  Now there are 11 in 20
    records = []
    for i in range(10, 20):
        record = {"unique_id": i, "value": "a"}
        records.append(record)

    db.write_list_dicts_parallel(records, unique_id_col='unique_id',  batch_size=5, write_column_counters=False)

    # Status 
    assert db.get_value_from_db_state_table('col_counters_in_sync') == 'false'

    db.write_all_col_counters_to_db()

    assert db.get_value_from_db_state_table('col_counters_in_sync') == 'true'

    db._update_token_stats_tables()

    sql_tkn_count = """
    select token_proportion

    from value_token_counts
    where token = 'A'
    """

    results = db.conn.execute(sql_tkn_count)
    results = results.fetchall()
    assert results[0]["token_proportion"] == 0.55

    # Add another 10 As, with repeated IDs, so they should be skipped
    records = []
    for i in range(10, 20):
        record = {"unique_id": i, "value": "a"}
        records.append(record)

    
    db.write_list_dicts_parallel(records,unique_id_col='unique_id',  batch_size=5)

    db._update_token_stats_tables()

    sql_tkn_count = """
    select token_proportion

    from value_token_counts
    where token = 'A'
    """

    results = db.conn.execute(sql_tkn_count)
    results = results.fetchall()
    assert results[0]["token_proportion"] == 0.55

    # Token proportions should sum to 1
    sql_tkn_count = """
    select sum(token_proportion) as sum

    from value_token_counts

    """
    results = db.conn.execute(sql_tkn_count)
    results = results.fetchall()
    assert results[0]["sum"] == 1.00

    db2 = SearchDatabase(db_filename)

    assert db.get_value_from_db_state_table('col_counters_in_sync') == 'true'

    db2.write_list_dicts_parallel(records, unique_id_col='unique_id',  batch_size=5, write_column_counters=False)

    with pytest.warns(UserWarning):
        db3 = SearchDatabase(db_filename)

    assert db.get_value_from_db_state_table('col_counters_in_sync') == 'false'

