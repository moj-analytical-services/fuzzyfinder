import tempfile
import pytest


from fuzzyfinder.database import SearchDatabase
from fuzzyfinder.record import Record


def test_record():


    db_filename = tempfile.NamedTemporaryFile().name


    db = SearchDatabase(db_filename)
    rec1 = {'unique_id': "rectest_1", 'first_name': 'robin', 'surname': 'linacre'}
    rec2 = {'unique_id': "rectest_2", 'first_name': 'robyn', 'surname': 'linaker'}
    rec3 = {'unique_id': "rectest_3", 'first_name': 'robin', 'surname': 'linacre'}


    dicts = [rec1, rec2, rec3]
    db.write_list_dicts_parallel(dicts, unique_id_col='unique_id')

    db.build_or_replace_stats_tables()

    # You have to be careful with caching here - deliberately do not include unique id here
    # Different unique ids should be assignd
    search_rec = {'unique_id': 'serach_rec_1', 'first_name': 'robin', 'surname': "smith"}
    
    r = Record(search_rec, 'unique_id', db.conn)
    
    assert 'ROBIN' in r.tokens_in_order_of_rarity
    assert 'SMITH' not in r.tokens_in_order_of_rarity
    
    search_rec = {'unique_id': 'serach_rec_2', 'first_name': 'dave', 'surname': "linacre"}
    r = Record(search_rec, 'unique_id', db.conn)
    
    assert 'LINACRE' in r.tokens_in_order_of_rarity
    assert 'DAVE' not in r.tokens_in_order_of_rarity

    

    
