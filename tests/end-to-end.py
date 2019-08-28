import os
import sys
import time
import concurrent.futures as futures
import rtlib
import yenot.client as yclient
import yenot.tests

TEST_DATABASE = 'yenot_e2e_test'

def test_url(dbname):
    if 'YENOT_DB_URL' in os.environ:
        return os.environ['YENOT_DB_URL']
    # Fall back to local unix socket.  This is the url for unix domain socket.
    return 'postgresql:///{}'.format(dbname)

def init_database(dburl):
    r = os.system('{} ../yenot/scripts/init-database.py {} --full-recreate \
            --ddl-script=schema/databits.sql \
            --module=bitserver'.format(sys.executable, dburl))
    if r != 0:
        print('error exit')
        sys.exit(r)

def test_crud_bits(srvparams):
    with yenot.tests.server_running(**srvparams) as server:
        session = yclient.YenotSession(server.url)
        client = session.std_client()

        content = client.get('api/databits/bit/new')
        bittable = content.named_table('bit')
        bitrow = bittable.rows[0]
        bitrow.caption = 'Constitution of the United States'
        bitrow.data = """We the People of the United States, in Order to form a more perfect Union, establish Justice, insure domestic Tranquility, provide for the common defence, promote the general Welfare, and secure the Blessings of Liberty to ourselves and our Posterity, do ordain and establish this Constitution for the United States of America."""

        client.put('api/databits/bit/{}', bitrow.id, files={'bit': bittable.as_http_post_file()})

        content = client.get('api/databits/bit/new')
        bittable = content.named_table('bit')
        bitrow = bittable.rows[0]
        bitrow.caption = 'Declaration of Independence'
        bitrow.data = """When in the Course of human events, it becomes necessary for one people to dissolve the political bands which have connected them with another, and to assume among the powers of the earth, the separate and equal station to which the Laws of Nature and of Nature's God entitle them, a decent respect to the opinions of mankind requires that they should declare the causes which impel them to the separation."""
        client.put('api/databits/bit/{}', bitrow.id, files={'bit': bittable.as_http_post_file()})
        decl_inde_id = bitrow.id

        content = client.get('api/databits/bit/new')
        bittable = content.named_table('bit')
        bitrow = bittable.rows[0]
        bitrow.caption = 'Monroe Doctrine'
        bitrow.data = """It is impossible that the allied powers should extend their political system to any portion of either continent without endangering our peace and happiness; nor can anyone believe that our southern brethren, if left to themselves, would adopt it of their own accord. It is equally impossible, therefore, that we should behold such interposition in any form with indifference. If we look to the comparative strength and resources of Spain and those new Governments, and their distance from each other, it must be obvious that she can never subdue them. It is still the true policy of the United States to leave the parties to themselves, in hope that other powers will pursue the same course. . . ."""

        client.put('api/databits/bit/{}', bitrow.id, files={'bit': bittable.as_http_post_file()})

        content = client.get('api/databits/bits/list')
        found = [row for row in content.main_table().rows if row.caption == 'Monroe Doctrine']
        assert len(found) == 1

        client.delete('api/databits/bit/{}', found[0].id)
        content = client.get('api/databits/bits/list')
        found = [row for row in content.main_table().rows if row.caption == 'Monroe Doctrine']
        assert len(found) == 0

        content = client.get('api/databits/bit/{}', decl_inde_id)
        per = content.named_table('bit')
        assert per.rows[0].caption == 'Declaration of Independence'

        session.close()

def test_crud_tags(srvparams):
    with yenot.tests.server_running(**srvparams) as server:
        session = yclient.YenotSession(server.url)
        client = session.std_client()

        content = client.get('api/databits/tag/new')
        tagtable = content.named_table('tag')
        tagrow = tagtable.rows[0]
        tagrow.name = 'USA Documents'
        tagrow.description = 'founding documents of the USA'
        client.put('api/databits/tag/{}', tagrow.id, files={'tag': tagtable.as_http_post_file()})
        usadoc = tagrow.id

        content = client.get('api/databits/tag/{}', usadoc)
        assert content.main_table().rows[0].name == 'USA Documents'

        client.delete('api/databits/tag/{}', usadoc)

        session.close()

def test_basic_lists(srvparams):
    with yenot.tests.server_running(**srvparams) as server:
        session = yclient.YenotSession(server.url)
        client = session.std_client()

        content = client.get('api/databits/bits/list', frag='blessing')
        table = content.main_table()
        assert 'Constitution of the United States' in [row.caption for row in table.rows]
        assert 'Declaration of Independence' not in [row.caption for row in table.rows]

        content = client.get('api/databits/bits/list', frag='equal')
        table = content.main_table()
        assert 'Constitution of the United States' not in [row.caption for row in table.rows]
        assert 'Declaration of Independence' in [row.caption for row in table.rows]

        client.get('api/databits/tags/list')

        session.close()

if __name__ == '__main__':
    srvparams = {
            'dburl': test_url(TEST_DATABASE),
            'modules': ['bitserver']}

    init_database(test_url(TEST_DATABASE))
    test_crud_bits(srvparams)
    test_crud_tags(srvparams)
    test_basic_lists(srvparams)
