import uuid
import yenot.backend.api as api

app = api.get_global_app()

@app.get('/api/databits/tags/list', name='get_api_databits_tags_list', \
        report_title='Tag List')
def get_api_databits_tags_list():
    select = """
select *
from databits.tags
order by name
"""

    params = {}

    results = api.Results(default_title=True)
    with app.dbconn() as conn:
        cm = api.ColumnMap(\
                id=api.cgen.lms_databits_tag.surrogate(),
                name=api.cgen.lms_databits_tag.name(url_key='id', represents=True))
        results.tables['tags', True] = api.sql_tab2(conn, select, params, cm)
    return results.json_out()

def _get_api_databits_tag(a_id=None, newrow=False):
    select = """
select *
from databits.tags
where /*WHERE*/"""

    wheres = []
    params = {}
    if a_id != None:
        params['i'] = a_id
        wheres.append("tags.id=%(i)s")
    if newrow:
        wheres.append("False")

    assert len(wheres) == 1
    select = select.replace("/*WHERE*/", wheres[0])

    results = api.Results()
    with app.dbconn() as conn:
        columns, rows = api.sql_tab2(conn, select, params)

        if newrow:
            def default_row(index, row):
                row.id = str(uuid.uuid1())
            rows = api.tab2_rows_default(columns, [None], default_row)

        results.tables['tag', True] = columns, rows
    return results

@app.get('/api/databits/tag/<a_id>', name='get_api_databits_tag')
def get_api_databits_tag(a_id):
    results = _get_api_databits_tag(a_id)
    return results.json_out()

@app.get('/api/databits/tag/new', name='get_api_databits_tag_new')
def get_api_databits_tag_new():
    results = _get_api_databits_tag(newrow=True)
    results.keys['new_row'] = True
    return results.json_out()

@app.put('/api/databits/tag/<tag_id>', name='put_api_databits_tag')
def put_api_databits_tag(tag_id):
    tag = api.table_from_tab2('tag', amendments=['id'], options=['name', 'description'])

    if len(tag.rows) != 1 or tag.rows[0].id != tag_id:
        raise api.UserError('invalid-input', 'There must be exactly one row and it must match the url.')

    with app.dbconn() as conn:
        with api.writeblock(conn) as w:
            w.upsert_rows('databits.tags', tag)
        conn.commit()

    return api.Results().json_out()

@app.delete('/api/databits/tag/<tag_id>', name='delete_api_databits_tag')
def delete_api_databits_tag(tag_id):
    delete_sql = """
delete from databits.tags where id=%(tid)s;
"""

    with app.dbconn() as conn:
        api.sql_void(conn, delete_sql, {'tid': tag_id})
        conn.commit()

    return api.Results().json_out()
