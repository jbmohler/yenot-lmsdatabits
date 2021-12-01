import uuid
import yenot.backend.api as api

app = api.get_global_app()


def get_api_databits_bits_list_prompts():
    return api.PromptList(frag=api.cgen.basic(label="Search"), __order__=["frag"])


@app.get(
    "/api/databits/bits/list",
    name="get_api_databits_bits_list",
    report_prompts=get_api_databits_bits_list_prompts,
    report_title="Data Bit List",
)
def get_api_databits_bits_list(request):
    frag = request.query.get("frag", None)
    tag = request.query.get("tag_id", None)

    select = """
select bits.id, bits.caption, bits.data, bits.website
from databits.bits
join databits.perfts_search fts on bits.id=fts.id
where /*WHERE*/
"""

    params = {}
    wheres = []
    if frag != None and frag != "":
        params["frag"] = api.sanitize_fts(frag)
        wheres.append("fts.fts_search @@ to_tsquery(%(frag)s)")
    if tag != None:
        params["tag"] = tag
        wheres.append(
            "(select count(*) from databits.tagbits where tag_id=%(tag)s and bit_id=bits.id)>0"
        )

    if len(wheres) == 0:
        wheres.append("True")
    select = select.replace("/*WHERE*/", " and ".join(wheres))

    results = api.Results(default_title=True)
    with app.dbconn() as conn:
        cm = api.ColumnMap(
            id=api.cgen.lms_bits_persona.surrogate(),
            l_name=api.cgen.lms_bits_persona.name(url_key="id", represents=True),
        )
        results.tables["bits", True] = api.sql_tab2(conn, select, params, cm)
    return results.json_out()


def _get_api_databits_bit(a_id=None, newrow=False):
    select = """
select *
from databits.bits
where /*WHERE*/"""

    wheres = []
    params = {}
    if a_id != None:
        params["i"] = a_id
        wheres.append("bits.id=%(i)s")
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

        results.tables["bit", True] = columns, rows
    return results


@app.get("/api/databits/bit/<a_id>", name="get_api_databits_bit")
def get_api_databits_bit(a_id):
    results = _get_api_databits_bit(a_id)
    return results.json_out()


@app.get("/api/databits/bit/new", name="get_api_databits_bit_new")
def get_api_databits_bit_new():
    results = _get_api_databits_bit(newrow=True)
    results.keys["new_row"] = True
    return results.json_out()


@app.put("/api/databits/bit/<bit_id>", name="put_api_databits_bit")
def put_api_databits_bit(bit_id):
    acc = api.table_from_tab2(
        "bit",
        amendments=["id"],
        options=["caption", "data", "website", "uname", "pword"],
    )

    if len(acc.rows) != 1 or acc.rows[0].id != bit_id:
        raise api.UserError(
            "invalid-input", "There must be exactly one row and it must match the url."
        )

    with app.dbconn() as conn:
        with api.writeblock(conn) as w:
            w.upsert_rows("databits.bits", acc)
        conn.commit()

    return api.Results().json_out()


@app.delete("/api/databits/bit/<bit_id>", name="delete_api_databits_bit")
def delete_api_databits_bit(bit_id):
    delete_sql = """
delete from databits.bits where id=%(bid)s;
"""

    with app.dbconn() as conn:
        api.sql_void(conn, delete_sql, {"bid": bit_id})
        conn.commit()

    return api.Results().json_out()
