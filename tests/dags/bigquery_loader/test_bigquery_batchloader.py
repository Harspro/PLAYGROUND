from util.miscutils import split_table_name


def test_split_table_name():
    source_schema, source_tablename = split_table_name("TML.TM_PROC p JOIN TML.TM_DOC d ON p.proc_id = d.proc_id")
    assert source_schema == "TML"
    assert source_tablename == "TM_PROC_p_JOIN_TML_TM_DOC_d_ON_p_proc_id_d_proc_id"
