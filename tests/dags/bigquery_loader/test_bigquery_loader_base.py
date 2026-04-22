from util.miscutils import split_table_name


def test_split_table_name():
    source_schema, source_tablename = split_table_name("Retail.dbo.Calendar")
    assert source_schema == "dbo"
    assert source_tablename == "Calendar"

    source_schema, source_tablename = split_table_name("PCMC.APP_OPTIONAL_PROD")
    assert source_schema == "PCMC"
    assert source_tablename == "APP_OPTIONAL_PROD"

    source_schema = split_table_name("Retail.dbo.Calendar", True)
    assert source_schema == "dbo"
