import subprocess
import pathlib

SCHEMAS_LIST = ['public', 'wine_decider', 'crawler', 'checks_files', 'scrapping', 'events', 'commands', 'commands_data', "mapping", "negociant"]


def _get_alembic_dir():
    main_dir = pathlib.Path(__file__).parent.parent / 'api_dbt'
    return str(main_dir)


def delete_all_tables(db_connection):

    cursor = db_connection.cursor()
    for schema in SCHEMAS_LIST:
        cursor.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
    cursor.execute("CREATE SCHEMA public")
    cursor.close()


def get_tables_names(db_connection, schema_name: str):
    cursor = db_connection.cursor()

    query = f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{schema_name}' "
    cursor.execute(query)
    res = cursor.fetchall()
    cursor.close()
    return {table[0] for table in res}


def run_alembic_upgrade():

    commands = ["alembic", "upgrade", "head"]
    res = subprocess.run(commands, cwd=_get_alembic_dir(), check=True)
    if res.returncode == 1:
        raise Exception("Alembic upgrade has failed")


def run_alembic_downgrade():

    commands = ["alembic", "downgrade", "base"]
    res = subprocess.run(commands, cwd=_get_alembic_dir(), check=True)
    if res.returncode == 1:
        raise Exception("Alembic downgrade has failed")


def test_migrations(db_connection):

    delete_all_tables(db_connection)

    run_alembic_upgrade()

    # Test tables number
    public_tables = get_tables_names(db_connection, schema_name='public')

    assert 182 <= len(public_tables)

    tables = get_tables_names(db_connection, schema_name='wine_decider')
    assert tables == {
        "webcheck_store_volumes",
        "webcheck",
        "mapping_wines",
        "mapping_web_sites",
        "mapping_formats",
        "mapping_web_site_market",
    }

    scrapping_tables = get_tables_names(db_connection, schema_name='scrapping')
    assert scrapping_tables == {
        "wine_list",
        "wine_list_scrapping_params",
        "new_wine_list_alert",
        "new_wine_list_alert_data",
        "new_wine_list_alert_batch",
        "new_wine_list_alert_batch_comment"
    }

    negociant_tables = get_tables_names(db_connection, schema_name='negociant')
    assert negociant_tables == {
        "customer",
        "distribution_channel",
        "partner",
        "supplier",
        "sale_type",
        "transactions",
        "files",
        "presentation",
        "presentation_params",
        "allocation",
        "presentation_params_wine"
    }

    mapping_tables = get_tables_names(db_connection, schema_name='mapping')
    assert mapping_tables == {
        "country",
        "format",
        "negociant_customer",
        "negociant_distribution_channel",
        "negociant_partner",
        "negociant_supplier",
        "wine",
        "negociant_sale_type",
    }

    run_alembic_downgrade()

    # Test tables number
    public_tables_after_downgrade = get_tables_names(db_connection, schema_name='public')

    assert 182 <= len(public_tables_after_downgrade)

    wd_tables = get_tables_names(db_connection, schema_name='wine_decider')
    # Tables too dangerous to drop in downgrades
    assert wd_tables == {"webcheck_store_volumes", "webcheck"}

