from sqlalchemy.engine import Engine
from sqlalchemy import text
from src.db_tools import get_db_engine

def test_get_db_engine_returns_valid_object():

    engine = get_db_engine()
    assert isinstance(engine, Engine)

def test_database_connection_is_alive():

    engine = get_db_engine()
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1")).scalar()
            assert result == 1
    except Exception as e:
        assert False, f"Cannot connect to the database: {e}"