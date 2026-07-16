from comms import check_schema

def test_schemas_exist():
   df = check_schema()
   tables = df['table_name'].unique().tolist()
   assert "components" in tables
