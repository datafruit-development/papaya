from sqlmodel import SQLModel, create_engine, Session, text
from sqlalchemy import inspect, MetaData
from typing import Optional, List, Dict, Any
from datetime import datetime

"""
---- OPEN ISSUES ----

1. Type compatibility checks are too basic and may lead to false positives/negatives.
2. Altering existing tables to match the model schema is not implemented in sync_local_to_online.
3. Does not handle foreign key relations - need to implement a way to create, check, and sync foreign keys.
"""



class postgres_db:
    def __init__(self, connection_string: str, tables: list[type[SQLModel]]):
        self.connection_string = connection_string
        self.tables = tables
        self.engine = create_engine(self.connection_string)
        
    def connect(self) -> Session:
        """
        Create a SQLModel Session for database operations.
        Returns a SQLModel Session object.
        """
        return Session(self.engine)
        
    def list_online_tables(self) -> List[str]:
        """
        List all tables in the public schema.
        """
        inspector = inspect(self.engine)
        return inspector.get_table_names(schema='public')
    
    def get_online_table_schema(self, table_name: str) -> List[tuple[str, str]]:
        """ 
        Returns a list of the columns and their data types as tuples, [(column_name, data_type), ...]
        """
        inspector = inspect(self.engine)
        columns = inspector.get_columns(table_name, schema='public')
        return [(col['name'], str(col['type'])) for col in columns]
    
    def create_table_from_model(self, model: type[SQLModel]):
        """
        Create a table from a SQLModel class.
        """
        model.metadata.create_all(self.engine, tables=[model.__table__])
        
    def get_model_schema(self, model: type[SQLModel]) -> Dict[str, Any]:
        """
        Extract schema information from a SQLModel class.
        """
        columns = {}
        for column_name, column in model.__table__.columns.items():
            columns[column_name] = {
                'type': str(column.type),
                'nullable': column.nullable,
                'primary_key': column.primary_key,
                'unique': column.unique,
                'default': column.default,
                'index': column.index
            }
        return columns
    
    def compare_schemas(self, model: type[SQLModel], online_schema: List[tuple[str, str]]) -> Dict[str, Any]:
        """
        Compare local model schema with online table schema.
        Returns differences between schemas.
        """
        model_schema = self.get_model_schema(model)
        online_dict = {col[0]: col[1] for col in online_schema}
        
        differences = {
            'missing_columns': [],
            'extra_columns': [],
            'type_mismatches': []
        }
        
        # Check for missing columns in online table
        for col_name in model_schema:
            if col_name not in online_dict:
                differences['missing_columns'].append(col_name)
                
        # Check for extra columns in online table
        for col_name in online_dict:
            if col_name not in model_schema:
                differences['extra_columns'].append(col_name)
                
        # Check for type mismatches (basic comparison)
        for col_name in model_schema:
            if col_name in online_dict:
                model_type = str(model_schema[col_name]['type']).upper()
                online_type = online_dict[col_name].upper()
                # This comparison is probably too basic and will be buggy - needs to be improved
                if not self._types_compatible(model_type, online_type):
                    differences['type_mismatches'].append({
                        'column': col_name,
                        'model_type': model_type,
                        'online_type': online_type
                    })
                    
        return differences
    
    def _types_compatible(self, model_type: str, online_type: str) -> bool:
        """
        Check if two SQL types are compatible (simplified version).
        """
        # Normalize types for comparison
        type_mappings = {
            'INTEGER': ['INTEGER', 'INT', 'SERIAL'],
            'VARCHAR': ['VARCHAR', 'CHARACTER VARYING', 'TEXT'],
            'BOOLEAN': ['BOOLEAN', 'BOOL'],
            'TIMESTAMP': ['TIMESTAMP', 'TIMESTAMP WITHOUT TIME ZONE'],
            'TEXT': ['TEXT', 'VARCHAR', 'CHARACTER VARYING']
        }
        
        for base_type, variants in type_mappings.items():
            if any(variant in model_type for variant in variants) and \
               any(variant in online_type for variant in variants):
                return True
        
        return model_type in online_type or online_type in model_type
    
    def sync_local_to_online(self):
        """
        Synchronize local SQLModel definitions with online database tables.
        """
        online_tables = self.list_online_tables()
        
        for table in self.tables:
            table_name = table.__tablename__
            
            if table_name not in online_tables:
                print(f"Creating table: {table_name}")
                self.create_table_from_model(table)
            else:
                online_schema = self.get_online_table_schema(table_name)
                differences = self.compare_schemas(table, online_schema)
                
                if any(differences.values()):
                    print(f"Differences found in table {table_name}:")
                    if differences['missing_columns']:
                        print(f"  Missing columns: {differences['missing_columns']}")
                    if differences['extra_columns']:
                        print(f"  Extra columns: {differences['extra_columns']}")
                    if differences['type_mismatches']:
                        print(f"  Type mismatches: {differences['type_mismatches']}")
                    
                    # TODO: Implement table alteration logic here
                else:
                    print(f"Table {table_name} is up to date")

    def create_all_tables(self):
        """
        Create all tables defined in the models.
        """
        SQLModel.metadata.create_all(self.engine)

if __name__ == "__main__":
    import os
    from sqlmodel import Field
    from dotenv import load_dotenv
    load_dotenv()

    class users(SQLModel, table=True):
        id: Optional[int] = Field(default=None, primary_key=True)
        username: str = Field(unique=True)
        email: str = Field(unique=True)
        full_name: Optional[str] = None
        is_active: bool = Field(default=True)
        created_at: datetime = Field(default_factory=datetime.utcnow)
    
    class posts(SQLModel, table=True):
        id: Optional[int] = Field(default=None, primary_key=True)
        title: str = Field(index=True)
        content: str
        published: bool = Field(default=False)
        created_at: datetime = Field(default_factory=datetime.utcnow)
        updated_at: Optional[datetime] = None
    
    # Note: Connection string format for SQLAlchemy/SQLModel
    db = postgres_db(
        os.getenv("PG_DB_URL"),
        [users, posts]
    )
    
    online_tables = db.list_online_tables()
    print(f"Online tables: {online_tables}")
    
    if online_tables:
        print(f"Schema for {online_tables[0]}:")
        print(db.get_online_table_schema(online_tables[0]))
    
    db.sync_local_to_online()