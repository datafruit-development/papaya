from sqlmodel import SQLModel, create_engine, Session, text
from sqlalchemy import inspect, MetaData
from typing import Optional, List, Dict, Any
from datetime import datetime
from dataclasses import dataclass
from enum import Enum
import networkx as nx 


"""
---- OPEN ISSUES ----

1. Type compatibility checks are too basic and may lead to false positives/negatives.
2. Altering existing tables to match the model schema is not implemented in sync_local_to_online.
3. Does not handle foreign key relations - need to implement a way to create, check, and sync foreign keys.
"""


# class for foreign key actions and relations
class FKAction(Enum): 
    # cascade, restrict, set null, set default, no action 
    CASCADE = "CASCADE"
    RESTRICT = "RESTRICT"
    SET_NULL = "SET NULL"
    SET_DEFAULT = "SET DEFAULT"
    NO_ACTION = "NO ACTION"
    
@dataclass
class ForeignKeyInfo:
    """Represents a foreign key constraint"""
    name: str
    source_table: str
    source_columns: List[str]
    target_table: str
    target_columns: List[str]
    on_delete: FKAction = FKAction.NO_ACTION
    on_update: FKAction = FKAction.NO_ACTION
    def __hash__(self):
        return hash(self.name)
    def get_signature(self) -> str: 
        """Create a unique signature for comparison, without the name"""
        return (f"{self.source_table}:{','.join(sorted(self.source_columns))}" 
                f"->{self.target_table}:{','.join(sorted(self.target_columns))}")
 

class ForeignKeyManager: 
    """Handles foreign key operations for schema synchronization"""
    def __init__(self, engine):
        self.engine = engine 
        self.inspector = inspect(engine)
        
    def get_online_foreign_keys(self, schema: str = 'public') -> Dict[str, List[ForeignKeyInfo]]: 
        """Extradt all foreign keys from existing database""" 
        foreign_keys = {}
        for table_name in self.inspector.get_table_names(schema=schema):
            fks = []
            for fk_info in self.inspector.get_foreign_keys(table_name, schema=schema):
                fk = ForeignKeyInfo(
                    name=fk_info['name'],
                    source_table=table_name,
                    source_columns=fk_info['constrained_columns'],
                    target_table=fk_info['referred_table'],
                    target_columns=fk_info['referred_columns'],
                    on_delete=FKAction(fk_info.get('ondelete', 'NO ACTION')),
                    on_update=FKAction(fk_info.get('onupdate', 'NO ACTION'))
                )
                fks.append(fk)
            foreign_keys[table_name] = fks
        
        return foreign_keys
    
    def get_model_foreign_keys(self, models: List[type[SQLModel]]) -> Dict[str, List[ForeignKeyInfo]]:
        """Extract foreign keys from SQLModel definitions"""
        foreign_keys = {}
        
        for model in models:
            table_name = model.__tablename__
            fks = []
            
            if hasattr(model, '__table__'):
                # Correct way to access foreign keys
                for column in model.__table__.columns:
                    if column.foreign_keys:
                        for fk in column.foreign_keys:
                            fk_info = ForeignKeyInfo(
                                name=fk.constraint.name or f"fk_{table_name}_{column.name}",
                                source_table=table_name,
                                source_columns=[column.name],
                                target_table=fk.column.table.name,
                                target_columns=[fk.column.name],
                                on_delete=FKAction(fk.ondelete or 'NO ACTION'),
                                on_update=FKAction(fk.onupdate or 'NO ACTION')
                            )
                            fks.append(fk_info)
            
            foreign_keys[table_name] = fks
        
        return foreign_keys  
        
    def compare_foreign_keys(self, model_fks: Dict[str, List[ForeignKeyInfo]], 
                            online_fks: Dict[str, List[ForeignKeyInfo]]) -> Dict [str, Any]:
        """Compare foreign keys between model and database"""
        differences = {
            'missing_fks': [],
            'extra_fks': [],
            'modified_fks': [],
        }
        
        all_tables = set(model_fks.keys()) | set(online_fks.keys())
        
        for table_name in all_tables:
            model_table_fks = {fk.name: fk for fk in model_fks.get(table_name, [])}
            online_table_fks = {fk.name: fk for fk in online_fks.get(table_name, [])}
            
            for fk_name, fk in model_table_fks.items():
                if fk_name not in online_table_fks:
                    differences['missing_fks'].append(fk)
                elif not self._fks_equal(fk, online_table_fks[fk_name]):
                    differences['modified_fks'].append({
                        'model_fk': fk,
                        'online_fk': online_table_fks[fk_name]
                    })
            
            for fk_name, fk in online_table_fks.items():
                if fk_name not in model_table_fks:
                    differences['extra_fks'].append(fk)
        
        return differences
    
    def _fks_equal(self, fk1: ForeignKeyInfo, fk2: ForeignKeyInfo) -> bool:
        """Compare two foreign key definitions for equality"""
        return (fk1.source_columns == fk2.source_columns and
                fk1.target_table == fk2.target_table and
                fk1.target_columns == fk2.target_columns and
                fk1.on_delete == fk2.on_delete and
                fk1.on_update == fk2.on_update)
    
    def generate_fk_creation_order(self, models: List[type[SQLModel]]) -> List[type[SQLModel]]:
        """Determine safe order for creating tables with foreign keys"""
        graph = nx.DiGraph()
        table_to_model = {model.__tablename__: model for model in models}
        
        for model in models:
            graph.add_node(model.__tablename__)
        
        model_fks = self.get_model_foreign_keys(models)
        for table_name, fks in model_fks.items():
            for fk in fks:
                if fk.target_table in table_to_model:
                    graph.add_edge(fk.target_table, table_name)
        
        try:
            sorted_tables = list(nx.topological_sort(graph))
            return [table_to_model[table] for table in sorted_tables]
        except nx.NetworkXError as e:
            raise ValueError(f"Circular dependency detected in foreign keys: {e}")
    
    def generate_add_fk_ddl(self, fk: ForeignKeyInfo) -> str:
        """Generate DDL to add a foreign key constraint"""
        source_cols = ', '.join(fk.source_columns)
        target_cols = ', '.join(fk.target_columns)
        
        ddl = f"""ALTER TABLE {fk.source_table} 
                ADD CONSTRAINT {fk.name} 
                FOREIGN KEY ({source_cols}) 
                REFERENCES {fk.target_table} ({target_cols})"""
        
        if fk.on_delete != FKAction.NO_ACTION:
            ddl += f"\n    ON DELETE {fk.on_delete.value}"
        
        if fk.on_update != FKAction.NO_ACTION:
            ddl += f"\n    ON UPDATE {fk.on_update.value}"
        
        return ddl + ";"
    
    def generate_drop_fk_ddl(self, fk: ForeignKeyInfo) -> str:
        """Generate DDL to drop a foreign key constraint"""
        return f"ALTER TABLE {fk.source_table} DROP CONSTRAINT IF EXISTS {fk.name};"

        
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