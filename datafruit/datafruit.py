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
        
    def __eq__(self, other: Any) -> bool : 
        if not isinstance(other, ForeignKeyInfo):
            return NotImplemented
            
        # Compare everything except the name, which can differ.
        if self.get_signature() != other.get_signature():
            return False
        if self.on_delete != other.on_delete:
            return False
        if self.on_update != other.on_update:
            return False
            
        return True
 

class ForeignKeyManager: 
    """Handles foreign key operations for schema synchronization"""
    def __init__(self, engine):
        self.engine = engine 
        self.inspector = inspect(engine)
        
    def get_online_foreign_keys(self, schema: str = 'public') -> Dict[str, List[ForeignKeyInfo]]: 
        """Extract all foreign keys from existing database"""
        foreign_keys: Dict[str, List[ForeignKeyInfo]] = {}
        fk_query = text("""
            SELECT
                tc.constraint_name,
                tc.table_name AS source_table,
                kcu.column_name AS source_column,
                ccu.table_name AS target_table,
                ccu.column_name AS target_column,
                rc.update_rule AS on_update,
                rc.delete_rule AS on_delete
            FROM information_schema.table_constraints AS tc
            JOIN information_schema.key_column_usage AS kcu
              ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema
            JOIN information_schema.constraint_column_usage AS ccu
              ON ccu.constraint_name = tc.constraint_name AND ccu.table_schema = tc.table_schema
            JOIN information_schema.referential_constraints AS rc
              ON tc.constraint_name = rc.constraint_name AND tc.table_schema = rc.constraint_schema
            WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_schema = :schema
        """)
        
        with self.engine.connect() as connection:
            result = connection.execute(fk_query, {'schema': schema})
            
            # Process results into ForeignKeyInfo objects, grouped by constraint name
            processed_fks: Dict[str, ForeignKeyInfo] = {}
            for row in result:
                if row.constraint_name not in processed_fks:
                    processed_fks[row.constraint_name] = ForeignKeyInfo(
                        name=row.constraint_name,
                        source_table=row.source_table,
                        source_columns=[row.source_column],
                        target_table=row.target_table,
                        target_columns=[row.target_column],
                        on_update=FKAction(row.on_update),
                        on_delete=FKAction(row.on_delete)
                    )
                else:
                    # Append columns for composite keys
                    processed_fks[row.constraint_name].source_columns.append(row.source_column)
                    processed_fks[row.constraint_name].target_columns.append(row.target_column)

            # Group by source table name for the final output
            for fk in processed_fks.values():
                if fk.source_table not in foreign_keys:
                    foreign_keys[fk.source_table] = []
                foreign_keys[fk.source_table].append(fk)

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
        
        # dictionaries into maps of {name: fk_object} for efficient lookup
        model_fk_map = {fk.name: fk for fks in model_fks.values() for fk in fks}
        online_fk_map = {fk.name: fk for fks in online_fks.values() for fk in fks}
        
        model_fk_names = set(model_fk_map.keys())
        online_fk_names = set(online_fk_map.keys())
        
        # find missing FKs in model but not in DB
        for fk_name in model_fk_names - online_fk_names:
            differences['missing_fks'].append(model_fk_map[fk_name])
            
        # find extra FKs in DB but not in model
        for fk_name in online_fk_names - model_fk_names:
            differences['extra_fks'].append(online_fk_map[fk_name])
            
        # find modified FKs (in both but different)
        for fk_name in model_fk_names & online_fk_names:
            model_fk = model_fk_map[fk_name]
            online_fk = online_fk_map[fk_name]
            if not self._fks_equal(model_fk, online_fk):
                differences['modified_fks'].append({
                    'model_fk': model_fk,
                    'online_fk': online_fk
                })
        
        return differences
    def _fks_equal(self, fk1: ForeignKeyInfo, fk2: ForeignKeyInfo) -> bool:
        """Compare two foreign key definitions for equality"""
        if fk1.get_signature() != fk2.get_signature():
            return False
        if fk1.on_delete != fk2.on_delete:
            return False
        if fk1.on_update != fk2.on_update:
            return False
        return True
    
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
                    graph.add_edge(table_name, fk.target_table)
        
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