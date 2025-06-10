from sqlalchemy import inspect, MetaData, create_engine, Engine 
from alembic.migration import MigrationContext 
from alembic.autogenerate import compare_metadata
from typing import Optional, List, Dict, Any
from datetime import datetime
from sqlmodel import SQLModel
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
        
    def connect(self) -> Engine:
        """
        returns a SQLAlchemy engine connected to the database. 
        """
        return create_engine(self.connection_string)
        
    def get_online_db_schema(self) -> MigrationContext:
        """ 
        returns the online database schema as a MigrationContext object.
        """
        mc = MigrationContext.configure(self.engine.connect())
        return mc
    
    def create_table_from_model(self, model: type[SQLModel]):
        """
        Create a table from a SQLModel class.
        """
        model.metadata.create_all(self.engine, tables=[model.__table__])
    
    def get_local_metadata(self) -> MetaData:
        """
        Get metadata for the tables specified in self.tables
        """
        metadata = MetaData()
        for table in self.tables:
            # Add each table's metadata to our metadata object
            table.__table__.to_metadata(metadata)
        return metadata

    def produce_migrations(self) -> List[str]:
        """
        Generate migration scripts based on the differences between the local model and the online schema.
        """
        local_schema = self.get_local_metadata() 
        online_schema = self.get_online_db_schema()
        migrations = compare_metadata(online_schema, local_schema)
        
        return migrations

    def sync_local_to_online(self):
        """
        Synchronize local SQLModel definitions with online database tables.
        """

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
    
    print(db.produce_migrations())
    