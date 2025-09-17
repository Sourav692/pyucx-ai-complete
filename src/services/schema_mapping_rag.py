"""
Schema Mapping RAG Service using ChromaDB

This service provides dynamic schema-to-catalog mapping using RAG (Retrieval Augmented Generation)
with ChromaDB as the vector database. It allows the PyUCX framework to dynamically convert
table references based on stored schema mappings.

Example mappings:
- Input: sales.table_name -> Output: catalog2.sales.table_name
- Input: analytics.table_name -> Output: catalog1.analytics.table_name
"""

import os
import json
import logging
from typing import Dict, List, Optional, Any, Tuple
from pathlib import Path
import chromadb
from chromadb.config import Settings
from chromadb.utils import embedding_functions

logger = logging.getLogger(__name__)


class SchemaMapping:
    """Data class to represent schema mapping information."""

    def __init__(self, source_schema: str, target_catalog: str, target_schema: str = None,
                 description: str = "", metadata: Dict[str, Any] = None):
        self.source_schema = source_schema
        self.target_catalog = target_catalog
        self.target_schema = target_schema or source_schema  # Keep same schema name by default
        self.description = description
        self.metadata = metadata or {}

    def to_dict(self) -> Dict[str, Any]:
        # Flatten metadata for ChromaDB compatibility
        result = {
            "source_schema": self.source_schema,
            "target_catalog": self.target_catalog,
            "target_schema": self.target_schema,
            "description": self.description,
        }
        # Add metadata fields directly to avoid nested dictionaries
        if self.metadata:
            for key, value in self.metadata.items():
                result[f"meta_{key}"] = value
        return result

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'SchemaMapping':
        # Extract metadata fields that start with "meta_"
        metadata = {}
        for key, value in data.items():
            if key.startswith("meta_"):
                metadata[key[5:]] = value  # Remove "meta_" prefix

        return cls(
            source_schema=data["source_schema"],
            target_catalog=data["target_catalog"],
            target_schema=data.get("target_schema"),
            description=data.get("description", ""),
            metadata=metadata
        )


class SchemaMappingRAG:
    """RAG service for dynamic schema-to-catalog mapping using ChromaDB."""

    def __init__(self, db_path: str = "data/chroma_db", collection_name: str = "schema_mappings"):
        """
        Initialize the Schema Mapping RAG service.

        Args:
            db_path: Path to the ChromaDB database directory
            collection_name: Name of the ChromaDB collection
        """
        self.db_path = Path(db_path)
        self.collection_name = collection_name
        self.client = None
        self.collection = None

        # Create database directory if it doesn't exist
        self.db_path.mkdir(parents=True, exist_ok=True)

        self._initialize_chromadb()
        self._setup_default_mappings()

    def _initialize_chromadb(self) -> None:
        """Initialize ChromaDB client and collection."""
        try:
            # Initialize ChromaDB client
            self.client = chromadb.PersistentClient(
                path=str(self.db_path),
                settings=Settings(
                    anonymized_telemetry=False,
                    allow_reset=True
                )
            )

            # Create or get collection with embedding function
            embedding_function = embedding_functions.DefaultEmbeddingFunction()

            try:
                self.collection = self.client.get_collection(
                    name=self.collection_name,
                    embedding_function=embedding_function
                )
                logger.info(f"Connected to existing collection: {self.collection_name}")
            except Exception:
                self.collection = self.client.create_collection(
                    name=self.collection_name,
                    embedding_function=embedding_function,
                    metadata={"description": "Schema to catalog mappings for Unity Catalog migration"}
                )
                logger.info(f"Created new collection: {self.collection_name}")

        except Exception as e:
            logger.error(f"Failed to initialize ChromaDB: {e}")
            raise

    def _setup_default_mappings(self) -> None:
        """Setup default schema mappings if collection is empty."""
        try:
            # Check if collection has any data
            count = self.collection.count()
            if count > 0:
                logger.info(f"Collection already has {count} mappings")
                return

            # Default mappings based on your requirements
            default_mappings = [
                SchemaMapping(
                    source_schema="sales",
                    target_catalog="catalog2",
                    target_schema="sales",
                    description="Sales domain data including transactions, customers, and products",
                    metadata={"domain": "sales", "priority": "high", "data_sensitivity": "medium"}
                ),
                SchemaMapping(
                    source_schema="analytics",
                    target_catalog="catalog1",
                    target_schema="analytics",
                    description="Analytics domain data including user behavior, events, and metrics",
                    metadata={"domain": "analytics", "priority": "high", "data_sensitivity": "low"}
                ),
                SchemaMapping(
                    source_schema="finance",
                    target_catalog="catalog2",
                    target_schema="finance",
                    description="Financial data including accounting, budgets, and reporting",
                    metadata={"domain": "finance", "priority": "critical", "data_sensitivity": "high"}
                ),
                SchemaMapping(
                    source_schema="marketing",
                    target_catalog="catalog1",
                    target_schema="marketing",
                    description="Marketing data including campaigns, leads, and attribution",
                    metadata={"domain": "marketing", "priority": "medium", "data_sensitivity": "medium"}
                ),
                SchemaMapping(
                    source_schema="operations",
                    target_catalog="catalog3",
                    target_schema="operations",
                    description="Operational data including logistics, inventory, and supply chain",
                    metadata={"domain": "operations", "priority": "medium", "data_sensitivity": "low"}
                )
            ]

            self.add_mappings(default_mappings)
            logger.info(f"Added {len(default_mappings)} default schema mappings")

        except Exception as e:
            logger.error(f"Failed to setup default mappings: {e}")

    def add_mapping(self, mapping: SchemaMapping) -> None:
        """Add a single schema mapping to the database."""
        self.add_mappings([mapping])

    def add_mappings(self, mappings: List[SchemaMapping]) -> None:
        """Add multiple schema mappings to the database."""
        try:
            documents = []
            metadatas = []
            ids = []

            for mapping in mappings:
                # Create document text for embedding
                document_text = f"""
                Source Schema: {mapping.source_schema}
                Target Catalog: {mapping.target_catalog}
                Target Schema: {mapping.target_schema}
                Description: {mapping.description}
                Domain: {mapping.metadata.get('domain', 'unknown')}
                Priority: {mapping.metadata.get('priority', 'medium')}
                Data Sensitivity: {mapping.metadata.get('data_sensitivity', 'medium')}
                """.strip()

                documents.append(document_text)
                # Use flattened dictionary for ChromaDB
                metadata_dict = mapping.to_dict()
                metadatas.append(metadata_dict)
                ids.append(f"mapping_{mapping.source_schema}")

            # Add to ChromaDB collection
            self.collection.add(
                documents=documents,
                metadatas=metadatas,
                ids=ids
            )

            logger.info(f"Successfully added {len(mappings)} schema mappings")

        except Exception as e:
            logger.error(f"Failed to add mappings: {e}")
            raise

    def get_catalog_mapping(self, source_schema: str) -> Optional[SchemaMapping]:
        """
        Get the catalog mapping for a given source schema.

        Args:
            source_schema: The source schema name to look up

        Returns:
            SchemaMapping object if found, None otherwise
        """
        try:
            # First try exact match
            results = self.collection.query(
                query_texts=[f"Source Schema: {source_schema}"],
                n_results=1,
                where={"source_schema": source_schema}
            )

            if results['metadatas'] and len(results['metadatas'][0]) > 0:
                metadata = results['metadatas'][0][0]
                return SchemaMapping.from_dict(metadata)

            # If no exact match, try semantic search
            results = self.collection.query(
                query_texts=[f"schema {source_schema} database table mapping"],
                n_results=3
            )

            # Look for close matches in the results
            for i, metadata in enumerate(results['metadatas'][0]):
                if metadata['source_schema'].lower() == source_schema.lower():
                    return SchemaMapping.from_dict(metadata)

            logger.warning(f"No mapping found for schema: {source_schema}")
            return None

        except Exception as e:
            logger.error(f"Failed to get catalog mapping for {source_schema}: {e}")
            return None

    def get_three_part_name(self, table_reference: str) -> str:
        """
        Convert a table reference to three-part naming using dynamic catalog mapping.

        Args:
            table_reference: Table reference in format 'schema.table' or 'table'

        Returns:
            Three-part name in format 'catalog.schema.table'
        """
        try:
            parts = table_reference.split('.')

            if len(parts) == 1:
                # Just table name, need to determine schema and catalog
                table_name = parts[0]
                logger.warning(f"No schema specified for table: {table_name}, using default mapping")
                return f"default_catalog.default_schema.{table_name}"

            elif len(parts) == 2:
                # schema.table format
                source_schema, table_name = parts
                mapping = self.get_catalog_mapping(source_schema)

                if mapping:
                    return f"{mapping.target_catalog}.{mapping.target_schema}.{table_name}"
                else:
                    # Fallback to default catalog
                    logger.warning(f"No mapping found for schema {source_schema}, using default")
                    return f"default_catalog.{source_schema}.{table_name}"

            elif len(parts) == 3:
                # Already three-part naming, return as-is
                return table_reference

            else:
                logger.error(f"Invalid table reference format: {table_reference}")
                return table_reference

        except Exception as e:
            logger.error(f"Failed to convert table reference {table_reference}: {e}")
            return table_reference

    def list_all_mappings(self) -> List[SchemaMapping]:
        """Get all schema mappings from the database."""
        try:
            # Get all documents in the collection
            results = self.collection.get()

            mappings = []
            for metadata in results['metadatas']:
                mappings.append(SchemaMapping.from_dict(metadata))

            return mappings

        except Exception as e:
            logger.error(f"Failed to list mappings: {e}")
            return []

    def update_mapping(self, source_schema: str, new_mapping: SchemaMapping) -> bool:
        """Update an existing schema mapping."""
        try:
            # Delete existing mapping
            self.collection.delete(ids=[f"mapping_{source_schema}"])

            # Add new mapping
            self.add_mapping(new_mapping)

            logger.info(f"Updated mapping for schema: {source_schema}")
            return True

        except Exception as e:
            logger.error(f"Failed to update mapping for {source_schema}: {e}")
            return False

    def delete_mapping(self, source_schema: str) -> bool:
        """Delete a schema mapping."""
        try:
            self.collection.delete(ids=[f"mapping_{source_schema}"])
            logger.info(f"Deleted mapping for schema: {source_schema}")
            return True

        except Exception as e:
            logger.error(f"Failed to delete mapping for {source_schema}: {e}")
            return False

    def search_mappings(self, query: str, n_results: int = 5) -> List[Tuple[SchemaMapping, float]]:
        """
        Search schema mappings using semantic similarity.

        Args:
            query: Search query
            n_results: Number of results to return

        Returns:
            List of tuples containing (SchemaMapping, similarity_score)
        """
        try:
            results = self.collection.query(
                query_texts=[query],
                n_results=n_results
            )

            mappings_with_scores = []
            for i, metadata in enumerate(results['metadatas'][0]):
                mapping = SchemaMapping.from_dict(metadata)
                # ChromaDB returns distances, convert to similarity score
                distance = results['distances'][0][i]
                similarity = 1.0 - distance  # Assuming cosine distance
                mappings_with_scores.append((mapping, similarity))

            return mappings_with_scores

        except Exception as e:
            logger.error(f"Failed to search mappings: {e}")
            return []