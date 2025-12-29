"""
MongoDB Atlas Vector Store

Manages vector embeddings in MongoDB Atlas with vector search capability.
"""

from typing import List, Dict, Optional
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
import urllib.parse


class MongoDBVectorStore:
    """MongoDB Atlas vector store for RAG retrieval"""

    def __init__(
        self,
        connection_string: str,
        username: str,
        password: str,
        database: str = "sepsis_guidelines",
        collection: str = "guideline_chunks"
    ):
        """
        Initialize MongoDB connection

        Args:
            connection_string: MongoDB Atlas connection string
            username: MongoDB username
            password: MongoDB password
            database: Database name
            collection: Collection name
        """
        # URL encode credentials
        username_encoded = urllib.parse.quote_plus(username)
        password_encoded = urllib.parse.quote_plus(password)

        # Build full connection string
        if "mongodb+srv://" in connection_string:
            # Extract the cluster part
            cluster_part = connection_string.replace("mongodb+srv://", "")
            full_uri = f"mongodb+srv://{username_encoded}:{password_encoded}@{cluster_part}"
        else:
            # Standard connection string
            full_uri = connection_string.replace(
                "mongodb://",
                f"mongodb://{username_encoded}:{password_encoded}@"
            )

        self.client = MongoClient(full_uri)
        self.database = self.client[database]
        self.collection = self.database[collection]

        # Test connection
        try:
            self.client.admin.command('ping')
            print(f"✓ Connected to MongoDB Atlas: {database}.{collection}")
        except ConnectionFailure as e:
            raise ConnectionFailure(f"Failed to connect to MongoDB Atlas: {e}")

    def insert_documents(self, documents: List[Dict]) -> List[str]:
        """
        Insert documents with embeddings

        Args:
            documents: List of documents with 'text', 'embedding', and metadata

        Returns:
            List of inserted document IDs
        """
        if not documents:
            return []

        result = self.collection.insert_many(documents)
        print(f"✓ Inserted {len(result.inserted_ids)} documents")
        return [str(id) for id in result.inserted_ids]

    def vector_search(
        self,
        query_embedding: List[float],
        top_k: int = 5,
        score_threshold: float = 0.7,
        patient_category: Optional[str] = None
    ) -> List[Dict]:
        """
        Perform vector similarity search with optional filtering

        Args:
            query_embedding: Query vector (768 dimensions)
            top_k: Number of results to return
            score_threshold: Minimum similarity score (0-1)
            patient_category: Filter by patient category (adult/pediatric/general/None)

        Returns:
            List of matching documents with scores
        """
        # Build vector search stage
        vector_search_stage = {
            "$vectorSearch": {
                "index": "vector_index",  # Must match index name in Atlas
                "path": "embedding",
                "queryVector": query_embedding,
                "numCandidates": top_k * 10,  # Oversample for better results
                "limit": top_k
            }
        }

        # Add filter if patient_category specified
        if patient_category:
            vector_search_stage["$vectorSearch"]["filter"] = {
                "$or": [
                    {"patient_category": patient_category},
                    {"patient_category": "general"}  # Always include general guidelines
                ]
            }

        # MongoDB Atlas vector search aggregation pipeline
        pipeline = [
            vector_search_stage,
            {
                "$project": {
                    "_id": 1,
                    "text": 1,
                    "source_file": 1,
                    "page_number": 1,
                    "chunk_index": 1,
                    "patient_category": 1,
                    "score": {"$meta": "vectorSearchScore"}
                }
            }
        ]

        results = list(self.collection.aggregate(pipeline))

        # Filter by score threshold
        filtered_results = [
            {
                "text": doc["text"],
                "source_file": doc.get("source_file", "unknown"),
                "page_number": doc.get("page_number", 0),
                "chunk_index": doc.get("chunk_index", 0),
                "patient_category": doc.get("patient_category", "unknown"),
                "relevance_score": doc["score"]
            }
            for doc in results
            if doc["score"] >= score_threshold
        ]

        return filtered_results

    def count_documents(self) -> int:
        """Get total document count"""
        return self.collection.count_documents({})

    def clear_collection(self):
        """Clear all documents from collection"""
        result = self.collection.delete_many({})
        print(f"✓ Deleted {result.deleted_count} documents")

    def get_sample_document(self) -> Optional[Dict]:
        """Get a sample document for testing"""
        return self.collection.find_one()

    def close(self):
        """Close MongoDB connection"""
        self.client.close()


def create_vector_index_command(
    database: str = "sepsis_guidelines",
    collection: str = "guideline_chunks",
    index_name: str = "guideline_vector_index",
    num_dimensions: int = 768
) -> Dict:
    """
    Generate the MongoDB Atlas vector index creation command

    This command should be run in MongoDB Atlas UI or via API.

    Args:
        database: Database name
        collection: Collection name
        index_name: Index name
        num_dimensions: Embedding dimensions (768 for text-embedding-004)

    Returns:
        Index definition as dict
    """
    index_definition = {
        "name": index_name,
        "type": "vectorSearch",
        "definition": {
            "fields": [
                {
                    "type": "vector",
                    "path": "embedding",
                    "numDimensions": num_dimensions,
                    "similarity": "cosine"
                }
            ]
        }
    }

    print("\n" + "="*60)
    print("MongoDB Atlas Vector Search Index Definition")
    print("="*60)
    print(f"Database: {database}")
    print(f"Collection: {collection}")
    print(f"\nCreate this index in MongoDB Atlas UI:")
    print("-"*60)
    import json
    print(json.dumps(index_definition, indent=2))
    print("="*60 + "\n")

    return index_definition
