"""
Sepsis Guidelines Ingestion Orchestrator

One-time script to:
1. Process PDFs from rag/ directory
2. Generate embeddings using Gemini
3. Insert into MongoDB Atlas
4. Verify vector search index
"""

import sys
import json
from pathlib import Path
from typing import List, Dict

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).parent.parent))

from rag.pdf_processor import SepsisGuidelineProcessor
from rag.embedding_service import GeminiEmbeddingService
from rag.mongodb_store import MongoDBVectorStore, create_vector_index_command


def load_rag_config(config_path: str = "_data/rag_config.json") -> Dict:
    """Load RAG configuration from JSON file"""
    possible_paths = [
        Path(config_path),
        Path("..") / config_path,
        Path(__file__).parent.parent / config_path,
    ]

    for path in possible_paths:
        if path.exists():
            with open(path) as f:
                return json.load(f)

    raise FileNotFoundError(
        f"RAG config not found. Create {config_path} with MongoDB and Gemini credentials."
    )


def main():
    """Main ingestion pipeline"""
    print("\n" + "="*60)
    print("Sepsis Guidelines Ingestion Pipeline")
    print("="*60 + "\n")

    # Load configuration
    print("[1/6] Loading configuration...")
    try:
        config = load_rag_config()
        print(f"✓ Configuration loaded")
        print(f"  - MongoDB: {config.get('mongodb_database', 'sepsis_guidelines')}")
        print(f"  - Collection: {config.get('mongodb_collection', 'guideline_chunks')}")
    except FileNotFoundError as e:
        print(f"\n❌ Error: {e}")
        print("\nCreate _data/rag_config.json with:")
        print(json.dumps({
            "mongodb_connection_string": "cluster0.xxxxx.mongodb.net",
            "mongodb_username": "your_username",
            "mongodb_password": "your_password",
            "mongodb_database": "sepsis_guidelines",
            "mongodb_collection": "guideline_chunks",
            "gemini_api_key": "your_gemini_api_key"
        }, indent=2))
        return

    # Step 1: Process PDFs
    print("\n[2/6] Processing PDFs...")
    pdf_directory = Path(__file__).parent  # Aorta/rag/ directory
    processor = SepsisGuidelineProcessor(chunk_size=1000, overlap=200)

    try:
        # Find all PDF files
        pdf_files = list(pdf_directory.glob("*.pdf"))
        if not pdf_files:
            print(f"❌ No PDF files found in {pdf_directory}")
            return

        # Process ALL PDFs
        print(f"Found {len(pdf_files)} PDF files")
        all_chunks = []
        for pdf_file in pdf_files:
            print(f"Processing: {pdf_file.name}")
            chunks = processor.process_pdf(str(pdf_file))
            all_chunks.extend(chunks)
            print(f"  → {len(chunks)} chunks extracted")

        chunks = all_chunks
        print(f"✓ Extracted {len(chunks)} total chunks from {len(pdf_files)} PDFs")
    except Exception as e:
        print(f"❌ Error processing PDFs: {e}")
        return

    if not chunks:
        print("❌ No chunks extracted. Make sure PDF files are in the rag/ directory.")
        return

    # Step 2: Generate embeddings
    print("\n[3/6] Generating embeddings with Gemini...")
    embedding_service = GeminiEmbeddingService(
        api_key=config["gemini_api_key"]
    )

    try:
        texts = [chunk["text"] for chunk in chunks]
        embeddings = embedding_service.embed_batch_sync(texts, batch_size=100)
        print(f"✓ Generated {len(embeddings)} embeddings")
    except Exception as e:
        print(f"❌ Error generating embeddings: {e}")
        return

    # Step 3: Prepare documents for MongoDB
    print("\n[4/6] Preparing documents...")
    documents = []
    for chunk, embedding in zip(chunks, embeddings):
        documents.append({
            "text": chunk["text"],
            "embedding": embedding,
            "source_file": chunk["source_file"],
            "page_number": chunk["page_number"],
            "chunk_index": chunk["chunk_index"],
            "patient_category": chunk["patient_category"]
        })
    print(f"✓ Prepared {len(documents)} documents")

    # Step 4: Connect to MongoDB Atlas
    print("\n[5/6] Connecting to MongoDB Atlas...")
    try:
        vector_store = MongoDBVectorStore(
            connection_string=config["mongodb_connection_string"],
            username=config["mongodb_username"],
            password=config["mongodb_password"],
            database=config.get("mongodb_database", "sepsis_guidelines"),
            collection=config.get("mongodb_collection", "guideline_chunks")
        )
    except Exception as e:
        print(f"❌ Error connecting to MongoDB: {e}")
        return

    # Step 5: Insert documents
    print("\n[6/6] Inserting documents into MongoDB...")
    try:
        # Optional: Clear existing documents
        existing_count = vector_store.count_documents()
        if existing_count > 0:
            response = input(f"  ⚠️  {existing_count} documents exist. Clear them? (y/N): ")
            if response.lower() == 'y':
                vector_store.clear_collection()

        # Insert new documents
        doc_ids = vector_store.insert_documents(documents)
        print(f"✓ Successfully inserted {len(doc_ids)} documents")

        # Verify
        total_docs = vector_store.count_documents()
        print(f"✓ Total documents in collection: {total_docs}")

    except Exception as e:
        print(f"❌ Error inserting documents: {e}")
        return
    finally:
        vector_store.close()

    # Step 6: Display vector index creation instructions
    print("\n" + "="*60)
    print("Ingestion Complete!")
    print("="*60)
    print(f"\n✓ {len(documents)} guideline chunks indexed in MongoDB Atlas")
    print("\nNext Steps:")
    print("1. Create a vector search index in MongoDB Atlas UI:")
    print("   - Go to your cluster → Atlas Search → Create Search Index")
    print("   - Choose 'JSON Editor' and paste the definition below")
    print("   - Index name MUST be: 'guideline_vector_index'\n")

    create_vector_index_command(
        database=config.get("mongodb_database", "sepsis_guidelines"),
        collection=config.get("mongodb_collection", "guideline_chunks")
    )

    print("2. Run the backend server:")
    print("   cd Aorta && uvicorn backend.api.main:app --reload\n")


if __name__ == "__main__":
    main()
