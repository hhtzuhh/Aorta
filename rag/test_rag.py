"""
Test RAG Module - Simulate sepsis alert and generate recommendation
"""

import sys
import json
import asyncio
from pathlib import Path

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent))

from rag.mongodb_store import MongoDBVectorStore
from rag.embedding_service import GeminiEmbeddingService
from google import genai
from google.genai import types


async def test_rag_pipeline():
    """Test the complete RAG pipeline"""

    print("\n" + "="*60)
    print("RAG Pipeline Test - Sepsis Recommendation Generation")
    print("="*60)

    # Load config
    print("\n[1/5] Loading configuration...")
    config_path = Path(__file__).parent.parent / "_data" / "rag_config.json"
    with open(config_path) as f:
        config = json.load(f)
    print("✓ Config loaded")

    # Initialize services
    print("\n[2/5] Initializing services...")

    # MongoDB
    vector_store = MongoDBVectorStore(
        connection_string=config["mongodb_connection_string"],
        username=config["mongodb_username"],
        password=config["mongodb_password"],
        database=config["mongodb_database"],
        collection=config["mongodb_collection"]
    )

    # Gemini
    embedding_service = GeminiEmbeddingService(api_key=config["gemini_api_key"])
    gemini_client = genai.Client(api_key=config["gemini_api_key"])

    print("✓ Services initialized")

    # Simulate sepsis alert
    print("\n[3/5] Simulating sepsis alert...")

    # Choose patient type for testing
    print("\nSelect patient type:")
    print("1. Adult (tests adult + general guidelines)")
    print("2. Pediatric (tests pediatric + general guidelines)")
    print("3. No filter (tests all guidelines)")

    choice = input("Enter choice (1-3): ").strip()

    if choice == "1":
        patient_category = "adult"
        patient_age = 45
        print(f"✓ Testing with ADULT patient (age {patient_age})")
    elif choice == "2":
        patient_category = "pediatric"
        patient_age = 8
        print(f"✓ Testing with PEDIATRIC patient (age {patient_age})")
    else:
        patient_category = None
        patient_age = 45
        print("✓ Testing with NO FILTER (all guidelines)")

    # Create test query
    sepsis_probability = 0.75
    risk_level = "HIGH"
    sofa_score = 6.0

    query_text = f"""
    Sepsis patient with {risk_level} risk (probability: {sepsis_probability:.1%}, SOFA: {sofa_score:.1f}).
    Patient age: {patient_age} years.
    What are the immediate treatment actions, monitoring protocols, and evidence-based interventions
    according to Surviving Sepsis Campaign guidelines?
    """

    print(f"\nQuery: {query_text.strip()}")

    # Generate query embedding
    print("\n[4/5] Searching vector database...")
    query_embedding = await embedding_service.embed_text(query_text)
    print(f"✓ Generated query embedding ({len(query_embedding)} dimensions)")

    # Search MongoDB
    results = vector_store.vector_search(
        query_embedding=query_embedding,
        top_k=5,
        score_threshold=0.7,
        patient_category=patient_category
    )

    print(f"✓ Found {len(results)} relevant guideline chunks")
    print("\nTop results:")
    for i, result in enumerate(results, 1):
        print(f"  {i}. {result['source_file']} (page {result['page_number']}) - "
              f"Category: {result['patient_category']} - "
              f"Score: {result['relevance_score']:.3f}")

    if not results:
        print("\n❌ No relevant guidelines found. Check your vector index and data.")
        return

    # Generate recommendation with Gemini
    print("\n[5/5] Generating clinical recommendation with Gemini...")

    # Build context from retrieved documents
    context = "\n\n".join([
        f"[Source: {doc['source_file']}, Page {doc['page_number']}, "
        f"Category: {doc['patient_category']}, Relevance: {doc['relevance_score']:.2f}]\n{doc['text']}"
        for doc in results
    ])

    # Build prompt
    prompt = f"""You are a clinical decision support system for sepsis management.

PATIENT CONTEXT:
{query_text}

RELEVANT GUIDELINES:
{context}

Generate evidence-based clinical recommendations in the following JSON format:
{{
  "immediate_actions": [
    {{
      "action": "Specific action to take",
      "priority": "IMMEDIATE|URGENT|ROUTINE",
      "timing": "Timeframe (e.g., 'Within 1 hour')",
      "rationale": "Clinical reasoning"
    }}
  ],
  "monitoring": {{
    "vital_signs": ["List of vital signs to monitor"],
    "laboratory_tests": ["List of labs to order"],
    "frequency": "Monitoring frequency"
  }},
  "clinical_rationale": "Brief explanation of the overall approach"
}}

Focus on:
1. Hour-1 Bundle for sepsis (blood cultures, antibiotics, lactate, fluids)
2. Monitoring protocols appropriate for risk level
3. Evidence-based interventions from the guidelines
4. Practical, actionable steps for clinicians

Return ONLY valid JSON, no additional text."""

    # Call Gemini
    response = await gemini_client.aio.models.generate_content(
        model="gemini-2.0-flash-001",
        contents=prompt
    )

    # Parse response
    response_text = response.text.strip()
    if response_text.startswith("```json"):
        response_text = response_text.split("```json")[1].split("```")[0].strip()
    elif response_text.startswith("```"):
        response_text = response_text.split("```")[1].split("```")[0].strip()

    recommendation = json.loads(response_text)

    # Display results
    print("\n" + "="*60)
    print("CLINICAL RECOMMENDATION")
    print("="*60)

    print(f"\nPatient: Age {patient_age}, {risk_level} sepsis risk ({sepsis_probability:.1%})")
    print(f"SOFA Score: {sofa_score}")
    if patient_category:
        print(f"Patient Category: {patient_category.upper()}")

    print("\n--- IMMEDIATE ACTIONS ---")
    for i, action in enumerate(recommendation.get("immediate_actions", []), 1):
        print(f"\n{i}. {action['action']}")
        print(f"   Priority: {action['priority']}")
        print(f"   Timing: {action['timing']}")
        print(f"   Rationale: {action['rationale']}")

    print("\n--- MONITORING PROTOCOL ---")
    monitoring = recommendation.get("monitoring", {})
    print(f"Vital Signs: {', '.join(monitoring.get('vital_signs', []))}")
    print(f"Labs: {', '.join(monitoring.get('laboratory_tests', []))}")
    print(f"Frequency: {monitoring.get('frequency', 'N/A')}")

    print("\n--- CLINICAL RATIONALE ---")
    print(recommendation.get("clinical_rationale", "N/A"))

    print("\n--- EVIDENCE SOURCES ---")
    for i, result in enumerate(results, 1):
        print(f"{i}. {result['source_file']} (page {result['page_number']}) - "
              f"{result['patient_category']} - Score: {result['relevance_score']:.3f}")

    print("\n" + "="*60)
    print("✓ RAG Pipeline Test Complete!")
    print("="*60 + "\n")

    # Cleanup
    vector_store.close()


if __name__ == "__main__":
    asyncio.run(test_rag_pipeline())
