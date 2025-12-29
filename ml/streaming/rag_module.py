"""
RAG Module - Pluggable clinical recommendation engine for UnifiedConsumer

Does NOT consume Kafka directly - receives sepsis alerts via callback from UnifiedConsumer.
Generates evidence-based recommendations using MongoDB Atlas + Gemini RAG.
Publishes to clinical-recommendations topic.
"""

import json
import logging
import time
import uuid
from datetime import datetime
from typing import Dict, List, Optional
from confluent_kafka import Producer
from google import genai

from Aorta.rag.embedding_service import GeminiEmbeddingService
from Aorta.rag.mongodb_store import MongoDBVectorStore
from Aorta.backend.api.models import (
    ClinicalRecommendation,
    SepsisAlertReference,
    RecommendationAction,
    MonitoringProtocol,
    EvidenceSource,
)

logger = logging.getLogger(__name__)


class RAGModule:
    """
    RAG module that plugs into UnifiedConsumer

    Architecture:
    - UnifiedConsumer calls on_sepsis_alert() for each sepsis alert
    - Filters by probability threshold (default: 0.5 = HIGH/CRITICAL only)
    - Queries MongoDB Atlas vector search for relevant guidelines
    - Generates recommendations using Gemini
    - Publishes to clinical-recommendations topic via its own Producer
    """

    def __init__(
        self,
        kafka_config: dict,
        mongodb_uri: str,
        mongodb_username: str,
        mongodb_password: str,
        gemini_api_key: str,
        mongodb_database: str = "sepsis_guidelines",
        mongodb_collection: str = "guideline_chunks",
        probability_threshold: float = 0.5,
    ):
        """
        Initialize RAG module

        Args:
            kafka_config: Kafka configuration dict
            mongodb_uri: MongoDB Atlas connection string
            mongodb_username: MongoDB username
            mongodb_password: MongoDB password
            gemini_api_key: Google Gemini API key
            mongodb_database: MongoDB database name
            mongodb_collection: MongoDB collection name
            probability_threshold: Minimum sepsis probability to generate recommendations
        """
        logger.info("Initializing RAGModule...")

        self.probability_threshold = probability_threshold

        # Initialize MongoDB Atlas vector store
        try:
            self.vector_store = MongoDBVectorStore(
                connection_string=mongodb_uri,
                username=mongodb_username,
                password=mongodb_password,
                database=mongodb_database,
                collection=mongodb_collection
            )
            doc_count = self.vector_store.count_documents()
            logger.info(f"Connected to MongoDB Atlas ({doc_count} guideline chunks)")
        except Exception as e:
            logger.error(f"Failed to connect to MongoDB Atlas: {e}")
            raise

        # Initialize Gemini embedding service
        try:
            self.embedding_service = GeminiEmbeddingService(api_key=gemini_api_key)
            logger.info("Initialized Gemini embedding service")
        except Exception as e:
            logger.error(f"Failed to initialize Gemini embeddings: {e}")
            raise

        # Initialize Gemini client for generation
        try:
            self.gemini_client = genai.Client(api_key=gemini_api_key)
            self.generation_model = "gemini-2.0-flash-001"
            logger.info(f"Initialized Gemini generation model: {self.generation_model}")
        except Exception as e:
            logger.error(f"Failed to initialize Gemini client: {e}")
            raise

        # Kafka producer for clinical-recommendations
        self.producer = Producer(kafka_config)
        self.recommendation_topic = "clinical-recommendations"
        logger.info(f"RAGModule ready (threshold: {probability_threshold})")

    async def on_sepsis_alert(self, event_data: dict):
        """
        Callback from UnifiedConsumer when sepsis alert is received

        Args:
            event_data: Sepsis alert event dictionary
        """
        try:
            # Extract alert details
            patient = event_data.get("patient", {})
            admission = event_data.get("admission", {})
            prediction = event_data.get("prediction", {})

            subject_id = patient.get("subject_id", "unknown")
            patient_age = patient.get("age")  # Get patient age
            hadm_id = admission.get("hadm_id")
            probability = prediction.get("sepsis_probability", 0.0)
            risk_level = prediction.get("risk_level", "UNKNOWN")
            event_time = event_data.get("event_time", datetime.utcnow().isoformat())

            # Filter by threshold
            if probability < self.probability_threshold:
                logger.debug(f"Skipping alert for {subject_id} (probability {probability:.2f} < {self.probability_threshold})")
                return

            # Determine patient category from age
            patient_category = self._determine_patient_category(patient_age)
            logger.info(f"Generating recommendation for {subject_id} (age: {patient_age}, category: {patient_category}, probability: {probability:.2f}, risk: {risk_level})")

            # Generate recommendation
            start_time = time.time()
            recommendation = await self._generate_recommendation(
                subject_id=subject_id,
                hadm_id=hadm_id,
                probability=probability,
                risk_level=risk_level,
                alert_time=event_time,
                sofa_score=prediction.get("sofa_score", 0.0),
                patient_age=patient_age,
                patient_category=patient_category
            )
            generation_time_ms = int((time.time() - start_time) * 1000)

            # Publish to Kafka
            self._publish_recommendation(recommendation, generation_time_ms)

        except Exception as e:
            logger.error(f"Error processing sepsis alert: {e}", exc_info=True)

    def _determine_patient_category(self, age: Optional[int]) -> Optional[str]:
        """
        Determine patient category from age

        Args:
            age: Patient age in years

        Returns:
            "adult", "pediatric", or None if age unknown
        """
        if age is None:
            logger.warning("Patient age unknown, will search all categories")
            return None

        # Standard cutoff: pediatric = under 18 years
        if age < 18:
            return "pediatric"
        else:
            return "adult"

    async def _generate_recommendation(
        self,
        subject_id: str,
        hadm_id: Optional[str],
        probability: float,
        risk_level: str,
        alert_time: str,
        sofa_score: float,
        patient_age: Optional[int],
        patient_category: Optional[str]
    ) -> ClinicalRecommendation:
        """
        Generate clinical recommendation using RAG

        Args:
            subject_id: Patient ID
            hadm_id: Admission ID
            probability: Sepsis probability
            risk_level: Risk level (LOW/MEDIUM/HIGH/CRITICAL)
            alert_time: Alert timestamp
            sofa_score: SOFA score

        Returns:
            ClinicalRecommendation model
        """
        # Build query context
        query_text = self._build_query(probability, risk_level, sofa_score)

        # Get query embedding
        query_embedding = await self.embedding_service.embed_text(query_text)

        # Search MongoDB Atlas vector database with patient category filter
        search_results = self.vector_store.vector_search(
            query_embedding=query_embedding,
            top_k=5,
            score_threshold=0.7,
            patient_category=patient_category  # Filter by adult/pediatric
        )

        if not search_results:
            logger.warning(f"No relevant guidelines found for {subject_id}")
            # Return minimal recommendation
            return self._create_fallback_recommendation(
                subject_id, hadm_id, probability, risk_level, alert_time
            )

        # Build Gemini prompt with retrieved context
        prompt = self._build_gemini_prompt(
            query_text=query_text,
            retrieved_docs=search_results,
            risk_level=risk_level,
            sofa_score=sofa_score
        )

        # Generate recommendation using Gemini
        response = await self.gemini_client.aio.models.generate_content(
            model=self.generation_model,
            contents=prompt
        )

        # Parse structured response
        recommendation = self._parse_gemini_response(
            response=response,
            subject_id=subject_id,
            hadm_id=hadm_id,
            probability=probability,
            risk_level=risk_level,
            alert_time=alert_time,
            evidence_sources=search_results
        )

        return recommendation

    def _build_query(self, probability: float, risk_level: str, sofa_score: float) -> str:
        """Build search query from alert context"""
        return f"""
        Sepsis patient with {risk_level} risk (probability: {probability:.1%}, SOFA: {sofa_score:.1f}).
        What are the immediate treatment actions, monitoring protocols, and evidence-based interventions
        according to Surviving Sepsis Campaign guidelines?
        """

    def _build_gemini_prompt(
        self,
        query_text: str,
        retrieved_docs: List[Dict],
        risk_level: str,
        sofa_score: float
    ) -> str:
        """Build prompt for Gemini with retrieved context"""
        context = "\n\n".join([
            f"[Source: {doc['source_file']}, Page {doc['page_number']}, Relevance: {doc['relevance_score']:.2f}]\n{doc['text']}"
            for doc in retrieved_docs
        ])

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

        return prompt

    def _parse_gemini_response(
        self,
        response,
        subject_id: str,
        hadm_id: Optional[str],
        probability: float,
        risk_level: str,
        alert_time: str,
        evidence_sources: List[Dict]
    ) -> ClinicalRecommendation:
        """Parse Gemini response into ClinicalRecommendation model"""
        try:
            # Extract JSON from response
            response_text = response.text.strip()
            # Remove markdown code blocks if present
            if response_text.startswith("```json"):
                response_text = response_text.split("```json")[1].split("```")[0].strip()
            elif response_text.startswith("```"):
                response_text = response_text.split("```")[1].split("```")[0].strip()

            data = json.loads(response_text)

            # Build recommendation
            return ClinicalRecommendation(
                event_time=datetime.utcnow().isoformat(),
                recommendation_id=f"rec_{uuid.uuid4().hex[:8]}",
                sepsis_alert=SepsisAlertReference(
                    subject_id=subject_id,
                    hadm_id=hadm_id,
                    sepsis_probability=probability,
                    risk_level=risk_level,
                    alert_time=alert_time
                ),
                immediate_actions=[
                    RecommendationAction(**action)
                    for action in data.get("immediate_actions", [])
                ],
                monitoring=MonitoringProtocol(**data.get("monitoring", {})),
                clinical_rationale=data.get("clinical_rationale", ""),
                evidence_sources=[
                    EvidenceSource(
                        source_file=src["source_file"],
                        page_number=src["page_number"],
                        section="Sepsis Guidelines",
                        relevance_score=src["relevance_score"]
                    )
                    for src in evidence_sources
                ],
                model_version=self.generation_model,
                generation_time_ms=0  # Will be set by caller
            )

        except Exception as e:
            logger.error(f"Failed to parse Gemini response: {e}")
            return self._create_fallback_recommendation(
                subject_id, hadm_id, probability, risk_level, alert_time
            )

    def _create_fallback_recommendation(
        self,
        subject_id: str,
        hadm_id: Optional[str],
        probability: float,
        risk_level: str,
        alert_time: str
    ) -> ClinicalRecommendation:
        """Create minimal fallback recommendation if RAG fails"""
        return ClinicalRecommendation(
            event_time=datetime.utcnow().isoformat(),
            recommendation_id=f"rec_{uuid.uuid4().hex[:8]}",
            sepsis_alert=SepsisAlertReference(
                subject_id=subject_id,
                hadm_id=hadm_id,
                sepsis_probability=probability,
                risk_level=risk_level,
                alert_time=alert_time
            ),
            immediate_actions=[
                RecommendationAction(
                    action="Obtain blood cultures before antibiotics",
                    priority="IMMEDIATE",
                    timing="Within 1 hour",
                    rationale="Standard Hour-1 Bundle"
                ),
                RecommendationAction(
                    action="Administer broad-spectrum antibiotics",
                    priority="IMMEDIATE",
                    timing="Within 1 hour",
                    rationale="Early antibiotics reduce mortality"
                )
            ],
            monitoring=MonitoringProtocol(
                vital_signs=["Blood pressure", "Heart rate", "SpO2"],
                laboratory_tests=["Lactate", "Procalcitonin"],
                frequency="Every 2 hours"
            ),
            clinical_rationale="Standard sepsis management protocol (RAG system unavailable)",
            evidence_sources=[],
            model_version="fallback",
            generation_time_ms=0
        )

    def _publish_recommendation(self, recommendation: ClinicalRecommendation, generation_time_ms: int):
        """Publish recommendation to Kafka"""
        try:
            # Update generation time
            recommendation.generation_time_ms = generation_time_ms

            # Serialize to JSON
            message = recommendation.model_dump_json()

            # Publish to Kafka
            self.producer.produce(
                topic=self.recommendation_topic,
                value=message.encode("utf-8"),
                key=recommendation.sepsis_alert.subject_id.encode("utf-8"),
                callback=self._delivery_callback
            )

            # Flush producer
            self.producer.poll(0)

            logger.info(f"Published recommendation {recommendation.recommendation_id} ({generation_time_ms}ms)")

        except Exception as e:
            logger.error(f"Failed to publish recommendation: {e}")

    def _delivery_callback(self, err, msg):
        """Kafka delivery callback"""
        if err:
            logger.error(f"Recommendation delivery failed: {err}")
        else:
            logger.debug(f"Recommendation delivered to {msg.topic()} [{msg.partition()}]")

    def flush(self):
        """Flush pending messages"""
        logger.info("Flushing RAG module producer...")
        self.producer.flush(timeout=10)

    def close(self):
        """Clean up resources"""
        logger.info("Closing RAG module...")
        self.flush()
        self.vector_store.close()
