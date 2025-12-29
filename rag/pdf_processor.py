"""
PDF Processing for Sepsis Guidelines

Extracts text from PDF files and chunks them intelligently
to preserve medical context for RAG retrieval.
"""

import pdfplumber
from pathlib import Path
from typing import List, Dict
import re


class SepsisGuidelineProcessor:
    """Process sepsis guideline PDFs into searchable chunks"""

    def __init__(self, chunk_size: int = 1000, overlap: int = 200):
        """
        Initialize the PDF processor

        Args:
            chunk_size: Target characters per chunk
            overlap: Character overlap between chunks
        """
        self.chunk_size = chunk_size
        self.overlap = overlap

    def process_pdf(self, pdf_path: str) -> List[Dict[str, any]]:
        """
        Process a single PDF file

        Args:
            pdf_path: Path to PDF file

        Returns:
            List of chunks with metadata
        """
        pdf_file = Path(pdf_path)
        if not pdf_file.exists():
            raise FileNotFoundError(f"PDF not found: {pdf_path}")

        # Determine patient category from filename
        patient_category = determine_patient_category(pdf_file.name)

        chunks = []

        with pdfplumber.open(pdf_path) as pdf:
            for page_num, page in enumerate(pdf.pages, start=1):
                # Extract text from page
                text = page.extract_text()
                if not text:
                    continue

                # Clean up text
                text = self._clean_text(text)

                # Create chunks from this page
                page_chunks = self._create_chunks(
                    text=text,
                    source_file=pdf_file.name,
                    page_number=page_num,
                    patient_category=patient_category
                )

                chunks.extend(page_chunks)

        return chunks

    def _clean_text(self, text: str) -> str:
        """Clean extracted text"""
        # Remove excessive whitespace
        text = re.sub(r'\s+', ' ', text)

        # Remove page headers/footers (common patterns)
        text = re.sub(r'Page \d+ of \d+', '', text, flags=re.IGNORECASE)

        # Fix common OCR issues
        text = text.replace('ﬁ', 'fi').replace('ﬂ', 'fl')

        return text.strip()

    def _create_chunks(
        self,
        text: str,
        source_file: str,
        page_number: int,
        patient_category: str
    ) -> List[Dict[str, any]]:
        """
        Split text into overlapping chunks

        Args:
            text: Text to chunk
            source_file: Source PDF filename
            page_number: Page number
            patient_category: Patient category (adult/pediatric/general)

        Returns:
            List of chunk dictionaries
        """
        chunks = []

        # Try to split on sentence boundaries
        sentences = re.split(r'(?<=[.!?])\s+', text)

        current_chunk = ""
        current_length = 0

        for sentence in sentences:
            sentence_length = len(sentence)

            # If adding this sentence exceeds chunk_size, save current chunk
            if current_length + sentence_length > self.chunk_size and current_chunk:
                chunks.append({
                    "text": current_chunk.strip(),
                    "source_file": source_file,
                    "page_number": page_number,
                    "chunk_index": len(chunks),
                    "patient_category": patient_category
                })

                # Start new chunk with overlap
                # Keep last few sentences for context
                overlap_text = self._get_overlap(current_chunk)
                current_chunk = overlap_text + " " + sentence
                current_length = len(current_chunk)
            else:
                current_chunk += " " + sentence
                current_length += sentence_length

        # Add final chunk
        if current_chunk.strip():
            chunks.append({
                "text": current_chunk.strip(),
                "source_file": source_file,
                "page_number": page_number,
                "chunk_index": len(chunks),
                "patient_category": patient_category
            })

        return chunks

    def _get_overlap(self, text: str) -> str:
        """Get overlap text from end of chunk"""
        if len(text) <= self.overlap:
            return text

        # Try to split at sentence boundary
        overlap_start = len(text) - self.overlap
        sentences = re.split(r'(?<=[.!?])\s+', text[overlap_start:])

        if len(sentences) > 1:
            # Return complete sentences
            return ' '.join(sentences[1:])
        else:
            # Fallback to character-based overlap
            return text[-self.overlap:]

    def process_directory(self, directory_path: str) -> List[Dict[str, any]]:
        """
        Process all PDFs in a directory

        Args:
            directory_path: Path to directory containing PDFs

        Returns:
            List of all chunks from all PDFs
        """
        directory = Path(directory_path)
        if not directory.exists():
            raise FileNotFoundError(f"Directory not found: {directory_path}")

        all_chunks = []
        pdf_files = list(directory.glob("*.pdf"))

        print(f"Found {len(pdf_files)} PDF files")

        for pdf_file in pdf_files:
            print(f"Processing: {pdf_file.name}")
            chunks = self.process_pdf(str(pdf_file))
            all_chunks.extend(chunks)
            print(f"  → {len(chunks)} chunks extracted")

        return all_chunks


def extract_section_heading(text: str) -> str:
    """
    Extract section heading from chunk text

    Tries to identify medical section headings like:
    - "Hour-1 Bundle"
    - "Initial Resuscitation"
    - "Antimicrobial Therapy"
    """
    # Look for common heading patterns
    patterns = [
        r'^([A-Z][A-Za-z\s\-]{3,40})(?:\n|:)',  # Title case heading
        r'^\d+\.\s*([A-Z][A-Za-z\s\-]{3,40})',  # Numbered heading
    ]

    for pattern in patterns:
        match = re.search(pattern, text[:200])  # Check first 200 chars
        if match:
            return match.group(1).strip()

    return "General Guidelines"


def determine_patient_category(filename: str) -> str:
    """
    Determine patient category from PDF filename

    Args:
        filename: PDF filename

    Returns:
        "adult", "pediatric", or "general"
    """
    filename_lower = filename.lower()

    if "adult" in filename_lower:
        return "adult"
    elif "child" in filename_lower or "pediatric" in filename_lower:
        return "pediatric"
    elif "ssc" in filename_lower or "surviving" in filename_lower:
        return "general"
    else:
        return "general"  # Default to general if unclear
