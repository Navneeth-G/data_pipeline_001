from typing import Dict, List, Tuple
import hashlib

def generate_record_hash_sha256(values: List[str]) -> str:
    """Generate SHA256 hash from list of values."""
    hash_input = "|".join(str(value) for value in values)
    return hashlib.sha256(hash_input.encode()).hexdigest()

