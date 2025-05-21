"""Security utilities for the async data pipeline."""

import base64
import json
import os
from typing import Any, Dict, Optional

from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC

from asyncdatapipeline.monitoring import PipelineMonitor


class DataEncryptor:
    """Utility class for payload encryption and decryption."""

    def __init__(self, monitor: PipelineMonitor, encryption_key: Optional[str] = None):
        """
        Initialize the data encryptor.

        Args:
            monitor: Monitor for logging.
            encryption_key: Key for encryption. If None, generates a new one.
        """
        self.monitor = monitor
        self._key = encryption_key or self._generate_key()
        self._fernet = self._initialize_fernet()

    def _generate_key(self) -> str:
        """Generate a new encryption key."""
        self.monitor.log_event("Generating new encryption key")
        salt = os.urandom(16)
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=salt,
            iterations=100000,
        )
        key = base64.urlsafe_b64encode(kdf.derive(b"secure_pipeline"))
        return key.decode('utf-8')

    def _initialize_fernet(self) -> Fernet:
        """Initialize Fernet instance with the encryption key."""
        try:
            key_bytes = self._key.encode() if isinstance(self._key, str) else self._key
            return Fernet(key_bytes)
        except Exception as e:
            self.monitor.log_error(f"Error initializing encryption: {e}")
            raise

    def encrypt(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Encrypt data payload.

        Args:
            data: Data to encrypt.

        Returns:
            Dict with encrypted payload.
        """
        try:
            # Serialize data to JSON
            json_data = json.dumps(data).encode('utf-8')

            # Encrypt
            encrypted_data = self._fernet.encrypt(json_data)

            # Return with metadata
            return {
                "encrypted": True,
                "payload": encrypted_data.decode('utf-8'),
                "version": "1.0"
            }
        except Exception as e:
            self.monitor.log_error(f"Encryption error: {e}")
            return data  # Return original data on error

    def decrypt(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Decrypt encrypted payload.

        Args:
            data: Data to decrypt.

        Returns:
            Decrypted data.
        """
        try:
            # Check if data is encrypted
            if not isinstance(data, dict) or not data.get("encrypted"):
                return data

            # Get encrypted payload
            encrypted_payload = data.get("payload")
            if not encrypted_payload:
                self.monitor.log_error("No encrypted payload found")
                return data

            # Decrypt
            decrypted_data = self._fernet.decrypt(encrypted_payload.encode('utf-8'))

            # Parse JSON
            return json.loads(decrypted_data.decode('utf-8'))
        except Exception as e:
            self.monitor.log_error(f"Decryption error: {e}")
            return data  # Return original data on error
