"""
Módulo comum para pipeline bancário com Data Vault 2.0.
Exporta principais classes e funções para uso nos jobs.
"""

import os
import sys

COMMON_DIR = os.path.dirname(__file__)
if COMMON_DIR not in sys.path:
    sys.path.insert(0, COMMON_DIR)

from config import Config, PipelineConfig
from runtime_profiles import get_runtime_profile, list_runtime_profiles
from spark_session import SparkSessionFactory, create_spark_session, SparkUtils
from hashing import HashingUtils, BusinessKeyHasher
from masking import MaskingUtils, MaskingPolicy
from delta_io import DeltaIO, DeltaVaultLoader
from monitoring import MonitoringLogger, DataQualityLogger, ExecutionMetrics, setup_logging
from validations import (
    DataQualityValidations,
    DataVaultValidations,
    run_quality_checks
)

__all__ = [
    "Config",
    "PipelineConfig",
    "get_runtime_profile",
    "list_runtime_profiles",
    "SparkSessionFactory",
    "create_spark_session",
    "SparkUtils",
    "HashingUtils",
    "BusinessKeyHasher",
    "MaskingUtils",
    "MaskingPolicy",
    "DeltaIO",
    "DeltaVaultLoader",
    "MonitoringLogger",
    "DataQualityLogger",
    "ExecutionMetrics",
    "setup_logging",
    "DataQualityValidations",
    "DataVaultValidations",
    "run_quality_checks",
]

__version__ = "0.1.0"
