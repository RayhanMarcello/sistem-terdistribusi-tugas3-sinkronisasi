"""
ML-based Adaptive Load Balancer (Bonus C)
  - RandomForest model for node selection
  - Trains on synthetic + real metrics data
  - Online retraining every N requests
  - Anomaly detection (Isolation Forest) → trigger scale alerts
  - Saves/loads model from disk
"""
import asyncio
import json
import logging
import os
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

import joblib
import numpy as np

from src.utils.config import config
from src.utils.metrics import ML_PREDICTIONS, ML_ACCURACY, ML_MODEL_VERSION

logger = logging.getLogger(__name__)

try:
    from sklearn.ensemble import RandomForestClassifier, IsolationForest
    from sklearn.preprocessing import LabelEncoder
    from sklearn.model_selection import train_test_split
    from sklearn.metrics import accuracy_score
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False
    logger.warning("[MLLoadBalancer] scikit-learn not available — ML disabled")


@dataclass
class NodeMetrics:
    node_id: str
    cpu_usage: float       # 0.0 – 1.0
    request_rate: float    # req/s
    latency_p99: float     # ms
    queue_depth: int
    error_rate: float      # 0.0 – 1.0
    cache_hit_rate: float  # 0.0 – 1.0
    timestamp: float = field(default_factory=time.time)

    def to_features(self) -> List[float]:
        return [
            self.cpu_usage,
            self.request_rate,
            self.latency_p99,
            float(self.queue_depth),
            self.error_rate,
            self.cache_hit_rate,
        ]


class MLLoadBalancer:
    """
    Adaptive ML-based load balancer.

    Features:
    - RandomForest classifier predicts best node for each request
    - IsolationForest detects anomalous nodes
    - Online retraining triggered every N requests
    - Model persisted to disk (reload on restart)
    - Falls back to round-robin if model unavailable
    """

    FEATURE_NAMES = [
        "cpu_usage", "request_rate", "latency_p99",
        "queue_depth", "error_rate", "cache_hit_rate"
    ]

    def __init__(self, node_ids: List[str]):
        self.node_ids = node_ids
        self._model: Optional[any] = None
        self._anomaly_detector: Optional[any] = None
        self._label_encoder = LabelEncoder().fit(node_ids) if SKLEARN_AVAILABLE else None
        self._model_version = 0
        self._request_count = 0
        self._training_buffer: List[Tuple[List[float], str]] = []  # (features, chosen_node)
        self._metrics_cache: Dict[str, NodeMetrics] = {}
        self._rr_index = 0  # round-robin fallback

    # --------------------------------------------------
    # Model Lifecycle
    # --------------------------------------------------
    async def initialize(self):
        """Load existing model or train fresh one"""
        if not SKLEARN_AVAILABLE:
            return

        model_path = config.ml_model_path
        os.makedirs(os.path.dirname(model_path), exist_ok=True)

        if os.path.exists(model_path):
            try:
                saved = joblib.load(model_path)
                self._model = saved.get("model")
                self._anomaly_detector = saved.get("anomaly_detector")
                self._model_version = saved.get("version", 0)
                logger.info(f"[MLLoadBalancer] Loaded model v{self._model_version} from {model_path}")
            except Exception as e:
                logger.warning(f"[MLLoadBalancer] Failed to load model: {e}, training fresh")
                await self._train_initial_model()
        else:
            await self._train_initial_model()

        asyncio.create_task(self._online_retraining_loop())

    async def _train_initial_model(self):
        """Train model on synthetic data at startup"""
        logger.info(f"[MLLoadBalancer] Generating {config.ml_training_samples} synthetic training samples...")

        X, y = self._generate_synthetic_data(config.ml_training_samples)
        await asyncio.get_event_loop().run_in_executor(None, self._fit_model, X, y)
        logger.info(f"[MLLoadBalancer] ✅ Initial model trained (v{self._model_version})")

    def _generate_synthetic_data(self, n_samples: int) -> Tuple[np.ndarray, np.ndarray]:
        """Generate synthetic training data based on domain knowledge"""
        np.random.seed(42)
        n_nodes = len(self.node_ids)
        X, y = [], []

        for _ in range(n_samples):
            # Generate node metrics
            node_metrics = []
            for _ in range(n_nodes):
                metrics = [
                    np.random.beta(2, 5),           # cpu_usage (skewed low)
                    np.random.exponential(10),       # request_rate
                    np.random.gamma(2, 20),          # latency_p99 ms
                    int(np.random.poisson(5)),       # queue_depth
                    np.random.beta(1, 20),           # error_rate (skewed low)
                    np.random.beta(5, 2),            # cache_hit_rate (skewed high)
                ]
                node_metrics.append(metrics)

            # Best node = lowest score (cpu + latency + queue, minus cache_hit)
            scores = [
                m[0] * 0.3 + m[2] / 1000 * 0.3 + m[3] / 50 * 0.2 + m[4] * 0.2 - m[5] * 0.1
                for m in node_metrics
            ]
            best_node_idx = int(np.argmin(scores))

            # Use aggregate features of all nodes as input features
            aggregate = [
                node_metrics[best_node_idx][i]  # best node's metrics
                for i in range(len(self.FEATURE_NAMES))
            ]
            X.append(aggregate)
            y.append(self.node_ids[best_node_idx % n_nodes])

        return np.array(X), np.array(y)

    def _fit_model(self, X: np.ndarray, y: np.ndarray):
        """Fit RandomForest + IsolationForest (runs in thread pool)"""
        if not SKLEARN_AVAILABLE:
            return

        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

        # Main classifier
        clf = RandomForestClassifier(
            n_estimators=100,
            max_depth=10,
            min_samples_leaf=2,
            class_weight="balanced",
            random_state=42,
            n_jobs=-1,
        )
        clf.fit(X_train, y_train)
        accuracy = accuracy_score(y_test, clf.predict(X_test))
        logger.info(f"[MLLoadBalancer] Model accuracy: {accuracy:.3f}")

        # Anomaly detector
        iso = IsolationForest(contamination=0.05, random_state=42)
        iso.fit(X_train)

        self._model = clf
        self._anomaly_detector = iso
        self._model_version += 1

        # Persist
        try:
            joblib.dump({
                "model": self._model,
                "anomaly_detector": self._anomaly_detector,
                "version": self._model_version,
                "node_ids": self.node_ids,
            }, config.ml_model_path)
        except Exception as e:
            logger.warning(f"[MLLoadBalancer] Failed to save model: {e}")

        ML_ACCURACY.labels(node_id="ml_balancer").set(accuracy)
        ML_MODEL_VERSION.labels(node_id="ml_balancer").set(self._model_version)

    # --------------------------------------------------
    # Prediction
    # --------------------------------------------------
    async def select_node(self, request_type: str = "generic") -> str:
        """
        Predict the best node to handle the next request.
        Falls back to round-robin if model unavailable.
        """
        if not self._model or not self._metrics_cache:
            return self._round_robin()

        # Get features for each available node and pick the best real-time metrics
        # Use the node with best current metrics as prediction basis
        best_features = self._get_best_node_features()
        if best_features is None:
            return self._round_robin()

        try:
            X = np.array([best_features])
            predicted_node = self._model.predict(X)[0]
            confidence = max(self._model.predict_proba(X)[0])

            # Check for anomaly
            is_anomaly = self._anomaly_detector.predict(X)[0] == -1 if self._anomaly_detector else False
            if is_anomaly:
                logger.warning(f"[MLLoadBalancer] Anomaly detected in node metrics!")

            self._request_count += 1
            ML_PREDICTIONS.labels(node_id="ml_balancer", predicted_node=predicted_node).inc()

            # Buffer for online retraining
            self._training_buffer.append((best_features, predicted_node))
            if len(self._training_buffer) > 5000:
                self._training_buffer = self._training_buffer[-2000:]

            return predicted_node

        except Exception as e:
            logger.warning(f"[MLLoadBalancer] Prediction error: {e}")
            return self._round_robin()

    def _get_best_node_features(self) -> Optional[List[float]]:
        """Get features of the node with the best current metrics"""
        if not self._metrics_cache:
            return None

        best_node = min(
            self._metrics_cache.values(),
            key=lambda m: (m.cpu_usage * 0.4 + m.latency_p99 / 500 * 0.3
                           + m.error_rate * 0.3 - m.cache_hit_rate * 0.1)
        )
        return best_node.to_features()

    def _round_robin(self) -> str:
        """Round-robin fallback"""
        node = self.node_ids[self._rr_index % len(self.node_ids)]
        self._rr_index += 1
        return node

    # --------------------------------------------------
    # Metrics Update
    # --------------------------------------------------
    def update_metrics(self, metrics: NodeMetrics):
        """Update real-time metrics for a node"""
        self._metrics_cache[metrics.node_id] = metrics

    async def collect_metrics_from_prometheus(self, prometheus_url: str):
        """Fetch real-time metrics from Prometheus (optional)"""
        try:
            import aiohttp
            async with aiohttp.ClientSession() as session:
                queries = {
                    "cpu": f'{prometheus_url}/api/v1/query?query=process_cpu_seconds_total',
                    "latency": f'{prometheus_url}/api/v1/query?query=grpc_request_duration_seconds_p99',
                }
                for metric_name, url in queries.items():
                    async with session.get(url, timeout=aiohttp.ClientTimeout(total=2)) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            # Parse and update metrics cache
                            for result in data.get("data", {}).get("result", []):
                                node_id = result.get("metric", {}).get("node_id", "unknown")
                                value = float(result.get("value", [0, 0])[1])
                                if node_id in self._metrics_cache:
                                    if metric_name == "cpu":
                                        self._metrics_cache[node_id].cpu_usage = value
        except Exception:
            pass  # Prometheus collection is best-effort

    # --------------------------------------------------
    # Online Retraining
    # --------------------------------------------------
    async def _online_retraining_loop(self):
        """Retrain model every N requests"""
        while True:
            await asyncio.sleep(60)  # Check every minute
            if self._request_count >= config.ml_retrain_interval and len(self._training_buffer) >= 50:
                logger.info(f"[MLLoadBalancer] Triggering online retraining ({len(self._training_buffer)} samples)...")
                X = np.array([f for f, _ in self._training_buffer])
                y = np.array([n for _, n in self._training_buffer])
                await asyncio.get_event_loop().run_in_executor(None, self._fit_model, X, y)
                self._request_count = 0
                logger.info(f"[MLLoadBalancer] ✅ Model retrained (v{self._model_version})")

    def get_status(self) -> Dict:
        return {
            "model_version": self._model_version,
            "node_ids": self.node_ids,
            "request_count": self._request_count,
            "training_buffer_size": len(self._training_buffer),
            "sklearn_available": SKLEARN_AVAILABLE,
            "cached_node_metrics": {
                nid: {
                    "cpu": round(m.cpu_usage, 3),
                    "latency_p99_ms": round(m.latency_p99, 1),
                    "request_rate": round(m.request_rate, 1),
                    "cache_hit_rate": round(m.cache_hit_rate, 3),
                }
                for nid, m in self._metrics_cache.items()
            },
        }
