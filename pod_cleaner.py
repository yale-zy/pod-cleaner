import asyncio
import datetime
import os
import logging
import sys
import time
from typing import Any, Set, List, Dict

from kubernetes import client, config
from kubernetes.client.rest import ApiException

from prometheus_client import Counter, CollectorRegistry, push_to_gateway

HEALTHY_POD_STATUSES = {"Running", "Init"}

class PodCleaner:
    def __init__(self):
        log_level = os.getenv("LOG_LEVEL", "INFO")
        logging.basicConfig(
            level=log_level,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler("pod_cleaner.log"),
                logging.StreamHandler(sys.stdout)
            ]
        )
        self.logger = logging.getLogger(__name__)
        self.init_prometheus_metrics()

        self.excluded_namespaces: Set[str] = set(os.getenv("EXCLUDED_NAMESPACE", "kube-system").split(","))
        self.max_concurrent:int = int(os.getenv("MAX_CONCURRENT", "10"))
        self.prometheus_gateway: str = os.getenv("PROMETHEUS_GATEWAY", "")
        self.prometheus_job_name: str = os.getenv("PROMETHEUS_JOB_NAME", "pod-cleaner")
        self.verify_sleep_period: int = int(os.getenv("VERIFY_PERIOD", "10"))
        self.verify_loop_count: int = int(os.getenv("VERIFY_LOOP_COUNT", "5"))
        self.cleaned_pods: List[Dict] = []
        self.namespace_semaphore = asyncio.Semaphore(self.max_concurrent)

        try:
            if os.getenv('KUBERNETES_SERVICE_HOST'):
                self.logger.info("Loading incluster config")
                config.load_incluster_config()
            else:
                self.logger.info("Loading kube config")
                config.load_kube_config()
            self.logger.info("Kube config loaded successfully")
        except Exception as e:
            self.logger.error(f"Error loading kube config: {e}")
            raise

        self.api_client = client.CoreV1Api()

    def init_prometheus_metrics(self) -> None:
        self.registry = CollectorRegistry()
        self.pod_cleaned_metric = Counter(
            "cleaned_pods_total",
            "Total number of pods cleaned",
            ["namespace","pod"],
            registry=self.registry
        )
        self.pod_restart_failed_metric = Counter(
            "pod_restart_failed_total",
            "Total number of pod restarts failed",
            ["namespace","pod"],
            registry=self.registry
        )

    async def get_namespaces(self) -> List[str]:
        self.logger.info("Fetching namespaces")
        try:
            namespaces = self.api_client.list_namespace().items
            self.logger.info(f"Found {len(namespaces)} namespaces")
            return [ns.metadata.name for ns in namespaces if ns.metadata.name not in self.excluded_namespaces]
        except ApiException as e:
            self.logger.error(f"Error fetching namespaces: {e}")
        return []
    
    async def get_problematic_pods(self, namespace: str) -> List[client.V1Pod]:
        self.logger.info(f"Fetching pods in namespace {namespace}")
        try:
            pods = self.api_client.list_namespaced_pod(namespace).items
            self.logger.info(f"Found {len(pods)} pods in namespace {namespace}")
            return [pod for pod in pods if not self.is_pod_healthy(pod)]
        except ApiException as e:
            self.logger.error(f"Error fetching pods in namespace {namespace}: {e}")
        return []
    
    # TODO: need to check status more carefully, e.g. InitContainerStatuses
    def is_pod_healthy(self, pod: client.V1Pod) -> bool:
        phase = pod.status.phase
        if phase in HEALTHY_POD_STATUSES:
            return True
        return False
    
    async def restart_pod(self, pod: client.V1Pod, namespace: str) -> bool:
        self.logger.info(f"Restarting pod {pod.metadata.name} in namespace {namespace}")
        try:
            self.pod_cleaned_metric.labels(namespace=namespace, pod=pod.metadata.name).inc()
            self.api_client.delete_namespaced_pod(pod.metadata.name, namespace)
            self.logger.info(f"Pod {pod.metadata.name} in namespace {namespace} deleted successfully")
            self.cleaned_pods.append({
                "pod": pod.metadata.name,
                "namespace": namespace,
                "phase": pod.status.phase,
                "reason": getattr(pod.status, "reason", "Unknown"),
                "cleaned_at": datetime.datetime.now().isoformat()
            })
            return True
        except ApiException as e:
            self.logger.error(f"Error deleting pod {pod.metadata.name} in namespace {namespace}: {e}")
            self.pod_restart_failed_metric.labels(namespace=namespace, pod=pod.metadata.name).inc()
            self.cleaned_pods.append({
                "pod": pod.metadata.name,
                "namespace": namespace,
                "phase": pod.status.phase,
                "reason": getattr(pod.status, "reason", "Unknown"),
                "error": str(e),
                "cleaned_at": datetime.datetime.now().isoformat()
            })
        return False
    
    # TODO: need to enhance the logic to handle dynamic pod names of Deployment
    # TODO: need to enhance the logic to handle the pod which not under Deployment or StatefulSet or DaemonSet
    async def verify_pod_restart(self, pod: client.V1Pod, namespace: str) -> None:
        pod_name = pod.metadata.name
        self.logger.info(f"Verifying restart of pod {pod_name} in namespace {namespace}")
        is_success = False
        try:
            await asyncio.sleep(self.verify_sleep_period)
            for _ in range(self.verify_loop_count):
                try:
                    pod = self.api_client.read_namespaced_pod(pod_name, namespace)
                    self.logger.info(f"Pod {pod_name} in namespace {namespace} restarted successfully")
                    if pod.status.phase == "Running":
                        self.logger.info(f"Pod {pod_name} successfully restarted and is running")
                        is_success = True
                        break
                    elif pod.status.phase == "Failed":
                        self.pod_restart_failed_metric.labels(namespace=namespace, pod=pod_name).inc()
                        self.logger.info(f"Pod {pod_name} restart failed, waiting for it to start")
                        break
                    else:
                        self.logger.info(f"Pod {pod_name} is {pod.status.phase}, waiting for it to start")
                except ApiException as e:
                    if e.status == 404:
                        self.logger.info(f"Pod {pod_name} not found after restart, waiting for it to start")
                    else:
                        self.logger.error(f"Error checking pod {pod_name} in namespace {namespace}: {e}")
            self.logger.warning(f"Pod {pod_name} restart verification failed with timeout")
        except Exception as e:
            self.logger.error(f"Error verifying restart of pod {pod_name} in namespace {namespace}: {e}")
        if not is_success:
            await self.send_notification(pod, namespace)
    
    async def send_notification(self, pod: client.V1Pod, namespace: str) -> None:
        if not self.prometheus_gateway:
            self.logger.warning("Prometheus gateway not configured, skipping notification")
            return
        try:
            push_to_gateway(
                self.prometheus_gateway,
                job_name="pod_cleaner",
                registry=self.registry
            )
        except Exception as e:
            self.logger.error(f"Error sending notification to Prometheus gateway: {e}")

    async def clean_pods_in_namespace(self, namespace: str) -> None:
        async with self.namespace_semaphore:
            try:
                self.logger.info(f"Cleaning pods in namespace {namespace}")
                pods = await self.get_problematic_pods(namespace)
                if not pods:
                    self.logger.info(f"No pods to clean in namespace {namespace}")
                    return
                self.logger.info(f"Found {len(pods)} pods to clean in namespace {namespace}")
                restart_tasks = [self.restart_pod(pod, namespace) for pod in pods]
                await asyncio.gather(*restart_tasks)
                logging.info(f"Cleaned {len(pods)} pods in namespace {namespace}")
            
                verify_tasks = [self.verify_pod_restart(pod, namespace) for pod in pods]
                await asyncio.gather(*verify_tasks)
            except Exception as e:
                self.logger.error(f"Error cleaning pods in namespace {namespace}: {e}")
                return
            
    async def clean_pods(self) -> None:
        self.logger.info("Pod cleaner started")
        start_time = time.time()
        try:
            self.cleaned_pods = []
            namespaces = await self.get_namespaces()
            self.logger.info(f"Found {len(namespaces)} namespaces to clean")
            clean_tasks = [self.clean_pods_in_namespace(namespace) for namespace in namespaces]
            await asyncio.gather(*clean_tasks)
            self.logger.info(f"Cleaned {len(self.cleaned_pods)} pods")

            if self.cleaned_pods:
                self.logger.info("=" * 40)
                for pod in self.cleaned_pods:
                    self.logger.info(f"{pod['namespace']}/{pod['pod']}, Phase: {pod['phase']}, Reason: {pod.get('reason', 'N/A')}")
                self.logger.info("=" * 40)
            
        except Exception as e:
            self.logger.error(f"Error in pod cleaner loop: {e}")
        finally:
            self.logger.info(f"Pod cleaner finished in {time.time() - start_time:.2f} seconds")

    def run(self):
        try:
            asyncio.run(self.clean_pods())
        except KeyboardInterrupt:
            self.logger.info("Received interrupt signal, shutting down gracefully")
        except Exception as e:
            self.logger.error(f"Unexpected error: {e}")
            sys.exit(1)

if __name__ == "__main__":
    pod_cleaner = PodCleaner()
    pod_cleaner.run()   