from fastapi import FastAPI
from prometheus_fastapi_instrumentator import Instrumentator

from prometheus_client.core import (
    REGISTRY,
    GaugeMetricFamily,
)

import docker

app = FastAPI()

client = docker.DockerClient(
    base_url="unix:///var/run/docker.sock"
)

class DockerCollector:

    def collect(self):

        metric = GaugeMetricFamily(
            "docker_container_info",
            "Docker container information",
            labels=[
                "container_id",
                "name",
                "image",
                "state",
                "compose_service",
                "compose_project",
            ],
        )

        for container in client.containers.list(all=True):

            labels = container.labels

            image = (
                container.image.tags[0]
                if container.image.tags
                else "unknown"
            )

            metric.add_metric(
                [
                    container.id,
                    container.name,
                    image,
                    container.status,
                    labels.get(
                        "com.docker.compose.service",
                        "",
                    ),
                    labels.get(
                        "com.docker.compose.project",
                        "",
                    ),
                ],
                1,
            )

        yield metric


REGISTRY.register(DockerCollector())

Instrumentator().instrument(app).expose(app)


@app.get("/health")
def health():
    return {"status": "ok"}