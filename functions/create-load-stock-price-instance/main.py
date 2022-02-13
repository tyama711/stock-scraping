#!/usr/bin/env python3
import google.cloud.compute_v1 as compute_v1
import requests
from dateutil.parser import isoparse
from google.cloud.functions.context import Context
from google.pubsub import PubsubMessage


def create_instance_from_template(
        project_id: str, zone: str, instance_name: str,
        instance_template_url: str) -> compute_v1.Instance:
    """
    Creates a Compute Engine VM instance from an instance template.

    Args:
        project_id: ID or number of the project you want to use.
        zone: Name of the zone you want to check, for example: us-west3-b
        instance_name: Name of the new instance.
        instance_template_url: URL of the instance template used for creating the new instance.
            It can be a full or partial URL.
            Examples:
            - https://www.googleapis.com/compute/v1/projects/project/global/instanceTemplates/example-instance-template
            - projects/project/global/instanceTemplates/example-instance-template
            - global/instanceTemplates/example-instance-template

    Returns:
        Instance object.
    """
    operation_client = compute_v1.ZoneOperationsClient()
    instance_client = compute_v1.InstancesClient()

    instance_insert_request = compute_v1.InsertInstanceRequest()
    instance_insert_request.project = project_id
    instance_insert_request.zone = zone
    instance_insert_request.source_instance_template = instance_template_url
    instance_insert_request.instance_resource.name = instance_name

    op = instance_client.insert_unary(instance_insert_request)
    operation_client.wait(project=project_id, zone=zone, operation=op.name)

    return instance_client.get(project=project_id,
                               zone=zone,
                               instance=instance_name)


def create_load_stock_price_instance(event: PubsubMessage,
                                     context: Context) -> None:
    resp = requests.get(
        "http://metadata.google.internal/computeMetadata/v1/project/project-id",
        headers={"Metadata-Flavor": "Google"})
    resp.raise_for_status()
    project_id = resp.text

    timestamp = isoparse(context.timestamp).strftime("%Y%m%d%H%M%S")

    create_instance_from_template(
        project_id=project_id,
        zone="us-central1-a",
        instance_name=f"load-stock-price-{timestamp}",
        instance_template_url=
        f"global/instanceTemplates/load-stock-price-template"
    )
