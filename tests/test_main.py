from pathlib import Path
from unittest.mock import Mock

from fastapi.encoders import jsonable_encoder
from starlette.testclient import TestClient

from src.dependencies import get_provision_service
from src.main import app
from src.models.api_models import (
    DescriptorKind,
    ProvisionInfo,
    ProvisioningRequest,
    ProvisioningStatus,
    Status1,
    SystemErr,
    UpdateAclRequest,
)

client = TestClient(app)


def test_provisioning_invalid_descriptor():
    provisioning_request = ProvisioningRequest(
        descriptorKind=DescriptorKind.DATAPRODUCT_DESCRIPTOR, descriptor="descriptor"
    )

    def mock_provision_service():
        m = Mock()
        return m

    app.dependency_overrides[get_provision_service] = mock_provision_service

    resp = client.post("/v1/provision", json=dict(provisioning_request))

    app.dependency_overrides = {}
    assert resp.status_code == 400
    assert "Unable to parse the descriptor." in resp.json().get("errors")


def test_provisioning_ok():
    descriptor_str = Path(
        "tests/descriptors/descriptor_output_port_valid.yaml"
    ).read_text()
    provisioning_request = ProvisioningRequest(
        descriptorKind=DescriptorKind.DATAPRODUCT_DESCRIPTOR, descriptor=descriptor_str
    )

    def mock_provision_service():
        m = Mock()
        m.provision.return_value = ProvisioningStatus(
            status=Status1.COMPLETED, result=""
        )
        return m

    app.dependency_overrides[get_provision_service] = mock_provision_service

    resp = client.post("/v1/provision", json=jsonable_encoder(provisioning_request))

    app.dependency_overrides = {}
    assert resp.status_code == 200
    assert resp.json() == {"info": None, "result": "", "status": "COMPLETED"}


def test_provisioning_ko():
    descriptor_str = Path(
        "tests/descriptors/descriptor_output_port_valid.yaml"
    ).read_text()
    provisioning_request = ProvisioningRequest(
        descriptorKind=DescriptorKind.DATAPRODUCT_DESCRIPTOR, descriptor=descriptor_str
    )
    error_msg = "unexpected error"

    def mock_provision_service():
        m = Mock()
        m.provision.return_value = SystemErr(error=error_msg)
        return m

    app.dependency_overrides[get_provision_service] = mock_provision_service

    resp = client.post("/v1/provision", json=jsonable_encoder(provisioning_request))

    app.dependency_overrides = {}
    assert resp.status_code == 500
    assert resp.json() == {"error": error_msg}


def test_unprovisioning_invalid_descriptor():
    unprovisioning_request = ProvisioningRequest(
        descriptorKind=DescriptorKind.DATAPRODUCT_DESCRIPTOR, descriptor="descriptor"
    )

    def mock_provision_service():
        m = Mock()
        return m

    app.dependency_overrides[get_provision_service] = mock_provision_service

    resp = client.post("/v1/unprovision", json=dict(unprovisioning_request))

    app.dependency_overrides = {}
    assert resp.status_code == 400
    assert "Unable to parse the descriptor." in resp.json().get("errors")


def test_unprovisioning_ok():
    descriptor_str = Path(
        "tests/descriptors/descriptor_output_port_valid.yaml"
    ).read_text()

    unprovisioning_request = ProvisioningRequest(
        descriptorKind=DescriptorKind.DATAPRODUCT_DESCRIPTOR, descriptor=descriptor_str
    )

    def mock_provision_service():
        m = Mock()
        m.unprovision.return_value = ProvisioningStatus(
            status=Status1.COMPLETED, result=""
        )
        return m

    app.dependency_overrides[get_provision_service] = mock_provision_service

    resp = client.post("/v1/unprovision", json=jsonable_encoder(unprovisioning_request))

    app.dependency_overrides = {}
    assert resp.status_code == 200
    assert resp.json() == {"info": None, "result": "", "status": "COMPLETED"}


def test_unprovisioning_ko():
    descriptor_str = Path(
        "tests/descriptors/descriptor_output_port_valid.yaml"
    ).read_text()
    provisioning_request = ProvisioningRequest(
        descriptorKind=DescriptorKind.DATAPRODUCT_DESCRIPTOR, descriptor=descriptor_str
    )
    error_msg = "unexpected error"

    def mock_provision_service():
        m = Mock()
        m.unprovision.return_value = SystemErr(error=error_msg)
        return m

    app.dependency_overrides[get_provision_service] = mock_provision_service

    resp = client.post("/v1/unprovision", json=jsonable_encoder(provisioning_request))

    app.dependency_overrides = {}
    assert resp.status_code == 500
    assert resp.json() == {"error": error_msg}


def test_validate_invalid_descriptor():
    validate_request = ProvisioningRequest(
        descriptorKind=DescriptorKind.DATAPRODUCT_DESCRIPTOR, descriptor="descriptor"
    )

    def mock_provision_service():
        return Mock()

    app.dependency_overrides[get_provision_service] = mock_provision_service

    resp = client.post("/v1/validate", json=dict(validate_request))

    assert resp.status_code == 200
    assert "Unable to parse the descriptor." in resp.json().get("error").get("errors")


def test_validate_valid_descriptor():
    descriptor_str = Path(
        "tests/descriptors/descriptor_output_port_valid.yaml"
    ).read_text()
    validate_request = ProvisioningRequest(
        descriptorKind=DescriptorKind.DATAPRODUCT_DESCRIPTOR, descriptor=descriptor_str
    )

    def mock_provision_service():
        m = Mock()
        m.validate.return_value = None
        return m

    app.dependency_overrides[get_provision_service] = mock_provision_service

    resp = client.post("/v1/validate", json=dict(validate_request))

    assert resp.status_code == 200
    assert {"error": None, "valid": True} == resp.json()


def test_updateacl_invalid_descriptor():
    updateacl_request = UpdateAclRequest(
        provisionInfo=ProvisionInfo(request="descriptor", result=""),
        refs=["user:alice", "user:bob"],
    )

    resp = client.post("/v1/updateacl", json=jsonable_encoder(updateacl_request))

    assert resp.status_code == 400
    assert "Unable to parse the descriptor." in resp.json().get("errors")


def test_updateacl_valid_descriptor():
    descriptor_str = Path(
        "tests/descriptors/descriptor_output_port_valid.yaml"
    ).read_text()

    updateacl_request = UpdateAclRequest(
        provisionInfo=ProvisionInfo(request=descriptor_str, result=""),
        refs=["user:alice", "user:bob"],
    )

    resp = client.post("/v1/updateacl", json=jsonable_encoder(updateacl_request))

    assert resp.status_code == 500
    assert "Response not yet implemented" in resp.json().get("error")
