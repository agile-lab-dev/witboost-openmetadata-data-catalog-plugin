from pathlib import Path
from unittest.mock import Mock

import pytest
import yaml

from src.models.api_models import (
    ProvisioningStatus,
    Status1,
    SystemErr,
    ValidationError,
)
from src.models.data_product_descriptor import (
    DataContract,
    DataProduct,
    OpenMetadataColumn,
    OpenMetadataTagLabel,
    OutputPort,
    TagSourceTagLabel,
)
from src.models.service_error import ServiceError
from src.services.provision_service import ProvisionService
from src.utility.parsing_pydantic_models import parse_yaml_with_model


@pytest.fixture(name="get_descriptor")
def descriptor_str_fixture():
    def get_descriptor(param):
        return Path(f"tests/descriptors/{param}").read_text()

    return get_descriptor


@pytest.fixture(name="unpacked_request")
def unpacked_request_fixture(get_descriptor, request):
    request = yaml.safe_load(get_descriptor(request.param))
    data_product = parse_yaml_with_model(request, DataProduct)
    if isinstance(data_product, ValidationError):
        raise ValueError("Failed to parse the descriptor")
    return data_product


@pytest.fixture
def mock_openmetadata_client():
    return Mock()


@pytest.fixture
def provision_service(mock_openmetadata_client):
    return ProvisionService(mock_openmetadata_client)


@pytest.fixture
def sample_dp():
    op = OutputPort(
        id="urn:dmb:cmp:healthcare:vaccinations:0:output-port",
        name="Test Output Port",
        description="Test Description",
        kind="outputport",
        platform="TestPlatform",
        technology="TestTech",
        specific=dict(),
        version="1.0.0",
        infrastructureTemplateId="",
        dependsOn=[],
        outputPortType="SQL",
        dataContract=DataContract(
            schema=[
                OpenMetadataColumn(
                    name="test_column",
                    dataType="STRING",
                    description="Test description",
                    tags=[
                        OpenMetadataTagLabel(
                            tagFQN="classification.tag1",
                            labelType="Manual",
                            state="Confirmed",
                            source=TagSourceTagLabel.CLASSIFICATION,
                        ),
                        OpenMetadataTagLabel(
                            tagFQN="glossary.term1",
                            labelType="Manual",
                            state="Confirmed",
                            source=TagSourceTagLabel.GLOSSARY,
                        ),
                    ],
                )
            ]
        ),
        tags=[],
        semanticLinking=[],
    )
    dp = DataProduct(
        id="urn:dmb:cmp:healthcare:vaccinations:0",
        name="Test DP",
        description="Test Description",
        domain="domain",
        kind="dataproduct",
        version="1.0.0",
        environment="dev",
        dataProductOwner="user:owner",
        ownerGroup="group:dev",
        devGroup="group:dev",
        tags=[],
        specific=dict(),
        components=[op],
    )
    return dp


@pytest.mark.parametrize(
    "unpacked_request", ["descriptor_output_port_valid.yaml"], indirect=True
)
def test_provision_success(
    provision_service, mock_openmetadata_client, unpacked_request
):
    mock_openmetadata_client._get_dp_name_from_id.return_value = "DP Name"
    mock_openmetadata_client.get_base_url.return_value = "http://localhost:8585/"

    result = provision_service.provision(unpacked_request)

    assert isinstance(result, ProvisioningStatus)
    assert result.status == Status1.COMPLETED
    mock_openmetadata_client.create_or_update_generic_storage_service.assert_called_once()
    mock_openmetadata_client.create_or_update_container_custom_attributes.assert_called_once()
    mock_openmetadata_client.create_or_update_domain.assert_called_once_with(
        unpacked_request.domain
    )
    mock_openmetadata_client.create_or_update_dp.assert_called_once_with(
        unpacked_request
    )
    mock_openmetadata_client.create_or_update_op.assert_called_once()


@pytest.mark.parametrize(
    "unpacked_request", ["descriptor_output_port_valid.yaml"], indirect=True
)
def test_provision_failure(
    provision_service, mock_openmetadata_client, unpacked_request
):
    error_msg = "Test error"
    mock_openmetadata_client.create_or_update_generic_storage_service.side_effect = (
        ServiceError(error_msg)
    )

    result = provision_service.provision(unpacked_request)

    assert isinstance(result, SystemErr)
    assert result.error == error_msg


@pytest.mark.parametrize(
    "unpacked_request", ["descriptor_output_port_valid.yaml"], indirect=True
)
def test_unprovision_success(
    provision_service, mock_openmetadata_client, unpacked_request
):
    result = provision_service.unprovision(unpacked_request, remove_data=True)

    assert isinstance(result, ProvisioningStatus)
    assert result.status == Status1.COMPLETED
    mock_openmetadata_client.delete_op.assert_called_once()
    mock_openmetadata_client.delete_dp.assert_called_once_with(unpacked_request)


@pytest.mark.parametrize(
    "unpacked_request", ["descriptor_output_port_valid.yaml"], indirect=True
)
def test_unprovision_failure(
    provision_service, mock_openmetadata_client, unpacked_request
):
    error_msg = "Test error"
    mock_openmetadata_client.delete_op.side_effect = ServiceError(error_msg)

    result = provision_service.unprovision(unpacked_request, remove_data=True)

    assert isinstance(result, SystemErr)
    assert result.error == error_msg


def test_validate_success(provision_service, mock_openmetadata_client, sample_dp):
    mock_tag = Mock()
    mock_tag.fullyQualifiedName = "classification.tag1"
    mock_term = Mock()
    mock_term.fullyQualifiedName.root = "glossary.term1"
    mock_openmetadata_client.get_all_classification_tags.return_value = [mock_tag]
    mock_openmetadata_client.get_all_glossary_terms.return_value = [mock_term]

    result = provision_service.validate(sample_dp)

    assert result is None


def test_validate_missing_classification_tags(
    provision_service, mock_openmetadata_client, sample_dp
):
    mock_term = Mock()
    mock_term.fullyQualifiedName.root = "glossary.term1"
    mock_openmetadata_client.get_all_classification_tags.return_value = []
    mock_openmetadata_client.get_all_glossary_terms.return_value = [mock_term]

    result = provision_service.validate(sample_dp)

    assert isinstance(result, ValidationError)
    assert "Missing classification tags classification.tag1" in result.errors[0]


def test_validate_missing_glossary_terms(
    provision_service, mock_openmetadata_client, sample_dp
):
    mock_tag = Mock()
    mock_tag.fullyQualifiedName = "classification.tag1"
    mock_openmetadata_client.get_all_classification_tags.return_value = [mock_tag]
    mock_openmetadata_client.get_all_glossary_terms.return_value = []

    result = provision_service.validate(sample_dp)

    assert isinstance(result, ValidationError)
    assert "Missing glossary terms glossary.term1" in result.errors[0]


def test_validate_missing_both_types(
    provision_service, mock_openmetadata_client, sample_dp
):
    mock_openmetadata_client.get_all_classification_tags.return_value = []
    mock_openmetadata_client.get_all_glossary_terms.return_value = []

    result = provision_service.validate(sample_dp)

    assert isinstance(result, ValidationError)
    assert len(result.errors) == 2
    assert "Missing classification tags" in result.errors[0]
    assert "Missing glossary terms" in result.errors[1]


def test_validate_service_error(provision_service, mock_openmetadata_client, sample_dp):
    mock_openmetadata_client.get_all_classification_tags.side_effect = ServiceError(
        "Test error"
    )

    result = provision_service.validate(sample_dp)

    assert isinstance(result, SystemErr)
    assert result.error == "Test error"


def test_validate_empty_schema(provision_service, mock_openmetadata_client, sample_dp):
    mock_openmetadata_client.get_all_classification_tags.return_value = []
    mock_openmetadata_client.get_all_glossary_terms.return_value = []
    modified_dp = sample_dp.model_copy(
        update={
            "components": [
                OutputPort(
                    id="urn:dmb:cmp:healthcare:vaccinations:0:output-port",
                    name="Test Output Port",
                    description="Test Description",
                    kind="outputport",
                    platform="TestPlatform",
                    technology="TestTech",
                    specific=dict(),
                    version="1.0.0",
                    infrastructureTemplateId="",
                    dependsOn=[],
                    outputPortType="SQL",
                    dataContract=DataContract(schema=[]),
                    tags=[],
                    semanticLinking=[],
                )
            ]
        }
    )

    result = provision_service.validate(modified_dp)

    assert result is None


def test_validate_no_tags(provision_service, mock_openmetadata_client, sample_dp):
    mock_openmetadata_client.get_all_classification_tags.return_value = []
    mock_openmetadata_client.get_all_glossary_terms.return_value = []
    modified_dp = sample_dp.model_copy(
        update={
            "components": [
                OutputPort(
                    id="urn:dmb:cmp:healthcare:vaccinations:0:output-port",
                    name="Test Output Port",
                    description="Test Description",
                    kind="outputport",
                    platform="TestPlatform",
                    technology="TestTech",
                    specific=dict(),
                    version="1.0.0",
                    infrastructureTemplateId="",
                    dependsOn=[],
                    outputPortType="SQL",
                    dataContract=DataContract(
                        schema=[
                            OpenMetadataColumn(
                                name="test_column",
                                dataType="STRING",
                                description="Test description",
                                tags=None,
                            )
                        ]
                    ),
                    tags=[],
                    semanticLinking=[],
                )
            ]
        }
    )

    result = provision_service.validate(modified_dp)

    assert result is None
