import uuid
from unittest.mock import Mock

import pytest
from metadata.generated.schema.api.domains.createDomain import CreateDomainRequest
from metadata.generated.schema.entity.classification.tag import Tag
from metadata.generated.schema.entity.data.container import Container
from metadata.generated.schema.entity.data.glossaryTerm import GlossaryTerm
from metadata.generated.schema.entity.data.table import DataType
from metadata.generated.schema.entity.domains.dataProduct import (
    DataProduct as OMDataProduct,
)
from metadata.generated.schema.entity.domains.domain import Domain, DomainType
from metadata.generated.schema.entity.services.storageService import (
    StorageService,
    StorageServiceType,
)
from metadata.generated.schema.type.basic import Markdown
from metadata.generated.schema.type.entityReference import EntityReference
from metadata.generated.schema.type.tagLabel import LabelType, State, TagSource

from src.models.data_product_descriptor import (
    DataContract,
    DataProduct,
    OpenMetadataColumn,
    OpenMetadataTagLabel,
    OutputPort,
)
from src.services.openmetadata_client_service import (
    OpenMetadataClientService,
    OpenMetadataClientServiceError,
)
from src.settings.openmetadata_settings import OpenMetadataSettings


@pytest.fixture
def mock_openmetadata_client(mocker):
    return mocker.MagicMock()


@pytest.fixture
def mock_settings():
    return OpenMetadataSettings(api_base_url="", jwt_token="")


@pytest.fixture
def client_service(mock_openmetadata_client, mock_settings):
    return OpenMetadataClientService(mock_openmetadata_client, mock_settings)


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
                tags=[],
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


def test_create_or_update_container_custom_attributes_success(
    client_service, mock_openmetadata_client
):
    mock_openmetadata_client.client.get.return_value = {
        "id": uuid.uuid4(),
        "name": "string",
    }

    client_service.create_or_update_container_custom_attributes()

    assert mock_openmetadata_client.create_or_update_custom_property.call_count == 3
    calls = mock_openmetadata_client.create_or_update_custom_property.call_args_list
    assert "kind" in str(calls[0])
    assert "platform" in str(calls[1])
    assert "technology" in str(calls[2])


def test_create_or_update_container_custom_attributes_failure(
    client_service, mock_openmetadata_client
):
    mock_openmetadata_client.client.get.return_value = None

    with pytest.raises(OpenMetadataClientServiceError) as exc_info:
        client_service.create_or_update_container_custom_attributes()
    assert "Failed to create or update container custom attributes" in str(
        exc_info.value
    )


def test_create_or_update_generic_storage_service_success(
    client_service, mock_openmetadata_client
):
    expected_service = StorageService(
        id=uuid.uuid4(), name="generic", serviceType=StorageServiceType.CustomStorage
    )
    mock_openmetadata_client.create_or_update.return_value = expected_service

    result = client_service.create_or_update_generic_storage_service()

    assert result == expected_service
    mock_openmetadata_client.create_or_update.assert_called_once()


def test_create_or_update_generic_storage_service_failure(
    client_service, mock_openmetadata_client
):
    mock_openmetadata_client.create_or_update.side_effect = Exception("API Error")

    with pytest.raises(OpenMetadataClientServiceError) as exc_info:
        client_service.create_or_update_generic_storage_service()
    assert "Failed to create or update storage service" in str(exc_info.value)


def test_create_or_update_domain_success(client_service, mock_openmetadata_client):
    domain_name = "test-domain"
    expected_domain = Domain(
        id=uuid.uuid4(),
        name=domain_name,
        description=Markdown(domain_name),
        domainType=DomainType.Aggregate,
    )
    mock_openmetadata_client.create_or_update.return_value = expected_domain

    result = client_service.create_or_update_domain(domain_name)

    assert result == expected_domain
    mock_openmetadata_client.create_or_update.assert_called_once()
    create_request = mock_openmetadata_client.create_or_update.call_args[1]["data"]
    assert isinstance(create_request, CreateDomainRequest)
    assert create_request.name.root == domain_name


def test_create_or_update_domain_failure(client_service, mock_openmetadata_client):
    domain_name = "test-domain"
    mock_openmetadata_client.create_or_update.side_effect = Exception("API Error")

    with pytest.raises(OpenMetadataClientServiceError) as exc_info:
        client_service.create_or_update_domain(domain_name)
    assert "Failed to create or update domain" in str(exc_info.value)


def test_create_or_update_dp_success(client_service, mock_openmetadata_client):
    expected_dp = OMDataProduct(
        id=uuid.uuid4(),
        name="healthcare:vaccinations:0",
        displayName="Test DP",
        description=Markdown("Test Description"),
    )
    mock_openmetadata_client.create_or_update.return_value = expected_dp

    result = client_service.create_or_update_dp(dp)

    assert result == expected_dp
    mock_openmetadata_client.create_or_update.assert_called_once()


def test_delete_dp_success(client_service, mock_openmetadata_client):
    mock_openmetadata_client.get_by_name.return_value = OMDataProduct(
        id=uuid.uuid4(),
        name="healthcare:vaccinations:0",
        description=Markdown("Test Description"),
    )

    client_service.delete_dp(dp)

    mock_openmetadata_client.delete.assert_called_once()


def test_delete_dp_not_found(client_service, mock_openmetadata_client):
    mock_openmetadata_client.get_by_name.return_value = None

    client_service.delete_dp(dp)

    mock_openmetadata_client.delete.assert_not_called()


def test_create_or_update_op_success(client_service, mock_openmetadata_client):
    expected_container = Container(
        id=uuid.uuid4(),
        name="healthcare:vaccinations:0:output-port",
        service=EntityReference(id=uuid.uuid4(), type="service"),
    )
    mock_openmetadata_client.create_or_update.return_value = expected_container

    result = client_service.create_or_update_op(dp, op)

    assert result == expected_container
    mock_openmetadata_client.create_or_update.assert_called_once()


def test_delete_op_success(client_service, mock_openmetadata_client):
    mock_openmetadata_client.get_by_name.return_value = Container(
        id=uuid.uuid4(),
        name="healthcare:vaccinations:0:output-port",
        service=EntityReference(id=uuid.uuid4(), type="service"),
    )

    client_service.delete_op(op)

    mock_openmetadata_client.delete.assert_called_once()


def test_delete_op_not_found(client_service, mock_openmetadata_client):
    mock_openmetadata_client.get_by_name.return_value = None

    client_service.delete_op(op)

    mock_openmetadata_client.delete.assert_not_called()


def test_get_dp_name_from_id(client_service):
    dp_id = "urn:dmb:cmp:healthcare:vaccinations:0:snowflake-output-port"

    result = client_service._get_dp_name_from_id(dp_id)

    assert result == "healthcare:vaccinations:0"


def test_get_component_name_from_id(client_service):
    component_id = "urn:dmb:cmp:healthcare:vaccinations:0:snowflake-output-port"

    result = client_service._get_component_name_from_id(component_id)

    assert result == "healthcare:vaccinations:0:snowflake-output-port"


def test_to_om_column_list_with_schema(client_service):
    result = client_service._to_om_column_list(op)

    assert len(result) == 1
    assert result[0].name.root == "test_column"
    assert result[0].dataType == DataType.STRING
    assert result[0].description.root == "Test description"


def test_to_om_tag_list_with_tags(client_service):
    test_tag = OpenMetadataTagLabel(
        tagFQN="test.tag",
        labelType="Manual",
        state="Confirmed",
        source="Classification",
    )
    test_column = OpenMetadataColumn(
        name="test_column",
        dataType="STRING",
        description="Test description",
        tags=[test_tag],
    )

    result = client_service._to_om_tag_list(test_column)

    assert len(result) == 1
    assert result[0].tagFQN.root == "test.tag"
    assert result[0].labelType == LabelType.Manual
    assert result[0].state == State.Confirmed
    assert result[0].source == TagSource.Classification


def test_to_om_tag_list_without_tags(client_service):
    test_column = OpenMetadataColumn(
        name="test_column", dataType="STRING", description="Test description", tags=None
    )

    result = client_service._to_om_tag_list(test_column)

    assert len(result) == 0


def test_get_all_classification_tags_success(client_service, mock_openmetadata_client):
    expected_tags = [Mock(spec=Tag), Mock(spec=Tag)]
    mock_openmetadata_client.list_all_entities.return_value = expected_tags

    result = client_service.get_all_classification_tags()

    assert result == expected_tags
    mock_openmetadata_client.list_all_entities.assert_called_once_with(Tag)


def test_get_all_classification_tags_failure(client_service, mock_openmetadata_client):
    mock_openmetadata_client.list_all_entities.side_effect = Exception("Test error")

    with pytest.raises(OpenMetadataClientServiceError) as exc_info:
        client_service.get_all_classification_tags()

    assert "Failed to retrieve classification tags" in str(exc_info.value)


def test_get_classification_tag_success(client_service, mock_openmetadata_client):
    expected_tag = Mock(spec=Tag)
    mock_openmetadata_client.get_by_name.return_value = expected_tag
    test_fqn = "test.tag"

    result = client_service.get_classification_tag(test_fqn)

    assert result == expected_tag
    mock_openmetadata_client.get_by_name.assert_called_once_with(Tag, test_fqn)


def test_get_classification_tag_failure(client_service, mock_openmetadata_client):
    test_fqn = "test.tag"
    mock_openmetadata_client.get_by_name.side_effect = Exception("Test error")

    with pytest.raises(OpenMetadataClientServiceError) as exc_info:
        client_service.get_classification_tag(test_fqn)

    assert f"Failed to retrieve classification tag {test_fqn}" in str(exc_info.value)


def test_get_classification_tag_not_found(client_service, mock_openmetadata_client):
    test_fqn = "test.tag"
    mock_openmetadata_client.get_by_name.return_value = None

    result = client_service.get_classification_tag(test_fqn)

    assert result is None
    mock_openmetadata_client.get_by_name.assert_called_once_with(Tag, test_fqn)


def test_get_all_glossary_terms_success(client_service, mock_openmetadata_client):
    expected_terms = [Mock(spec=GlossaryTerm), Mock(spec=GlossaryTerm)]
    mock_openmetadata_client.list_all_entities.return_value = expected_terms

    result = client_service.get_all_glossary_terms()

    assert result == expected_terms
    mock_openmetadata_client.list_all_entities.assert_called_once_with(GlossaryTerm)


def test_get_all_glossary_terms_failure(client_service, mock_openmetadata_client):
    mock_openmetadata_client.list_all_entities.side_effect = Exception("Test error")

    with pytest.raises(OpenMetadataClientServiceError) as exc_info:
        client_service.get_all_glossary_terms()

    assert "Failed to retrieve glossary terms" in str(exc_info.value)


def test_get_glossary_term_success(client_service, mock_openmetadata_client):
    expected_term = Mock(spec=GlossaryTerm)
    mock_openmetadata_client.get_by_name.return_value = expected_term
    test_fqn = "test.term"

    result = client_service.get_glossary_term(test_fqn)

    assert result == expected_term
    mock_openmetadata_client.get_by_name.assert_called_once_with(GlossaryTerm, test_fqn)


def test_get_glossary_term_failure(client_service, mock_openmetadata_client):
    test_fqn = "test.term"
    mock_openmetadata_client.get_by_name.side_effect = Exception("Test error")

    with pytest.raises(OpenMetadataClientServiceError) as exc_info:
        client_service.get_glossary_term(test_fqn)

    assert f"Failed to retrieve glossary term {test_fqn}" in str(exc_info.value)


def test_get_glossary_term_not_found(client_service, mock_openmetadata_client):
    test_fqn = "test.term"
    mock_openmetadata_client.get_by_name.return_value = None

    result = client_service.get_glossary_term(test_fqn)

    assert result is None
    mock_openmetadata_client.get_by_name.assert_called_once_with(GlossaryTerm, test_fqn)
