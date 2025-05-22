import uuid
from unittest.mock import Mock

import pytest
from metadata.generated.schema.type.entityReference import EntityReference

from src.models.customurlpicker_models import (
    CustomUrlPickerItem,
    CustomUrlPickerResourcesRequestBody,
    CustomUrlPickerSystemError,
    CustomUrlPickerValidationError,
    CustomUrlPickerValidationRequest,
)
from src.models.service_error import ServiceError
from src.services.glossary_terms_service import GlossaryTermsService


@pytest.fixture
def mock_openmetadata_client_service():
    return Mock()


@pytest.fixture
def glossary_terms_service(mock_openmetadata_client_service):
    return GlossaryTermsService(mock_openmetadata_client_service)


@pytest.fixture
def sample_term():
    term = Mock()
    term.fullyQualifiedName.root = "glossary1.term1"
    term.name.root = "term1"
    term.glossary = EntityReference(id=uuid.uuid4(), type="Glossary", name="glossary1")
    term.domain = EntityReference(id=uuid.uuid4(), type="Glossary", name="domain1")
    return term


def test_get_terms_success_no_filters(
    glossary_terms_service, mock_openmetadata_client_service, sample_term
):
    mock_openmetadata_client_service.get_all_glossary_terms.return_value = [sample_term]

    result = glossary_terms_service.get_terms(None, 0, 10, None)

    assert len(result) == 1
    assert isinstance(result[0], CustomUrlPickerItem)
    assert result[0].id == "glossary1.term1"
    assert result[0].name == "term1"
    assert result[0].glossary == "glossary1"


def test_get_terms_with_text_filter(
    glossary_terms_service, mock_openmetadata_client_service, sample_term
):
    mock_openmetadata_client_service.get_all_glossary_terms.return_value = [sample_term]

    result = glossary_terms_service.get_terms(None, 0, 10, "term")

    assert len(result) == 1
    assert result[0].fqn == "glossary1.term1"


def test_get_terms_with_domain_filter(
    glossary_terms_service, mock_openmetadata_client_service, sample_term
):
    mock_openmetadata_client_service.get_all_glossary_terms.return_value = [sample_term]
    request_body = CustomUrlPickerResourcesRequestBody(domain="domain1")

    result = glossary_terms_service.get_terms(request_body, 0, 10, None)

    assert len(result) == 1
    assert result[0].fqn == "glossary1.term1"


def test_get_terms_with_pagination(
    glossary_terms_service, mock_openmetadata_client_service, sample_term
):
    term2 = Mock()
    term2.fullyQualifiedName.root = "glossary1.term2"
    term2.name.root = "term2"
    term2.glossary = EntityReference(id=uuid.uuid4(), type="Glossary", name="glossary1")
    mock_openmetadata_client_service.get_all_glossary_terms.return_value = [
        sample_term,
        term2,
    ]

    result = glossary_terms_service.get_terms(None, 0, 1, None)

    assert len(result) == 1
    assert result[0].fqn == "glossary1.term1"


def test_get_terms_empty_results(
    glossary_terms_service, mock_openmetadata_client_service
):
    mock_openmetadata_client_service.get_all_glossary_terms.return_value = []

    result = glossary_terms_service.get_terms(None, 0, 10, None)

    assert len(result) == 0


def test_get_terms_service_error(
    glossary_terms_service, mock_openmetadata_client_service
):
    mock_openmetadata_client_service.get_all_glossary_terms.side_effect = ServiceError(
        "Test error"
    )

    result = glossary_terms_service.get_terms(None, 0, 10, None)

    assert isinstance(result, CustomUrlPickerSystemError)
    assert result.errors[0] == "Test error"


def test_validate_terms_success(
    glossary_terms_service, mock_openmetadata_client_service, sample_term
):
    mock_openmetadata_client_service.get_glossary_term.return_value = sample_term
    validation_request = CustomUrlPickerValidationRequest(
        selectedObjects=[
            CustomUrlPickerItem(
                id="1", name="term1", fqn="glossary1.term1", glossary="glossary1"
            )
        ]
    )

    result = glossary_terms_service.validate_terms(validation_request)

    assert result == "Validation successful"


def test_validate_terms_not_found(
    glossary_terms_service, mock_openmetadata_client_service
):
    mock_openmetadata_client_service.get_glossary_term.return_value = None
    validation_request = CustomUrlPickerValidationRequest(
        selectedObjects=[
            CustomUrlPickerItem(
                id="1", name="term1", fqn="glossary1.term1", glossary="glossary1"
            )
        ]
    )

    result = glossary_terms_service.validate_terms(validation_request)

    assert isinstance(result, CustomUrlPickerValidationError)
    assert "Glossary term glossary1.term1 not found" in result.errors[0].error


def test_validate_terms_service_error(
    glossary_terms_service, mock_openmetadata_client_service
):
    mock_openmetadata_client_service.get_glossary_term.side_effect = ServiceError(
        "Test error"
    )
    validation_request = CustomUrlPickerValidationRequest(
        selectedObjects=[
            CustomUrlPickerItem(
                id="1", name="term1", fqn="glossary1.term1", glossary="glossary1"
            )
        ]
    )

    result = glossary_terms_service.validate_terms(validation_request)

    assert isinstance(result, CustomUrlPickerSystemError)
    assert result.errors[0] == "Test error"
