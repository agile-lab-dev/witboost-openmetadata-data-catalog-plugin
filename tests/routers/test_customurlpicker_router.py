from unittest.mock import Mock

import pytest
from fastapi.encoders import jsonable_encoder
from fastapi.testclient import TestClient

from src.dependencies import get_glossary_terms_service
from src.main import app
from src.models.customurlpicker_models import (
    CustomUrlPickerItem,
    CustomUrlPickerMalformedRequestError,
    CustomUrlPickerResourcesRequestBody,
    CustomUrlPickerSystemError,
    CustomUrlPickerValidationError,
)


@pytest.fixture
def mock_glossary_terms_service():
    return Mock()


@pytest.fixture
def client(mock_glossary_terms_service):
    app.dependency_overrides[
        get_glossary_terms_service
    ] = lambda: mock_glossary_terms_service
    return TestClient(app)


def test_resources_success(client, mock_glossary_terms_service):
    expected_terms = [
        CustomUrlPickerItem(
            id="term1", name="Term 1", glossary="Glossary1", fqn="glossary1.term1"
        )
    ]
    mock_glossary_terms_service.get_terms.return_value = expected_terms

    response = client.post("/v1/resources?offset=0&limit=10", json={})

    assert response.status_code == 200
    assert response.json() == jsonable_encoder(expected_terms)


def test_resources_with_filter(client, mock_glossary_terms_service):
    expected_terms = [
        CustomUrlPickerItem(
            id="term1", name="Term 1", glossary="Glossary1", fqn="glossary1.term1"
        )
    ]
    mock_glossary_terms_service.get_terms.return_value = expected_terms

    response = client.post(
        "/v1/resources?offset=0&limit=10&filter=term", json={"domain": "test-domain"}
    )

    assert response.status_code == 200
    assert response.json() == jsonable_encoder(expected_terms)
    mock_glossary_terms_service.get_terms.assert_called_once_with(
        resources_request_body=CustomUrlPickerResourcesRequestBody(
            domain="test-domain"
        ),
        offset=0,
        limit=10,
        filter="term",
    )


def test_resources_system_error(client, mock_glossary_terms_service):
    error = CustomUrlPickerSystemError(errors=["Test error"])
    mock_glossary_terms_service.get_terms.return_value = error

    response = client.post(
        "/v1/resources?offset=0&limit=10", json={"domain": "test-domain"}
    )

    assert response.status_code == 500
    assert response.json() == {"errors": ["Test error"]}


def test_resources_malformed_request(client, mock_glossary_terms_service):
    error = CustomUrlPickerMalformedRequestError(errors=["Invalid request"])
    mock_glossary_terms_service.get_terms.return_value = error

    response = client.post(
        "/v1/resources?offset=0&limit=10", json={"domain": "test-domain"}
    )

    assert response.status_code == 400
    assert response.json() == {"errors": ["Invalid request"]}


def test_validate_success(client, mock_glossary_terms_service):
    mock_glossary_terms_service.validate_terms.return_value = "Validation successful"
    request_data = {
        "selectedObjects": [
            {
                "id": "term1",
                "name": "Term 1",
                "glossary": "Glossary1",
                "fqn": "glossary1.term1",
            }
        ]
    }

    response = client.post("/v1/resources/validate", json=request_data)

    assert response.status_code == 200
    assert response.text == "Validation successful"


def test_validate_error(client, mock_glossary_terms_service):
    error = CustomUrlPickerValidationError(errors=[{"error": "Term not found"}])
    mock_glossary_terms_service.validate_terms.return_value = error
    request_data = {
        "selectedObjects": [
            {
                "id": "term1",
                "name": "Term 1",
                "glossary": "Glossary1",
                "fqn": "glossary1.term1",
            }
        ]
    }

    response = client.post("/v1/resources/validate", json=request_data)

    assert response.status_code == 400
    assert response.json() == {
        "errors": [{"error": "Term not found", "suggestion": None}]
    }


def test_validate_system_error(client, mock_glossary_terms_service):
    error = CustomUrlPickerSystemError(errors=["System error"])
    mock_glossary_terms_service.validate_terms.return_value = error
    request_data = {
        "selectedObjects": [
            {
                "id": "term1",
                "name": "Term 1",
                "glossary": "Glossary1",
                "fqn": "glossary1.term1",
            }
        ]
    }

    response = client.post("/v1/resources/validate", json=request_data)

    assert response.status_code == 500
    assert response.json() == {"errors": ["System error"]}
