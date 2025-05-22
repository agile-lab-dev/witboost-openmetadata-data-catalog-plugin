from src.models.customurlpicker_models import (
    CustomUrlPickerError,
    CustomUrlPickerItem,
    CustomUrlPickerMalformedRequestError,
    CustomUrlPickerResourcesRequestBody,
    CustomUrlPickerSystemError,
    CustomUrlPickerValidationError,
    CustomUrlPickerValidationRequest,
)
from src.models.service_error import ServiceError
from src.services.openmetadata_client_service import OpenMetadataClientService


class GlossaryTermsService:
    def __init__(
        self,
        openmetadata_client_service: OpenMetadataClientService,
    ):
        self.openmetadata_client_service = openmetadata_client_service

    def get_terms(
        self,
        resources_request_body: CustomUrlPickerResourcesRequestBody | None,
        offset: int,
        limit: int,
        filter: str | None,
    ) -> (
        list[CustomUrlPickerItem]
        | CustomUrlPickerMalformedRequestError
        | CustomUrlPickerSystemError
    ):
        try:
            all_terms = self.openmetadata_client_service.get_all_glossary_terms()
            filtered_terms = (
                [
                    term
                    for term in all_terms
                    if term.fullyQualifiedName
                    and filter.lower() in term.fullyQualifiedName.root.lower()
                ]
                if filter
                else all_terms
            )
            filtered_terms_by_domain = (
                [
                    term
                    for term in filtered_terms
                    if term.fullyQualifiedName
                    and term.domain
                    and term.domain.name
                    and resources_request_body.domain.lower()
                    in term.domain.name.lower()
                ]
                if resources_request_body and resources_request_body.domain
                else filtered_terms
            )
            paginated_terms = filtered_terms_by_domain[
                offset * limit : (offset * limit) + limit
            ]
            return [
                CustomUrlPickerItem(
                    id=term.fullyQualifiedName.root,
                    glossary=term.glossary.name,
                    name=term.name.root,
                    fqn=term.fullyQualifiedName.root,
                )
                for term in paginated_terms
                if term.fullyQualifiedName
            ]
        except ServiceError as se:
            return CustomUrlPickerSystemError(errors=[se.error_msg])

    def validate_terms(
        self, validation_request: CustomUrlPickerValidationRequest
    ) -> str | CustomUrlPickerValidationError | CustomUrlPickerSystemError:
        try:
            not_found_terms = [
                item
                for item in validation_request.selectedObjects
                if not self.openmetadata_client_service.get_glossary_term(item.fqn)
            ]
            return (
                CustomUrlPickerValidationError(
                    errors=[
                        CustomUrlPickerError(
                            error=f"Glossary term {term.fqn} not found in OpenMetadata"
                        )
                        for term in not_found_terms
                    ]
                )
                if not_found_terms
                else "Validation successful"
            )
        except ServiceError as se:
            return CustomUrlPickerSystemError(errors=[se.error_msg])
