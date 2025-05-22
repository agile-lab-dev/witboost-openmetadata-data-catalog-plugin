from fastapi import APIRouter, Response
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse

from src.check_return_type import check_response
from src.dependencies import GlossaryTermsServiceDep
from src.models.customurlpicker_models import (
    CustomUrlPickerItem,
    CustomUrlPickerMalformedRequestError,
    CustomUrlPickerResourcesRequestBody,
    CustomUrlPickerSystemError,
    CustomUrlPickerValidationError,
    CustomUrlPickerValidationRequest,
)

router = APIRouter()


@router.post(
    "/v1/resources",
    response_model=None,
    responses={
        200: {"model": list[CustomUrlPickerItem]},
        400: {"model": CustomUrlPickerMalformedRequestError},
        500: {"model": CustomUrlPickerSystemError},
    },
    tags=["CustomUrlPicker"],
)
def resources(
    glossary_terms_service: GlossaryTermsServiceDep,
    offset: int,
    limit: int,
    filter: str | None = None,
    resources_request_body: CustomUrlPickerResourcesRequestBody | None = None,
) -> Response:
    terms = glossary_terms_service.get_terms(
        resources_request_body=resources_request_body,
        offset=offset,
        limit=limit,
        filter=filter,
    )
    json_data = jsonable_encoder(terms)
    if isinstance(terms, CustomUrlPickerMalformedRequestError):
        return JSONResponse(content=json_data, status_code=400)
    elif isinstance(terms, CustomUrlPickerSystemError):
        return JSONResponse(content=json_data, status_code=500)
    return JSONResponse(content=json_data, status_code=200)


@router.post(
    "/v1/resources/validate",
    response_model=None,
    responses={
        200: {"model": str},
        400: {"model": CustomUrlPickerValidationError},
        500: {"model": CustomUrlPickerSystemError},
    },
    tags=["CustomUrlPicker"],
)
def resources_validate(
    validation_request: CustomUrlPickerValidationRequest,
    glossary_terms_service: GlossaryTermsServiceDep,
) -> Response:
    return check_response(
        out_response=glossary_terms_service.validate_terms(validation_request)
    )
