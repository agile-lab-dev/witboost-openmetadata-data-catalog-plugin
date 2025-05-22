from __future__ import annotations

import uuid

from fastapi import Request
from fastapi.middleware.cors import CORSMiddleware
from loguru import logger
from starlette.background import BackgroundTask
from starlette.responses import Response

from src.app_config import app
from src.check_return_type import check_response
from src.dependencies import (
    ProvisionServiceDep,
    UnpackedProvisioningRequestDep,
    UnpackedUnprovisioningRequestDep,
    UnpackedUpdateAclRequestDep,
)
from src.models.api_models import (
    ProvisioningStatus,
    SystemErr,
    ValidationError,
    ValidationRequest,
    ValidationResult,
    ValidationStatus,
)
from src.routers.customurlpicker_router import router as customurlpicker_router


def log_info(req_body, res_code, res_body):
    id = str(uuid.uuid4())
    logger.info("[{}] REQUEST: {}", id, req_body.decode("utf-8"))
    logger.info("[{}] RESPONSE({}): {}", id, res_code, res_body.decode("utf-8"))


@app.middleware("http")
async def log_request_response_middleware(request: Request, call_next):
    req_body = await request.body()
    response = await call_next(request)
    chunks = []
    async for chunk in response.body_iterator:
        chunks.append(chunk)
    res_body = b"".join(chunks)
    task = BackgroundTask(log_info, req_body, response.status_code, res_body)
    return Response(
        content=res_body,
        status_code=response.status_code,
        headers=dict(response.headers),
        media_type=response.media_type,
        background=task,
    )


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(customurlpicker_router)


@app.post(
    "/v1/provision",
    response_model=None,
    responses={
        "200": {"model": ProvisioningStatus},
        "202": {"model": str},
        "400": {"model": ValidationError},
        "500": {"model": SystemErr},
    },
    tags=["SpecificProvisioner"],
)
def provision(
    request: UnpackedProvisioningRequestDep, provision_service: ProvisionServiceDep
) -> Response:
    """
    Deploy a data product or a single component starting from a provisioning descriptor
    """
    if isinstance(request, ValidationError):
        return check_response(out_response=request)
    resp = provision_service.provision(request)
    return check_response(out_response=resp)


@app.get(
    "/v1/provision/{token}/status",
    response_model=None,
    responses={
        "200": {"model": ProvisioningStatus},
        "400": {"model": ValidationError},
        "500": {"model": SystemErr},
    },
    tags=["SpecificProvisioner"],
)
def get_status(token: str) -> Response:
    """
    Get the status for a provisioning request
    """

    # todo: define correct response
    resp = SystemErr(error="Response not yet implemented")

    return check_response(out_response=resp)


@app.post(
    "/v1/unprovision",
    response_model=None,
    responses={
        "200": {"model": ProvisioningStatus},
        "202": {"model": str},
        "400": {"model": ValidationError},
        "500": {"model": SystemErr},
    },
    tags=["SpecificProvisioner"],
)
def unprovision(
    request: UnpackedUnprovisioningRequestDep, provision_service: ProvisionServiceDep
) -> Response:
    """
    Undeploy a data product or a single component
    given the provisioning descriptor relative to the latest complete provisioning request
    """  # noqa: E501

    if isinstance(request, ValidationError):
        return check_response(out_response=request)

    data_product, remove_data = request
    resp = provision_service.unprovision(data_product, remove_data)
    return check_response(out_response=resp)


@app.post(
    "/v1/updateacl",
    response_model=None,
    responses={
        "200": {"model": ProvisioningStatus},
        "202": {"model": str},
        "400": {"model": ValidationError},
        "500": {"model": SystemErr},
    },
    tags=["SpecificProvisioner"],
)
def updateacl(request: UnpackedUpdateAclRequestDep) -> Response:
    """
    Request the access to a specific provisioner component
    """

    if isinstance(request, ValidationError):
        return check_response(out_response=request)

    data_product, witboost_users = request

    # todo: define correct response. You can define your pydantic component type with the expected specific schema
    #  and use `.get_type_component_by_id` to extract it from the data product

    # componentToProvision = data_product.get_typed_component_by_id(component_id, MyTypedComponent)

    resp = SystemErr(error="Response not yet implemented")

    return check_response(out_response=resp)


@app.post(
    "/v1/validate",
    response_model=None,
    responses={"200": {"model": ValidationResult}, "500": {"model": SystemErr}},
    tags=["SpecificProvisioner"],
)
def validate(
    request: UnpackedProvisioningRequestDep, provision_service: ProvisionServiceDep
) -> Response:
    """
    Validate a provisioning request
    """
    if isinstance(request, ValidationError):
        return check_response(ValidationResult(valid=False, error=request))
    validate_res = provision_service.validate(request)
    if validate_res is None:
        return check_response(out_response=ValidationResult(valid=True))
    if isinstance(validate_res, ValidationError):
        return check_response(ValidationResult(valid=False, error=validate_res))
    return check_response(validate_res)


@app.post(
    "/v2/validate",
    response_model=None,
    responses={
        "202": {"model": str},
        "400": {"model": ValidationError},
        "500": {"model": SystemErr},
    },
    tags=["SpecificProvisioner"],
)
def async_validate(
    body: ValidationRequest,
) -> Response:
    """
    Validate a deployment request
    """

    # todo: define correct response. You can define your pydantic component type with the expected specific schema
    #  and use `.get_type_component_by_id` to extract it from the data product

    # componentToProvision = data_product.get_typed_component_by_id(component_id, MyTypedComponent)

    resp = SystemErr(error="Response not yet implemented")

    return check_response(out_response=resp)


@app.get(
    "/v2/validate/{token}/status",
    response_model=None,
    responses={
        "200": {"model": ValidationStatus},
        "400": {"model": ValidationError},
        "500": {"model": SystemErr},
    },
    tags=["SpecificProvisioner"],
)
def get_validation_status(
    token: str,
) -> Response:
    """
    Get the status for a provisioning request
    """

    # todo: define correct response
    resp = SystemErr(error="Response not yet implemented")

    return check_response(out_response=resp)
