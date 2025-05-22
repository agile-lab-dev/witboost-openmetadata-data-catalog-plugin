from functools import lru_cache
from typing import Annotated, Tuple

import yaml
from fastapi import Depends
from loguru import logger
from metadata.generated.schema.entity.services.connections.metadata.openMetadataConnection import (
    AuthProvider,
    OpenMetadataConnection,
)
from metadata.generated.schema.security.client.openMetadataJWTClientConfig import (
    OpenMetadataJWTClientConfig,
)
from metadata.ingestion.ometa.ometa_api import OpenMetadata

from src.models.api_models import (
    DescriptorKind,
    ProvisioningRequest,
    UpdateAclRequest,
    ValidationError,
)
from src.models.data_product_descriptor import DataProduct
from src.services.glossary_terms_service import GlossaryTermsService
from src.services.openmetadata_client_service import OpenMetadataClientService
from src.services.provision_service import ProvisionService
from src.settings.openmetadata_settings import OpenMetadataSettings
from src.utility.parsing_pydantic_models import parse_yaml_with_model


async def unpack_provisioning_request(
    provisioning_request: ProvisioningRequest,
) -> DataProduct | ValidationError:
    """
    Unpacks a Provisioning Request.

    This function takes a `ProvisioningRequest` object and extracts relevant information
    to perform provisioning for a data product.

    Args:
        provisioning_request (ProvisioningRequest): The provisioning request to be unpacked.

    Returns:
        Union[DataProduct, ValidationError]:
            - If successful, returns the data product.
            - If unsuccessful, returns a `ValidationError` object with error details.

    Note:
        - This function expects the `provisioning_request` to have a descriptor kind of `DescriptorKind.DATAPRODUCT_DESCRIPTOR`.
        - It will attempt to parse the descriptor and return the relevant information. If parsing fails or the descriptor kind is unexpected, a `ValidationError` will be returned.

    """  # noqa: E501

    if (
        not provisioning_request.descriptorKind == DescriptorKind.DATAPRODUCT_DESCRIPTOR
        and not provisioning_request.descriptorKind
        == DescriptorKind.DATAPRODUCT_DESCRIPTOR_WITH_RESULTS
    ):
        error = (
            "Expecting a DATAPRODUCT_DESCRIPTOR but got a "
            f"{provisioning_request.descriptorKind} instead; please check with the "
            f"platform team."
        )
        return ValidationError(errors=[error])
    try:
        descriptor_dict = yaml.safe_load(provisioning_request.descriptor)
        data_product_or_error = parse_yaml_with_model(descriptor_dict, DataProduct)
        return data_product_or_error
    except Exception as ex:
        logger.exception("Unable to parse the descriptor.")
        return ValidationError(errors=["Unable to parse the descriptor.", str(ex)])


UnpackedProvisioningRequestDep = Annotated[
    DataProduct | ValidationError,
    Depends(unpack_provisioning_request),
]


async def unpack_unprovisioning_request(
    provisioning_request: ProvisioningRequest,
) -> Tuple[DataProduct, bool] | ValidationError:
    """
    Unpacks a Unprovisioning Request.

    This function takes a `ProvisioningRequest` object and extracts relevant information
    to perform unprovisioning for a data product.

    Args:
        provisioning_request (ProvisioningRequest): The unprovisioning request to be unpacked.

    Returns:
        Union[Tuple[DataProduct, bool], ValidationError]:
            - If successful, returns a tuple containing:
                - `DataProduct`: The data product for provisioning.
                - `bool`: The value of the removeData field.
            - If unsuccessful, returns a `ValidationError` object with error details.

    Note:
        - This function expects the `provisioning_request` to have a descriptor kind of `DescriptorKind.DATAPRODUCT_DESCRIPTOR`.
        - It will attempt to parse the descriptor and return the relevant information. If parsing fails or the descriptor kind is unexpected, a `ValidationError` will be returned.

    """  # noqa: E501

    unpacked_request = await unpack_provisioning_request(provisioning_request)
    remove_data = (
        provisioning_request.removeData
        if provisioning_request.removeData is not None
        else False
    )

    if isinstance(unpacked_request, ValidationError):
        return unpacked_request
    else:
        return unpacked_request, remove_data


UnpackedUnprovisioningRequestDep = Annotated[
    Tuple[DataProduct, bool] | ValidationError,
    Depends(unpack_unprovisioning_request),
]


async def unpack_update_acl_request(
    update_acl_request: UpdateAclRequest,
) -> Tuple[DataProduct, list[str]] | ValidationError:
    """
    Unpacks an Update ACL Request.

    This function takes an `UpdateAclRequest` object and extracts relevant information
    to update access control lists (ACL) for a data product.

    Args:
        update_acl_request (UpdateAclRequest): The update ACL request to be unpacked.

    Returns:
        Union[Tuple[DataProduct, str, List[str]], ValidationError]:
            - If successful, returns a tuple containing:
                - `DataProduct`: The data product to update ACL for.
                - `List[str]`: A list of references.
            - If unsuccessful, returns a `ValidationError` object with error details.

    Note:
        This function expects the `update_acl_request` to contain a valid YAML string
        in the 'provisionInfo.request' field. It will attempt to parse the YAML and
        return the relevant information. If parsing fails, a `ValidationError` will
        be returned.

    """  # noqa: E501

    try:
        request = yaml.safe_load(update_acl_request.provisionInfo.request)
        data_product = parse_yaml_with_model(request, DataProduct)
        if isinstance(data_product, DataProduct):
            return (
                data_product,
                update_acl_request.refs,
            )
        elif isinstance(data_product, ValidationError):
            return data_product
        else:
            return ValidationError(
                errors=[
                    "An unexpected error occurred while parsing the update acl request."
                ]
            )
    except Exception as ex:
        return ValidationError(errors=["Unable to parse the descriptor.", str(ex)])


UnpackedUpdateAclRequestDep = Annotated[
    Tuple[DataProduct, list[str]] | ValidationError,
    Depends(unpack_update_acl_request),
]


@lru_cache
def get_openmetadata_settings() -> OpenMetadataSettings:
    return OpenMetadataSettings()


def get_openmetadata_client_service(
    openmetadata_settings: Annotated[
        OpenMetadataSettings, Depends(get_openmetadata_settings)
    ]
) -> OpenMetadataClientService:
    server_config = OpenMetadataConnection(  # type: ignore
        hostPort=openmetadata_settings.api_base_url,
        authProvider=AuthProvider.openmetadata,
        securityConfig=OpenMetadataJWTClientConfig(
            jwtToken=openmetadata_settings.jwt_token,
        ),
    )
    open_metadata: OpenMetadata = OpenMetadata(server_config)
    return OpenMetadataClientService(open_metadata, openmetadata_settings)


def get_provision_service(
    openmetadata_client_service: Annotated[
        OpenMetadataClientService, Depends(get_openmetadata_client_service)
    ],
) -> ProvisionService:
    return ProvisionService(openmetadata_client_service)


ProvisionServiceDep = Annotated[ProvisionService, Depends(get_provision_service)]


def get_glossary_terms_service(
    openmetadata_client_service: Annotated[
        OpenMetadataClientService, Depends(get_openmetadata_client_service)
    ],
) -> GlossaryTermsService:
    return GlossaryTermsService(openmetadata_client_service)


GlossaryTermsServiceDep = Annotated[
    GlossaryTermsService, Depends(get_glossary_terms_service)
]
