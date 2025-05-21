import urllib.parse

from loguru import logger

from src.models.api_models import (
    Info,
    ProvisioningStatus,
    Status1,
    SystemErr,
    ValidationError,
)
from src.models.data_product_descriptor import DataProduct, TagSourceTagLabel
from src.models.service_error import ServiceError
from src.services.openmetadata_client_service import OpenMetadataClientService


class ProvisionService:
    def __init__(
        self,
        openmetadata_client_service: OpenMetadataClientService,
    ):
        self.openmetadata_client_service = openmetadata_client_service

    def validate(self, data_product: DataProduct) -> None | ValidationError | SystemErr:
        try:
            logger.info("Starting validation for system {}", data_product.id)
            columns = [
                column
                for op in data_product.get_output_ports()
                for column in (op.dataContract.schema_ or [])
            ]
            tags = [tag for column in columns if column.tags for tag in column.tags]
            classification_tags = {
                t.tagFQN for t in tags if t.source == TagSourceTagLabel.CLASSIFICATION
            }
            glossary_terms = {
                t.tagFQN for t in tags if t.source == TagSourceTagLabel.GLOSSARY
            }
            existing_tags = {
                t.fullyQualifiedName
                for t in self.openmetadata_client_service.get_all_classification_tags()
                if t.fullyQualifiedName
            }
            existing_terms = {
                g.fullyQualifiedName.root
                for g in self.openmetadata_client_service.get_all_glossary_terms()
                if g.fullyQualifiedName
            }
            missing_tags = classification_tags - existing_tags
            missing_terms = glossary_terms - existing_terms
            if missing_tags or missing_terms:
                errors = []
                if missing_tags:
                    errors.append(
                        f"Missing classification tags {','.join(missing_tags)} in OpenMetadata"
                    )
                if missing_terms:
                    errors.append(
                        f"Missing glossary terms {','.join(missing_terms)} in OpenMetadata"
                    )
                logger.error(
                    "Validation failed for system {}: {}", data_product.id, errors
                )
                return ValidationError(errors=errors)
            logger.info("Validation successful for system {}", data_product.id)
            return None
        except ServiceError as se:
            return SystemErr(error=se.error_msg)

    def provision(self, data_product: DataProduct) -> ProvisioningStatus | SystemErr:
        try:
            logger.info("Starting provisioning for system {}", data_product.id)
            self.openmetadata_client_service.create_or_update_generic_storage_service()
            self.openmetadata_client_service.create_or_update_container_custom_attributes()
            self.openmetadata_client_service.create_or_update_domain(
                data_product.domain
            )
            self.openmetadata_client_service.create_or_update_dp(data_product)
            for op in data_product.get_output_ports():
                self.openmetadata_client_service.create_or_update_op(data_product, op)
            logger.info("Successfully provisioned system {}", data_product.id)
            return ProvisioningStatus(
                status=Status1.COMPLETED,
                result="",
                info=Info(
                    publicInfo=self._get_public_info(data_product),
                    privateInfo=dict(),
                ),
            )
        except ServiceError as se:
            return SystemErr(error=se.error_msg)

    def unprovision(
        self, data_product: DataProduct, remove_data: bool
    ) -> ProvisioningStatus | SystemErr:
        try:
            logger.info("Starting unprovisioning for system {}", data_product.id)
            for op in data_product.get_output_ports():
                self.openmetadata_client_service.delete_op(op)
            self.openmetadata_client_service.delete_dp(data_product)
            logger.info("Successfully unprovisioned system {}", data_product.id)
            return ProvisioningStatus(status=Status1.COMPLETED, result="")
        except ServiceError as se:
            return SystemErr(error=se.error_msg)

    def _get_public_info(self, data_product: DataProduct) -> dict:
        openmetadata_info = dict()
        openmetadata_info["system_url"] = {
            "type": "string",
            "label": "OpenMetadata URL",
            "value": "View in OpenMetadata",
            "href": f"{self.openmetadata_client_service.get_base_url()}dataProduct/{urllib.parse.quote(self.openmetadata_client_service._get_dp_name_from_id(data_product.id))}",  # noqa: E501
        }
        public_info = {data_product.id: openmetadata_info}
        return public_info
