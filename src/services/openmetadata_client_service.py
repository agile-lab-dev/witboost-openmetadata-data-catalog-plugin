from urllib.parse import urlparse, urlunparse

from loguru import logger
from metadata.generated.schema.api.data.createContainer import CreateContainerRequest
from metadata.generated.schema.api.data.createCustomProperty import (
    CreateCustomPropertyRequest,
)
from metadata.generated.schema.api.domains.createDataProduct import (
    CreateDataProductRequest,
)
from metadata.generated.schema.api.domains.createDomain import CreateDomainRequest
from metadata.generated.schema.api.services.createStorageService import (
    CreateStorageServiceRequest,
)
from metadata.generated.schema.entity.classification.tag import Tag
from metadata.generated.schema.entity.data.container import (
    Container,
    ContainerDataModel,
)
from metadata.generated.schema.entity.data.glossaryTerm import GlossaryTerm
from metadata.generated.schema.entity.data.table import Column, ColumnName, DataType
from metadata.generated.schema.entity.domains.dataProduct import (
    DataProduct as OMDataProduct,
)
from metadata.generated.schema.entity.domains.domain import Domain, DomainType
from metadata.generated.schema.entity.services.storageService import (
    StorageService,
    StorageServiceType,
)
from metadata.generated.schema.type.basic import (
    EntityExtension,
    FullyQualifiedEntityName,
    Markdown,
)
from metadata.generated.schema.type.customProperty import PropertyType
from metadata.generated.schema.type.entityReference import EntityReference
from metadata.generated.schema.type.tagLabel import (
    LabelType,
    State,
    TagFQN,
    TagLabel,
    TagSource,
)
from metadata.ingestion.models.custom_properties import OMetaCustomProperties
from metadata.ingestion.ometa.ometa_api import OpenMetadata

from src.models.data_product_descriptor import (
    DataProduct,
    OpenMetadataColumn,
    OutputPort,
)
from src.models.service_error import ServiceError
from src.settings.openmetadata_settings import OpenMetadataSettings


class OpenMetadataClientServiceError(ServiceError):
    pass


class OpenMetadataClientService:
    def __init__(
        self,
        openmetadata_client: OpenMetadata,
        openmetadata_settings: OpenMetadataSettings,
    ):
        self.openmetadata_client = openmetadata_client
        self.openmetadata_settings = openmetadata_settings

    def create_or_update_container_custom_attributes(self) -> None:
        try:
            string_type: dict | None = self.openmetadata_client.client.get(
                "/metadata/types/name/string"
            )
            if not string_type:
                raise ValueError(
                    "Unable to retrieve the string type definition. Please contact the platform team."
                )
            for type_name, description in [
                ("kind", "Type of the entity."),
                (
                    "platform",
                    "Represents the vendor: Azure, GCP, AWS, CDP on AWS, etc. It is a free field, but it is useful to understand better the platform where the component will be running.",  # noqa: E501
                ),
                (
                    "technology",
                    "Represents which technology is used for the component.",
                ),
            ]:
                self.openmetadata_client.create_or_update_custom_property(
                    OMetaCustomProperties(  # type: ignore
                        entity_type=Container,
                        createCustomPropertyRequest=CreateCustomPropertyRequest(  # type: ignore
                            name=type_name,
                            description=Markdown(description),
                            propertyType=PropertyType(
                                EntityReference(  # type: ignore
                                    id=string_type["id"], type=string_type["name"]
                                )
                            ),
                        ),
                    )
                )
                logger.info(
                    "Upserted custom attribute {} for Container entity", type_name
                )
        except Exception as e:
            error_message = f"Failed to create or update container custom attributes. Details: {str(e)}"  # noqa: E501
            logger.exception(error_message)
            raise OpenMetadataClientServiceError(error_message)

    def create_or_update_generic_storage_service(self) -> StorageService:
        try:
            create_storage_service = CreateStorageServiceRequest(  # type: ignore
                name=self.openmetadata_settings.default_storage_service_name,
                serviceType=StorageServiceType[
                    self.openmetadata_settings.default_storage_service_type
                ],
            )
            created_or_updated_storage_service: StorageService = (
                self.openmetadata_client.create_or_update(data=create_storage_service)
            )
            logger.info(
                "Upserted storage service {}", created_or_updated_storage_service
            )
            return created_or_updated_storage_service
        except Exception as e:
            error_message = f"Failed to create or update storage service {self.openmetadata_settings.default_storage_service_name}. Details: {str(e)}"  # noqa: E501
            logger.exception(error_message)
            raise OpenMetadataClientServiceError(error_message)

    def create_or_update_domain(
        self,
        name: str,
    ) -> Domain:
        try:
            create_domain = CreateDomainRequest(  # type: ignore
                domainType=DomainType[self.openmetadata_settings.default_domain_type],
                name=name,
                description=Markdown(name),
            )
            domain: Domain = self.openmetadata_client.create_or_update(
                data=create_domain
            )
            logger.info("Upserted domain {}", domain)
            return domain
        except Exception as e:
            error_message = (
                f"Failed to create or update domain {name}. Details: {str(e)}"
            )
            logger.exception(error_message)
            raise OpenMetadataClientServiceError(error_message)

    def create_or_update_dp(self, dp: DataProduct) -> OMDataProduct:
        try:
            create_dp = CreateDataProductRequest(  # type: ignore
                name=self._get_dp_name_from_id(dp.id),
                displayName=dp.name,
                description=dp.description,
                domain=dp.domain,
            )
            created_or_updated_dp: OMDataProduct = (
                self.openmetadata_client.create_or_update(data=create_dp)
            )
            logger.info("Upserted DP {}", created_or_updated_dp)
            return created_or_updated_dp
        except Exception as e:
            error_message = f"Failed to create or update DP {dp.id}. Details: {str(e)}"
            logger.exception(error_message)
            raise OpenMetadataClientServiceError(error_message)

    def delete_dp(self, dp: DataProduct) -> None:
        try:
            existing_dp: OMDataProduct | None = self.openmetadata_client.get_by_name(
                OMDataProduct, self._get_dp_name_from_id(dp.id)
            )
            if existing_dp:
                self.openmetadata_client.delete(
                    OMDataProduct, existing_dp.id, False, True
                )
            logger.info("Deleted DP {}", dp.id)
            return None
        except Exception as e:
            error_message = f"Failed to delete DP {dp.id}. Details: {str(e)}"
            logger.exception(error_message)
            raise OpenMetadataClientServiceError(error_message)

    def create_or_update_op(self, dp: DataProduct, op: OutputPort) -> Container:
        try:
            create_op = CreateContainerRequest(  # type: ignore
                name=self._get_component_name_from_id(op.id),
                displayName=op.name,
                description=op.description,
                domain=dp.domain,
                dataProducts=[
                    FullyQualifiedEntityName(self._get_dp_name_from_id(dp.id))
                ],
                dataModel=ContainerDataModel(
                    isPartitioned=None, columns=self._to_om_column_list(op)
                ),
                service=FullyQualifiedEntityName(
                    self.openmetadata_settings.default_storage_service_name
                ),
                extension=EntityExtension(
                    {
                        "kind": op.kind,
                        "platform": op.platform or "",
                        "technology": op.technology or "",
                    }
                ),
            )
            created_or_updated_op: Container = (
                self.openmetadata_client.create_or_update(data=create_op)
            )
            logger.info("Upserted Output Port {}", created_or_updated_op)
            return created_or_updated_op
        except Exception as e:
            error_message = (
                f"Failed to create or update Output Port {op.id}. Details: {str(e)}"
            )
            logger.exception(error_message)
            raise OpenMetadataClientServiceError(error_message)

    def delete_op(self, op: OutputPort) -> None:
        try:
            existing_op: Container | None = self.openmetadata_client.get_by_name(
                Container,
                self.openmetadata_settings.default_storage_service_name
                + "."
                + self._get_component_name_from_id(op.id),
            )
            if existing_op:
                self.openmetadata_client.delete(Container, existing_op.id, False, True)
            logger.info("Deleted Output Port {}", op.id)
            return None
        except Exception as e:
            error_message = f"Failed to delete Output Port {op.id}. Details: {str(e)}"
            logger.exception(error_message)
            raise OpenMetadataClientServiceError(error_message)

    def get_all_classification_tags(self) -> list[Tag]:
        try:
            return list(self.openmetadata_client.list_all_entities(Tag))
        except Exception as e:
            error_message = f"Failed to retrieve classification tags. Details: {str(e)}"
            logger.exception(error_message)
            raise OpenMetadataClientServiceError(error_message)

    def get_classification_tag(self, fqn: str) -> Tag | None:
        try:
            existing_tag: Tag | None = self.openmetadata_client.get_by_name(
                Tag,
                fqn,
            )
            logger.info("Tag {} exists: {}", fqn, existing_tag is not None)
            return existing_tag
        except Exception as e:
            error_message = (
                f"Failed to retrieve classification tag {fqn}. Details: {str(e)}"
            )
            logger.exception(error_message)
            raise OpenMetadataClientServiceError(error_message)

    def get_all_glossary_terms(self) -> list[GlossaryTerm]:
        try:
            return list(self.openmetadata_client.list_all_entities(GlossaryTerm))
        except Exception as e:
            error_message = f"Failed to retrieve glossary terms. Details: {str(e)}"
            logger.exception(error_message)
            raise OpenMetadataClientServiceError(error_message)

    def get_glossary_term(self, fqn: str) -> GlossaryTerm | None:
        try:
            existing_term: GlossaryTerm | None = self.openmetadata_client.get_by_name(
                GlossaryTerm,
                fqn,
            )
            logger.info("Glossary term {} exists: {}", fqn, existing_term is not None)
            return existing_term
        except Exception as e:
            error_message = f"Failed to retrieve glossary term {fqn}. Details: {str(e)}"
            logger.exception(error_message)
            raise OpenMetadataClientServiceError(error_message)

    def _get_dp_name_from_id(self, id: str):
        # format id is: urn:dmb:cmp:healthcare:vaccinations:0:snowflake-output-port
        splitted = id.split(":")
        return f"{splitted[3]}:{splitted[4]}:{splitted[5]}"

    def _get_component_name_from_id(self, id: str):
        # format id is: urn:dmb:cmp:healthcare:vaccinations:0:snowflake-output-port
        splitted = id.split(":")
        return f"{splitted[3]}:{splitted[4]}:{splitted[5]}:{splitted[6]}"

    def _to_om_column_list(self, op: OutputPort) -> list[Column]:
        return (
            list(
                map(
                    lambda c: Column(  # type: ignore
                        name=ColumnName(c.name),
                        dataType=DataType[c.dataType],
                        description=Markdown(c.description or ""),
                        tags=self._to_om_tag_list(c),
                    ),
                    op.dataContract.schema_,
                )
            )
            if op.dataContract.schema_
            else []
        )

    def _to_om_tag_list(self, c: OpenMetadataColumn) -> list[TagLabel]:
        return (
            list(
                map(
                    lambda t: TagLabel(  # type: ignore
                        tagFQN=TagFQN(t.tagFQN),
                        labelType=LabelType[t.labelType],
                        source=TagSource[t.source],
                        state=State[t.state],
                    ),
                    c.tags,
                )
            )
            if c.tags
            else []
        )

    def get_base_url(self) -> str:
        host = self.openmetadata_client.config.hostPort
        parsed = urlparse(host)
        return urlunparse((parsed.scheme, parsed.netloc, "/", "", "", ""))
