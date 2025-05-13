import unittest
from pathlib import Path
from unittest.mock import Mock

from src.dependencies import (
    unpack_provisioning_request,
    unpack_update_acl_request,
)
from src.models.api_models import (
    ProvisionInfo,
    ProvisioningRequest,
    UpdateAclRequest,
    ValidationError,
)
from src.models.data_product_descriptor import DataProduct


class TestUnpackUpdateAclRequest(unittest.IsolatedAsyncioTestCase):
    descriptor_str = Path(
        "tests/descriptors/descriptor_output_port_valid.yaml"
    ).read_text()
    update_acl_request = UpdateAclRequest(
        refs=["user:testuser", "bigData"],
        provisionInfo=ProvisionInfo(
            request=descriptor_str,  # noqa: E501
            result="result_prov",
        ),
    )

    async def test_successful_unpack(self):
        result = await unpack_update_acl_request(self.update_acl_request)
        self.assertIsInstance(result, tuple)
        self.assertEqual(len(result), 2)
        self.assertIsInstance(result[0], DataProduct)
        self.assertEqual(result[1], self.update_acl_request.refs)

    async def test_invalid_request(self):
        # Create a mock UpdateAclRequest instance with an invalid request
        update_acl_request = Mock()
        update_acl_request.provisionInfo.request = "Invalid JSON"

        # Call the function and assert the result
        result = await unpack_update_acl_request(update_acl_request)
        self.assertIsInstance(result, ValidationError)
        self.assertIn("Unable to parse the descriptor.", result.errors[0])

    async def test_exception_handling(self):
        update_acl_request = Mock()
        update_acl_request.provisionInfo.request = "{}"

        result = await unpack_update_acl_request(update_acl_request)
        self.assertIsInstance(result, ValidationError)


class TestUnpackProvisioningRequest(unittest.IsolatedAsyncioTestCase):
    descriptor_str = Path(
        "tests/descriptors/descriptor_output_port_valid.yaml"
    ).read_text()
    provisioning_request = ProvisioningRequest(
        descriptorKind="DATAPRODUCT_DESCRIPTOR",
        descriptor=descriptor_str,  # noqa: E501
    )

    invalid_provisioning_request = ProvisioningRequest(
        # dropped the 'name' field from the previous provisioning_request
        descriptorKind="DATAPRODUCT_DESCRIPTOR",
        descriptor=descriptor_str.replace(
            "name: Vaccinations", "invalid_field: Invalid Value"
        ),  # noqa: E501
    )

    async def test_successful_unpack(self):
        result = await unpack_provisioning_request(self.provisioning_request)
        self.assertIsInstance(result, DataProduct)

    async def test_invalid_request(self):
        result = await unpack_provisioning_request(self.invalid_provisioning_request)
        self.assertIsInstance(result, ValidationError)
        self.assertIn("Failed to parse the descriptor", result.errors[0])

    async def test_exception_handling(self):
        provisioning_request = Mock()
        provisioning_request.descriptorKind = "DATAPRODUCT_DESCRIPTOR"
        provisioning_request.descriptor = "Invalid JSON"

        result = await unpack_provisioning_request(provisioning_request)
        self.assertIsInstance(result, ValidationError)
