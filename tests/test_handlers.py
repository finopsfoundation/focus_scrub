"""Tests for column handlers."""

from __future__ import annotations

import pandas as pd

from focus_scrub.handlers import (
    AccountIdHandler,
    CommitmentDiscountIdHandler,
    ResourceIdHandler,
    StellarNameHandler,
)
from focus_scrub.mapping import MappingCollector, MappingEngine


class TestAccountIdHandler:
    """Test the AccountIdHandler."""

    def test_pure_numeric_id(self, mapping_engine: MappingEngine) -> None:
        """Test scrubbing pure numeric account IDs."""
        handler = AccountIdHandler(mapping_engine=mapping_engine)

        account_id = "000011112222"
        result1 = handler.scrub(account_id)
        result2 = handler.scrub(account_id)

        assert result1 == result2
        assert len(result1) == len(account_id)
        assert result1.isdigit()
        assert result1 != account_id

    def test_arn_with_account_id(self, mapping_engine: MappingEngine) -> None:
        """Test scrubbing ARN with embedded account ID."""
        handler = AccountIdHandler(mapping_engine=mapping_engine)

        arn = "arn:aws:ec2:us-east-1:000011112222:reserved-instances/00000000-1111-2222-3333-444444444444"
        result = handler.scrub(arn)

        # Should still be an ARN
        assert result.startswith("arn:aws:ec2:us-east-1:")
        assert ":reserved-instances/" in result

        # Account ID should be mapped
        assert "000011112222" not in result

        # UUID should be mapped
        assert "00000000-1111-2222-3333-444444444444" not in result

    def test_arn_consistency_across_columns(self, mapping_engine: MappingEngine) -> None:
        """Test that account ID in ARN maps consistently with standalone account ID."""
        handler = AccountIdHandler(mapping_engine=mapping_engine)

        account_id = "333344445555"
        arn = f"arn:aws:ec2:us-east-1:{account_id}:reserved-instances/00000000-1111-2222-3333-444444444444"

        # Map standalone account ID first
        mapped_account = handler.scrub(account_id)

        # Map ARN
        mapped_arn = handler.scrub(arn)

        # Account ID in ARN should match standalone mapping
        assert mapped_account in mapped_arn

    def test_na_values_passed_through(self, mapping_engine: MappingEngine) -> None:
        """Test that NA values are passed through unchanged."""
        handler = AccountIdHandler(mapping_engine=mapping_engine)

        result = handler.scrub(pd.NA)
        assert pd.isna(result)

        result = handler.scrub(None)
        assert pd.isna(result)

    def test_collector_records_mappings(self, mapping_engine: MappingEngine) -> None:
        """Test that handler records mappings to collector."""
        handler = AccountIdHandler(mapping_engine=mapping_engine)
        collector = MappingCollector()
        handler.attach_collector("TestColumn", collector)

        original = "777788889999"
        result = handler.scrub(original)

        mappings = collector.to_dict()
        assert "TestColumn" in mappings
        assert len(mappings["TestColumn"]) == 1
        assert mappings["TestColumn"][0] == (original, result)


class TestStellarNameHandler:
    """Test the StellarNameHandler."""

    def test_name_mapping_consistency(self, mapping_engine: MappingEngine) -> None:
        """Test that names map consistently."""
        handler = StellarNameHandler(mapping_engine=mapping_engine)

        name = "Example Corporation"
        result1 = handler.scrub(name)
        result2 = handler.scrub(name)

        assert result1 == result2
        assert result1 != name

    def test_stellar_name_format(self, mapping_engine: MappingEngine) -> None:
        """Test that generated names follow stellar format."""
        handler = StellarNameHandler(mapping_engine=mapping_engine)

        name = "Test Company"
        result = handler.scrub(name)

        # Should have format "Term Letter" or "Term Letter N"
        parts = result.split()
        assert len(parts) >= 2

    def test_unique_names(self, mapping_engine: MappingEngine) -> None:
        """Test that different input names get different stellar names."""
        handler = StellarNameHandler(mapping_engine=mapping_engine)

        names = ["Company A", "Company B", "Company C"]
        results = [handler.scrub(name) for name in names]

        # All results should be unique
        assert len(results) == len(set(results))


class TestCommitmentDiscountIdHandler:
    """Test the CommitmentDiscountIdHandler."""

    def test_delegates_to_account_id_handler(self, mapping_engine: MappingEngine) -> None:
        """Test that CommitmentDiscountIdHandler delegates to AccountIdHandler."""
        handler = CommitmentDiscountIdHandler(mapping_engine=mapping_engine)

        account_id = "666677778888"
        result = handler.scrub(account_id)

        assert result != account_id
        assert len(result) == len(account_id)
        assert result.isdigit()

    def test_shares_mapping_engine(self, mapping_engine: MappingEngine) -> None:
        """Test that CommitmentDiscountIdHandler shares mappings with AccountIdHandler."""
        account_handler = AccountIdHandler(mapping_engine=mapping_engine)
        commitment_handler = CommitmentDiscountIdHandler(mapping_engine=mapping_engine)

        account_id = "333344445555"

        # Map via account handler
        account_result = account_handler.scrub(account_id)

        # Map via commitment handler - should get same result
        commitment_result = commitment_handler.scrub(account_id)

        assert account_result == commitment_result


class TestResourceIdHandler:
    """Test the ResourceIdHandler."""

    def test_aws_arn_scrubbing(self, mapping_engine: MappingEngine) -> None:
        """Test scrubbing AWS ARNs with account IDs and resource names."""
        handler = ResourceIdHandler(mapping_engine=mapping_engine)

        # Test ARN with account ID and queue name
        arn = "arn:aws:sqs:us-west-2:123456789012:MyTestQueue"
        result = handler.scrub(arn)

        # Structure should be preserved
        assert result.startswith("arn:aws:sqs:us-west-2:")
        # Account ID should be changed
        assert "123456789012" not in result
        # Queue name should be scrambled
        assert "MyTestQueue" not in result
        # Should still have a queue name part
        parts = result.split(":")
        assert len(parts) == 6

    def test_aws_arn_with_resource_type(self, mapping_engine: MappingEngine) -> None:
        """Test ARN with resource type preservation."""
        handler = ResourceIdHandler(mapping_engine=mapping_engine)

        # Test ARN with loadbalancer resource type
        arn = "arn:aws:elasticloadbalancing:us-east-1:123456789012:loadbalancer/net/my-lb/1234567890abcdef"
        result = handler.scrub(arn)

        # Resource type "loadbalancer" and scheme "net" should be preserved
        assert "loadbalancer" in result
        assert "/net/" in result
        # Load balancer name should be scrambled
        assert "my-lb" not in result

    def test_aws_arn_with_uuid(self, mapping_engine: MappingEngine) -> None:
        """Test ARN with embedded UUID."""
        handler = ResourceIdHandler(mapping_engine=mapping_engine)

        arn = "arn:aws:logs:us-west-2:123456789012:log-group:/test/abc12345-6789-abcd-ef01-234567890abc"
        result = handler.scrub(arn)

        # Account ID should be changed
        assert "123456789012" not in result
        # UUID should be replaced consistently
        assert "abc12345-6789-abcd-ef01-234567890abc" not in result
        # Structure should be maintained (colon count)
        assert result.count(":") == arn.count(":")
        # Should preserve slash structure in resource path
        assert "/" in result

    def test_azure_resource_id_scrubbing(self, mapping_engine: MappingEngine) -> None:
        """Test scrubbing Azure Resource IDs."""
        handler = ResourceIdHandler(mapping_engine=mapping_engine)

        resource_id = "/subscriptions/abc12345-6789-abcd-ef01-234567890abc/resourcegroups/my-rg/providers/microsoft.compute/virtualmachines/test-vm"
        result = handler.scrub(resource_id)

        # Structure keywords should be preserved
        assert "/subscriptions/" in result
        assert "/resourcegroups/" in result
        assert "/providers/" in result
        assert "microsoft.compute" in result
        # Subscription UUID should be changed
        assert "abc12345-6789-abcd-ef01-234567890abc" not in result
        # Resource group and VM name should be scrambled
        assert "my-rg" not in result
        assert "test-vm" not in result

    def test_azure_resource_id_with_multiple_segments(self, mapping_engine: MappingEngine) -> None:
        """Test Azure Resource ID with multiple path segments."""
        handler = ResourceIdHandler(mapping_engine=mapping_engine)

        resource_id = "/subscriptions/abc12345-6789-abcd-ef01-234567890abc/resourcegroups/test-group/providers/microsoft.storage/storageaccounts/teststorage123"
        result = handler.scrub(resource_id)

        # Provider namespace preserved
        assert "microsoft.storage" in result
        # Names scrambled
        assert "test-group" not in result
        assert "teststorage123" not in result
        # Structure maintained
        assert result.count("/") == resource_id.count("/")

    def test_oci_ocid_scrubbing(self, mapping_engine: MappingEngine) -> None:
        """Test scrubbing OCI OCIDs."""
        handler = ResourceIdHandler(mapping_engine=mapping_engine)

        ocid = "ocid1.instance.oc1.us-phoenix-1.abcdefghijklmnopqrstuvwxyz1234567890abcdefghijklmnopqrst"
        result = handler.scrub(ocid)

        # OCID structure should be preserved
        assert result.startswith("ocid1.")
        assert ".oc1.us-phoenix-1." in result
        # Resource type should be scrambled
        assert "instance" not in result
        # Unique ID should be scrambled
        assert "abcdefghijklmnopqrstuvwxyz1234567890abcdefghijklmnopqrst" not in result

    def test_oci_service_name_scrubbing(self, mapping_engine: MappingEngine) -> None:
        """Test scrubbing OCI service names."""
        handler = ResourceIdHandler(mapping_engine=mapping_engine)

        service_name = "oci_testservice"
        result = handler.scrub(service_name)

        # Should preserve oci_ prefix
        assert result.startswith("oci_")
        # Service name should be scrambled
        assert "testservice" not in result

    def test_aws_instance_id_scrubbing(self, mapping_engine: MappingEngine) -> None:
        """Test scrubbing AWS instance IDs."""
        handler = ResourceIdHandler(mapping_engine=mapping_engine)

        instance_id = "i-0123456789abcdef0"
        result = handler.scrub(instance_id)

        # Prefix should be preserved
        assert result.startswith("i-")
        # ID should be scrambled
        assert "0123456789abcdef0" not in result
        # Length should match
        assert len(result) == len(instance_id)

    def test_consistency_across_scrubs(self, mapping_engine: MappingEngine) -> None:
        """Test that same resource ID scrubs consistently."""
        handler = ResourceIdHandler(mapping_engine=mapping_engine)

        resource_ids = [
            "arn:aws:s3:::my-test-bucket",
            "/subscriptions/abc12345-6789-abcd-ef01-234567890abc/resourcegroups/test/providers/microsoft.compute/disks/disk1",
            "ocid1.volume.oc1.us-phoenix-1.abcdefgh1234567890",
            "i-1234567890abcdef0",
        ]

        for resource_id in resource_ids:
            result1 = handler.scrub(resource_id)
            result2 = handler.scrub(resource_id)
            assert result1 == result2, f"Inconsistent scrubbing for {resource_id}"

    def test_account_id_consistency_in_arn(self, mapping_engine: MappingEngine) -> None:
        """Test that account IDs in ARNs map consistently."""
        handler = ResourceIdHandler(mapping_engine=mapping_engine)

        account_id = "999988887777"
        arn1 = f"arn:aws:ec2:us-east-1:{account_id}:instance/i-1234567890abcdef"
        arn2 = f"arn:aws:s3:us-west-2:{account_id}:bucket/my-bucket"

        result1 = handler.scrub(arn1)
        result2 = handler.scrub(arn2)

        # Extract account ID from both results
        account_in_result1 = result1.split(":")[4]
        account_in_result2 = result2.split(":")[4]

        # Should map to same account ID
        assert account_in_result1 == account_in_result2

    def test_na_values_passed_through(self, mapping_engine: MappingEngine) -> None:
        """Test that NA values are passed through unchanged."""
        handler = ResourceIdHandler(mapping_engine=mapping_engine)

        assert pd.isna(handler.scrub(pd.NA))
        assert pd.isna(handler.scrub(None))
        assert handler.scrub("") == ""

    def test_collector_records_mappings(
        self, mapping_engine: MappingEngine, mapping_collector: MappingCollector
    ) -> None:
        """Test that handler records mappings in collector."""
        handler = ResourceIdHandler(mapping_engine=mapping_engine)
        handler.attach_collector("ResourceId", mapping_collector)

        original = "arn:aws:s3:::my-test-bucket-name"
        result = handler.scrub(original)

        mappings = mapping_collector.to_dict()
        assert "ResourceId" in mappings
        assert original in dict(mappings["ResourceId"])
        assert dict(mappings["ResourceId"])[original] == result
