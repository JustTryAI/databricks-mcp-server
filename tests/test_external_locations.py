import unittest
from unittest.mock import patch, AsyncMock
import pytest
import asyncio
import json

from src.api.external_locations import list_external_locations, mcp_list_external_locations

"""
MCP ERROR DIAGNOSIS:

Based on our tests, we can see that the error "object dict can't be used in 'await' expression"
occurs when trying to directly await a dictionary object. 

This suggests that in the MCP implementation for list_external_locations, the server is 
likely trying to await the params dictionary directly instead of passing it to the function.

Correct pattern:
    result = await list_external_locations(**params)

Incorrect pattern (causing the error):
    result = await params
    
To fix this, you would need to:
1. Check the MCP handler for list_external_locations
2. Ensure it's using the params to call the async function rather than awaiting the params directly
3. Compare with working endpoints like list_schemas to see the difference

The params dictionary needs to be unpacked and passed to the function, not awaited directly.
"""


class TestExternalLocations(unittest.TestCase):
    """Tests for the external_locations module."""

    @pytest.mark.asyncio
    async def test_list_external_locations(self):
        """Test listing external locations."""
        # Mock data for the API response
        mock_response = {
            "external_locations": [
                {
                    "name": "test_location",
                    "url": "abfss://container@account.dfs.core.windows.net/path",
                    "credential_name": "test_credential",
                    "read_only": True,
                    "comment": "Test location",
                    "owner": "owner@example.com",
                    "metastore_id": "metastore_id",
                    "credential_id": "credential_id",
                    "created_at": 1234567890,
                    "created_by": "creator@example.com",
                    "updated_at": 1234567890,
                    "updated_by": "updater@example.com"
                }
            ],
            "next_page_token": None
        }
        
        # Mock the API request function
        with patch("src.api.external_locations.make_api_request", new_callable=AsyncMock) as mock_api:
            mock_api.return_value = mock_response
            
            # Call the function with no parameters
            result = await list_external_locations()
            
            # Verify API was called with correct parameters
            mock_api.assert_called_once_with(
                "GET", 
                "/api/2.1/unity-catalog/external-locations", 
                params={}
            )
            
            # Verify the result
            self.assertEqual(result, mock_response)
            self.assertEqual(len(result["external_locations"]), 1)
            self.assertEqual(result["external_locations"][0]["name"], "test_location")
    
    @pytest.mark.asyncio
    async def test_list_external_locations_with_max_results(self):
        """Test listing external locations with max_results parameter."""
        # Mock data for the API response
        mock_response = {
            "external_locations": [],
            "next_page_token": None
        }
        
        # Mock the API request function
        with patch("src.api.external_locations.make_api_request", new_callable=AsyncMock) as mock_api:
            mock_api.return_value = mock_response
            
            # Call the function with max_results parameter
            result = await list_external_locations(max_results=100)
            
            # Verify API was called with correct parameters
            mock_api.assert_called_once_with(
                "GET", 
                "/api/2.1/unity-catalog/external-locations", 
                params={"max_results": 100}
            )
            
            # Verify the result
            self.assertEqual(result, mock_response)
    
    @pytest.mark.asyncio
    async def test_reproducing_mcp_error(self):
        """Simplified test to reproduce the MCP error."""
        # This is what happens when MCP tries to await a dictionary directly
        params = {}
        
        # This will fail with "object dict can't be used in 'await' expression"
        with self.assertRaises(TypeError) as context:
            await params
        
        # Check that we got the expected error message
        self.assertIn("object dict can't be used in 'await' expression", str(context.exception))
        
        # This is the correct way to use params
        with patch("src.api.external_locations.make_api_request", new_callable=AsyncMock) as mock_api:
            mock_api.return_value = {"result": "success"}
            result = await list_external_locations(**params)
            self.assertEqual(result, {"result": "success"})
    
    @pytest.mark.asyncio
    async def test_mcp_list_external_locations(self):
        """Test the MCP wrapper for list_external_locations."""
        # Mock data for the API response
        mock_response = {
            "external_locations": [
                {
                    "name": "test_location",
                    "url": "abfss://container@account.dfs.core.windows.net/path",
                    "credential_name": "test_credential"
                }
            ]
        }
        
        # Mock the API request function
        with patch("src.api.external_locations.make_api_request", new_callable=AsyncMock) as mock_api:
            mock_api.return_value = mock_response
            
            # Create a params dictionary similar to what MCP would provide
            params = {"max_results": 50}
            
            # Use the MCP wrapper function - this is what would be called by the MCP handler
            result = await mcp_list_external_locations(params)
            
            # Verify API was called with correct parameters
            mock_api.assert_called_once_with(
                "GET", 
                "/api/2.1/unity-catalog/external-locations", 
                params={"max_results": 50}
            )
            
            # Verify the result
            self.assertEqual(result, mock_response)
            
            # Reset the mock
            mock_api.reset_mock()
            
            # Test with empty params (edge case)
            empty_params = {}
            result = await mcp_list_external_locations(empty_params)
            
            # Verify API was called with empty params
            mock_api.assert_called_once_with(
                "GET", 
                "/api/2.1/unity-catalog/external-locations", 
                params={}
            )


if __name__ == "__main__":
    unittest.main() 