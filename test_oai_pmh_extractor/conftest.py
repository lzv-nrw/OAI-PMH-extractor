import requests
import pytest

@pytest.fixture(name="generate_FakeRequestsResponse")
def return_generate_FakeRequestsResponse():
    "Helper function wrapped into a pytest-fixture."

    def generate_FakeRequestsResponse(expected_response, raise_for_status_info=None):
        """
        Helper for faking server behavior.

        Returns fake of a requests.Response-object.

        Keyword arguments:
        extected_response -- request's response content
        raise_for_status_info -- tuple of status_code and error_message for
                                faking HTTPErrors
        """
        class FakeRequestsResponse():
            content = expected_response
            status_code = 200 if raise_for_status_info is None else raise_for_status_info[0]
            def raise_for_status(self):
                if raise_for_status_info is not None:
                    raise requests.HTTPError(
                        str(self.status_code) + ": " + raise_for_status_info[1],
                        response=self
                    )
        return FakeRequestsResponse()
    return generate_FakeRequestsResponse
