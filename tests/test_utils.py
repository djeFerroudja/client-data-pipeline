import pytest

from country_age_analyses.scripts.utils import get_department


class TestUtils:

    @pytest.mark.parametrize(
        "zipcode, expected",
        [
            ("75001", "75"),  # standard Paris zipcode
            (75001, "75"),  # input provided as integer instead of string
            ("06000", "06"),  # standard Nice zipcode
            ("97400", "97"),  # overseas department (Réunion)
            ("20000", "2A"),  # Corse-du-Sud, lower bound of range
            ("20199", "2A"),  # Corse-du-Sud, upper bound of range
            ("20200", "2B"),  # Haute-Corse, lower bound of range
            ("20999", "2B"),  # Haute-Corse, upper bound of range
            ("21000", "21"),  # standard zipcode just after Corsica range
            ("00123", "00"),  # zipcode with leading zeros, tests padding
            ("0", "00"),
        ],
    )
    def test_get_department_valid(self, zipcode: str, expected: str) -> None:
        """
        Test that get_department returns the correct department code
        for a valid zipcode (string or integer).
        """
        # Act: call the function with the test input
        dept = get_department(zipcode)

        # Assert: check that the returned department code is correct
        assert dept == expected

    @pytest.mark.parametrize(
        "invalid_zipcode",
        [
            "aZ123",  # standard Paris zipcode
            None,  # input provided as integer instead of string
            "",
        ],
    )
    def test_get_department_invalid(self, invalid_zipcode: str) -> None:
        """
        Test that get_department raises a ValueError for invalid zipcodes.
        """

        # Act & Assert:
        with pytest.raises(ValueError):
            get_department(invalid_zipcode)


if __name__ == "__main__":
    pytest.main()
