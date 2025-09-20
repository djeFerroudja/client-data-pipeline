def get_department(zipcode: str) -> str:
    """
    Return the French department code based on a given zipcode.

    Special handling for Corsica:
        - Zipcodes 20000 to 20199 → '2A' (Corse-du-Sud)
        - Zipcodes 20200 to 20999 → '2B' (Haute-Corse)

    For all other zipcodes, the department code is derived from the first two digits.

    Args:
        zipcode (str or int): The zipcode to convert.

    Returns:
        str: The corresponding department code.

    Raises:
        ValueError: If the zipcode is not a valid 5-digit number.

    """
    # Raise exception if value is empty (or None, which may be converted to string in some Python versions)
    if zipcode is None or zipcode == "":
        raise ValueError(f"Invalid zipcode: {zipcode}")

    # normalize to 5 digits
    z = str(zipcode).zfill(5)

    # check that it only contains digits
    if not z.isdigit():
        raise ValueError(f"Invalid zipcode format: {zipcode}")

    # handle Corsica
    if z.startswith("20"):
        dept = "2A" if int(z[2:]) <= 199 else "2B"
    else:
        dept = z[:2]
    return dept
