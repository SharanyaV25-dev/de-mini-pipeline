from pipeline_utils import validation_error


def test_valid_order_has_no_validation_error() -> None:
    assert validation_error(123, "Laptop", 100) is None


def test_invalid_order_id_is_rejected() -> None:
    assert validation_error(99, "Laptop", 100) == "INVALID_ORDER_ID"


def test_missing_product_is_rejected() -> None:
    assert validation_error(123, "  ", 100) == "MISSING_PRODUCT"


def test_non_positive_amount_is_rejected() -> None:
    assert validation_error(123, "Laptop", 0) == "INVALID_AMOUNT"
