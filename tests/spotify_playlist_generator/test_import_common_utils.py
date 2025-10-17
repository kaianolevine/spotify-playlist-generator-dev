from kaiano_common_utils import helpers


def test_import_common_utils():
    # Sanity check that the shared utils package is available
    assert hasattr(helpers, "try_lock_folder")
