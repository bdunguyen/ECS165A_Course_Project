from lstore.page import Page

def test_has_capacity():
    page = Page()
    value = 12345

    assert page.has_capacity(value) is True

def test_write():
    page = Page()
    value = 123

    result = page.write(value)
    assert result == True
    assert page.num_records == 1
    assert page.curr == 5

    # check data was written correctly
    expected = value.to_bytes(5, "big")
    assert page.data[0:5] == expected