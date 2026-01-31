from lstore.page import Page

def test_has_capacity():
    page = Page()
    value = "hello"

    assert page.has_capacity(value) is True

def test_write():
    page = Page()
    value = "abc"

    result = page.write(value)
    assert result == True
    assert page.num_records == 1
    assert page.curr == len(value)

    # check data was written correctly
    assert page.data[0:3] == bytearray(b"abc")