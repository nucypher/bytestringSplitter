from bytestring_splitter import BytestringSplitter


def test_splitting_single_message():
    """
    Strictly speaking, this isn't "splitting" yet - just showing
    that we get the original message back by splitting it into
    a single message of its entire length.
    """
    bytestring = b"hello world"
    splitter = BytestringSplitter(11)
    result = splitter(bytestring)
    assert result == [bytestring]


def test_splitting_hello_world():
    bytestring = b"hello world"
    splitter = BytestringSplitter(5, 1, 5)
    result = splitter(bytestring)
    assert result == [b'hello', b' ', b'world']


def test_split_bytestring_into_strs():
    bytestring = b"hello world"
    splitter = BytestringSplitter((str, 5, {"encoding":"utf-8"}),
                                  (str, 1, {"encoding":"utf-8"}),
                                  (str, 5, {"encoding":"utf-8"})
                                  )
    result = splitter(bytestring)
    assert result == ["hello", " ", "world"]
