from bytestring_splitter import BytestringSplitter, VariableLengthBytestring
import pytest
import msgpack


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


def test_arbitrary_object():

    class Thing:
        def __init__(self, bytes_representaiton):
            self.whatever = bytes_representaiton

    bytestring = b"This is a Thing.This is another Thing."
    splitter = BytestringSplitter((Thing, 16), (Thing, 22))
    thing, other_thing = splitter(bytestring)

    # The splitter made two Things...
    assert isinstance(thing, Thing)
    assert isinstance(other_thing, Thing)

    # ...and passed the bytes into their __init__ accordingly.
    assert thing.whatever == b"This is a Thing."
    assert other_thing.whatever == b"This is another Thing."


def test_too_many_of_bytes_raises_error():
    bytestring = b"This is 16 bytes"
    splitter_15 = BytestringSplitter(8, 7)
    with pytest.raises(ValueError):
        splitter_15(bytestring)


def test_get_remainder_as_bytes():
    bytestring = b"This is 16 bytesthis is an addendum"
    splitter = BytestringSplitter(16)
    message, addendum = splitter(bytestring, return_remainder=True)
    assert message == b"This is 16 bytes"
    assert addendum == b"this is an addendum"


def test_not_enough_bytes_still_raises_error():
    bytestring = b"This is 16 bytes"
    splitter_17 = BytestringSplitter(10, 7)
    with pytest.raises(ValueError):
        splitter_17(bytestring, return_remainder=True)


def test_append_msgpacked_dict_at_the_end():
    bytestring = b"This is 16 bytes"
    another_thing = {b"something": True}
    splitter = BytestringSplitter(16)
    message, appended_dict = splitter(bytestring + msgpack.dumps(another_thing), msgpack_remainder=True)
    assert another_thing == appended_dict
    assert message == bytestring


def test_add_splitters():
    bytestring = b"8 bytes."
    splitter_8 = BytestringSplitter(8)
    splitter_16 = splitter_8 + splitter_8
    result = splitter_16(bytestring + bytestring)
    assert result == [bytestring, bytestring]


def test_repeating_splitter():
    times_to_repeat = 50
    bytestring = b"peace at dawn"
    splitter = BytestringSplitter(13)
    results = splitter.repeat(bytestring * times_to_repeat)
    assert len(results) == times_to_repeat
    for result in results:
        assert result == bytestring


def test_variable_length():
    class ThingThatWillBeDifferentLengths:
        _EXPECTED_LENGTH = VariableLengthBytestring

        def __init__(self, thing_as_bytes):
            self.what_it_be = thing_as_bytes

    thing_as_bytes = VariableLengthBytestring(b"Sometimes, it's short.")
    another_thing_as_bytes = VariableLengthBytestring(b"Sometimes, it's really really really really long.")

    both_things = thing_as_bytes + another_thing_as_bytes
    splitter = BytestringSplitter(ThingThatWillBeDifferentLengths)
    first_thing, second_thing = splitter.repeat(both_things)
    assert thing_as_bytes == first_thing.what_it_be
    assert another_thing_as_bytes == second_thing.what_it_be

