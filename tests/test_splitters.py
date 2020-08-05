
import msgpack
import pytest

from bytestring_splitter import BytestringSplitter, VariableLengthBytestring, BytestringSplittingError, \
    BytestringKwargifier, VersionedBytestringKwargifier


def test_splitting_one_message():
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
    splitter = BytestringSplitter((str, 5, {"encoding": "utf-8"}),
                                  (str, 1, {"encoding": "utf-8"}),
                                  (str, 5, {"encoding": "utf-8"})
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


def test_arbitrary_object_as_single():
    class Thing:
        def __init__(self, bytes_representaiton):
            self.whatever = bytes_representaiton

    # We can make a collection with something in it, obviously.
    bytestring = b"This is a Thing."
    splitter = BytestringSplitter((Thing, 16))
    collection_with_thing = splitter(bytestring)

    assert isinstance(collection_with_thing[0], Thing)

    # But by passing single=True, we get the object alone.
    thing_alone = splitter(bytestring, single=True)

    assert isinstance(thing_alone, Thing)

    # But we can't do single with multiple objects.
    bytestring = b"This is a Thing.This is another Thing."
    splitter = BytestringSplitter((Thing, 16), (Thing, 22))

    with pytest.raises(ValueError):
        splitter(bytestring, single=True)


def test_too_many_of_bytes_raises_error():
    bytestring = b"This is 16 bytes"
    splitter_15 = BytestringSplitter(8, 7)
    with pytest.raises(BytestringSplittingError):
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
    with pytest.raises(BytestringSplittingError):
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


def test_multiply_splitters():
    bytestring = b"8 bytes." * 5
    splitter = BytestringSplitter(8)
    splitter_40 = splitter * 5
    result = splitter_40(bytestring)
    assert bytes().join(result) == bytestring


def test_repeating_splitter():
    times_to_repeat = 50
    bytestring = b"peace at dawn"
    splitter = BytestringSplitter(13)
    results = splitter.repeat(bytestring * times_to_repeat)
    assert len(results) == times_to_repeat
    for result in results:
        assert result == bytestring


class ThingThatWillBeDifferentLengths:
    expected_bytes_length = lambda: VariableLengthBytestring

    def __init__(self, thing_as_bytes):
        self.what_it_be = thing_as_bytes


def test_variable_length_in_first_positions():
    thing_as_bytes = VariableLengthBytestring(b"Sometimes, it's short.")
    another_thing_as_bytes = VariableLengthBytestring(b"Sometimes, it's really really really really long.")

    both_things = thing_as_bytes + another_thing_as_bytes
    splitter = BytestringSplitter(ThingThatWillBeDifferentLengths)
    first_thing, second_thing = splitter.repeat(both_things)
    assert thing_as_bytes == first_thing.what_it_be
    assert another_thing_as_bytes == second_thing.what_it_be


def test_variable_length_after_fixed_length():
    bytestring1 = b"This is a Thing."
    bytestring2 = b"This is another Thing."

    # One splitter, designed for the fixed length first and last messages,
    # But any variable length middle message.
    splitter = BytestringSplitter(16, VariableLengthBytestring, 22)

    variable1 = VariableLengthBytestring(b"short.")
    variable2 = VariableLengthBytestring(b"much much much much much longer.")

    # Same beginning and end, but with different middles.
    splittable1 = bytestring1 + variable1 + bytestring2
    splittable2 = bytestring1 + variable2 + bytestring2

    result1 = splitter(splittable1)
    result2 = splitter(splittable2)

    assert result1[0] == result2[0]  # The beginning is the same.
    assert result1[2] == result2[2]  # The end is the same.

    # And the two middles match their respective variable length bytestrings.
    assert result1[1] == variable1.message_as_bytes
    assert result2[1] == variable2.message_as_bytes


def test_passing_kwargs_along_with_bytes():
    """
    This time, we'll show splitting something that needs kwargs passed
    into its from_bytes constructor, and which raises RuntimeError
    if that thing isn't passed.
    """

    class ThingThatNeedsKwargs:
        """
        Here's the thing.
        """

        def __init__(self, thing_as_bytes):
            self.what_it_be = thing_as_bytes

        @classmethod
        def from_bytes(cls, thing_as_bytes, necessary_kwarg=False):
            if necessary_kwarg:
                return cls(thing_as_bytes)
            else:
                raise RuntimeError

    things_as_bytes = b"This is a thing that needs a kwarg.This is a thing that needs a kwarg."

    bad_spliter = BytestringSplitter((ThingThatNeedsKwargs, 35))
    bad_splitter_twice = bad_spliter * 2

    with pytest.raises(BytestringSplittingError):
        bad_splitter_twice(things_as_bytes)

    good_splitter = BytestringSplitter((ThingThatNeedsKwargs,
                                        35,
                                        {"necessary_kwarg": True})
                                       )
    good_splitter_twice = good_splitter * 2

    result = good_splitter_twice(things_as_bytes)

    assert result[0].what_it_be == things_as_bytes[:35]


def test_bundle_and_dispense_variable_length():
    items = [b'llamas', b'dingos', b'christmas-tree']
    vbytes = bytes(VariableLengthBytestring.bundle(items))
    items_again = VariableLengthBytestring.dispense(vbytes)
    assert items == items_again
