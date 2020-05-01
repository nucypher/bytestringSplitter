from hypothesis import given, example
from hypothesis import strategies as st
from hypothesis.strategies import composite

from bytestring_splitter import BytestringSplitter


@given(st.binary(min_size=1))
@example(b'hello world')
def test_tiny_splitter_properties(bytestring):
    splitter = BytestringSplitter(len(bytestring))
    assert splitter(bytestring) == [bytestring]


@composite
def bytesting_and_trid_pattern(draw):

    # Hypothesis
    bytestring = draw(st.binary(min_size=3))
    near_the_end = len(bytestring)-1
    first_member_length = draw(st.integers(min_value=1, max_value=near_the_end))
    remainder = len(bytestring) - first_member_length
    second_member_length = draw(st.integers(min_value=1, max_value=remainder))

    # Setup Pattern
    header_pattern = (first_member_length, second_member_length)
    pattern = (*header_pattern, len(bytestring)-sum(header_pattern))

    return bytestring, pattern


@given(bytesting_and_trid_pattern())
@example((b'hello world', (5, 1, 5)))
def test_triad_splitter_properties(bytestring_and_pattern):

    bytestring, pattern = bytestring_and_pattern
    first_position, second_position = pattern[0], pattern[0]+pattern[1]

    # Splitter
    splitter = BytestringSplitter(*pattern)
    result = splitter(bytestring)

    # Length
    assert len(result) == len(pattern)
    assert sum(len(elem) for elem in result) == len(bytestring)

    # Content
    expected = [bytestring[0:first_position],
                bytestring[first_position:second_position],
                bytestring[second_position:len(bytestring)]]
    assert expected == result
