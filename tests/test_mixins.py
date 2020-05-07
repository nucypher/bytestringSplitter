import msgpack
import pytest

from bytestring_splitter import BytestringSplitter, HeaderMetaDataMixinBase, VersioningMixin, StructureChecksumMixin, \
    VariableLengthBytestring


class AddsDeadBeefMixin(HeaderMetaDataMixinBase):

    METADATA_TAG = 'funny_bytes_pun'
    HEADER_LENGTH = 8

    funny_bytes_pun = b'deafbeef'

    @classmethod
    def _deserialize_metadata(cls, data_bytes):
        return data_bytes.decode('ascii')

    @classmethod
    def _serialize_metadata(cls, value):
        return value or cls.funny_bytes_pun


class AddsDeadBeefSplitter(AddsDeadBeefMixin, BytestringSplitter):
    pass


def test_that_it_addsdeafbeef():
    bytestring = b"hello world"
    with_metadata = AddsDeadBeefSplitter.assign_metadata(bytestring)
    assert with_metadata.startswith(b'deafbeef')


def test_deafbeefparsing():
    bytestring = b"deafbeefhello world"
    splitter = AddsDeadBeefSplitter(
        (str, 5, {"encoding": "utf-8"}),
        (str, 1, {"encoding": "utf-8"}),
        (str, 5, {"encoding": "utf-8"}))

    result = splitter(bytestring)
    assert result == ["hello", " ", "world"]
    assert splitter.funny_bytes_pun == b'deafbeef'
    assert splitter.get_metadata(bytestring)['funny_bytes_pun'] == 'deafbeef' # as utf-8


class AddsBadFoodMixin(HeaderMetaDataMixinBase):
    METADATA_TAG = 'bed_food_bytes'
    HEADER_LENGTH = 7

    @classmethod
    def _deserialize_metadata(cls, data_bytes):
        return data_bytes.decode('ascii')


class FeelingsMixin(HeaderMetaDataMixinBase):
    METADATA_TAG = 'current_feeling'
    HEADER_LENGTH = 8
    current_feeling = b'fee15bed'

    @classmethod
    def _deserialize_metadata(cls, data_bytes):
        return data_bytes.decode('ascii')


class AddsAllMannerOfHeadersSplitter(VersioningMixin, FeelingsMixin, AddsBadFoodMixin, AddsDeadBeefMixin, BytestringSplitter):
    pass


def test_mixin_chain():
    innocent_bytestring = b'i have no weird stuff in front of me.'

    prepended = AddsAllMannerOfHeadersSplitter.assign_metadata(
        innocent_bytestring,
        version=2,
        funny_bytes_pun=b'deafbeef',
        current_feeling=b'fee1deaf',
        bed_food_bytes=b'01dbeef'
    )
    assert prepended.startswith(b'\x00\x02fee1deaf01dbeefdeafbeef')

    # now lets read this back out.
    splitter = AddsAllMannerOfHeadersSplitter(
        (str, 34, {"encoding": "utf-8"}),
        (str, 3, {"encoding": "utf-8"}))

    result = splitter(prepended)
    assert result == ['i have no weird stuff in front of ', 'me.']

    metadata = splitter.get_metadata(prepended)

    assert metadata['version'] == 2
    assert metadata['funny_bytes_pun'] == 'deafbeef'
    assert metadata['bed_food_bytes'] == '01dbeef'
    assert metadata['current_feeling'] == 'fee1deaf'


def test_mixin_chain_with_kwargs():
    innocent_bytestring = b'i have no weird stuff in front of me.'

    prepended = AddsAllMannerOfHeadersSplitter.assign_metadata(
        innocent_bytestring,
        funny_bytes_pun=b'facefeed',
        bed_food_bytes=b'feedc0d',
        version=5
    )
    assert prepended.startswith(b'\x00\x05fee15bedfeedc0dfacefeed')

    # now lets read this back out.
    splitter = AddsAllMannerOfHeadersSplitter(
        (str, 34, {"encoding": "utf-8"}),
        (str, 3, {"encoding": "utf-8"}),
    )

    result = splitter(prepended)
    assert result == ['i have no weird stuff in front of ', 'me.']

    metadata = splitter.get_metadata(prepended)

    assert metadata['funny_bytes_pun'] == 'facefeed'
    assert metadata['bed_food_bytes'] == 'feedc0d'
    assert metadata['current_feeling'] == 'fee15bed'
    assert metadata['version'] == 5


class LocalOverridesSplitter(VersioningMixin, FeelingsMixin, AddsBadFoodMixin, AddsDeadBeefMixin, BytestringSplitter):
    version = 666
    funny_bytes_pun = b'beefdeaf'
    current_feeling = b'f331600d'
    bed_food_bytes = b'deafc0d'


def test_splitter_local_overrides():
    innocent_bytestring = b'i have no weird stuff in front of me.'
    prepended = LocalOverridesSplitter.assign_metadata(innocent_bytestring)
    assert prepended == b'\x02\x9a' +b'f331600d' + b'deafc0d' + b'beefdeaf' + b'i have no weird stuff in front of me.'

    splitter = LocalOverridesSplitter(
        (str, 34, {"encoding": "utf-8"}),
        (str, 3, {"encoding": "utf-8"}),
    )

    result = splitter(prepended)
    assert result == ['i have no weird stuff in front of ', 'me.']

    metadata = splitter.get_metadata(prepended)

    assert metadata['funny_bytes_pun'] == 'beefdeaf'
    assert metadata['bed_food_bytes'] == 'deafc0d'
    assert metadata['current_feeling'] == 'f331600d'
    assert metadata['version'] == 666


class ChecksumVerifyingSplitter(StructureChecksumMixin, BytestringSplitter):
    pass


def test_checksum_validation():

    three_v_three_splitter = ChecksumVerifyingSplitter(
        3,
        VariableLengthBytestring,
        3
    )

    five_v_six_splitter = ChecksumVerifyingSplitter(
        5,
        VariableLengthBytestring,
        6
    )

    threevthree = three_v_three_splitter.assign_metadata(
        b'bob' + VariableLengthBytestring(b' enjoys petting his ') + b'cat',
        checksum=three_v_three_splitter.generate_checksum()
    )
    fivevsix = three_v_three_splitter.assign_metadata(
        b'alice' + VariableLengthBytestring(b' recently adopted a ') + b'puppy',
        checksum=five_v_six_splitter.generate_checksum()
    )

    assert three_v_three_splitter.validate_checksum(threevthree)
    assert five_v_six_splitter.validate_checksum(fivevsix)

    assert three_v_three_splitter.validate_checksum(fivevsix) is False
    assert five_v_six_splitter.validate_checksum(threevthree) is False

    threevthree_result = three_v_three_splitter(threevthree)
    assert threevthree_result == [b'bob', b' enjoys petting his ', b'cat']

    fivevsix_result = five_v_six_splitter(fivevsix)
    assert fivevsix_result == [b'alice', b' recently adopted a ', b'puppy']


def test_same_signatures_validate():

    three_v_three_splitter = ChecksumVerifyingSplitter(
        3,
        VariableLengthBytestring,
        3
    )

    other_similar_splitter = ChecksumVerifyingSplitter(
        3,
        VariableLengthBytestring,
        3
    )

    assert three_v_three_splitter.generate_checksum() == other_similar_splitter.generate_checksum()
    assert three_v_three_splitter.validate_checksum(other_similar_splitter.generate_checksum())


def test_checksum_exception():

    attack_splitter = ChecksumVerifyingSplitter(
        3,
        1,
        3,
        1,
        5
    )

    revenge_splitter = ChecksumVerifyingSplitter(
        5,
        1,
        7,
        1,
        3
    )

    attack_bytes = attack_splitter.render(b'bob hit alice')
    revenge_bytes = revenge_splitter.render(b'alice smacked bob')

    assert attack_splitter.validate_checksum(revenge_bytes) is False
    with pytest.raises(ChecksumVerifyingSplitter.InvalidBytestringException):
        attack_splitter.validate_checksum(revenge_bytes, raise_exception=True)

    assert revenge_splitter.validate_checksum(attack_bytes) is False

    with pytest.raises(ChecksumVerifyingSplitter.InvalidBytestringException) as exception:
        revenge_splitter.validate_checksum(attack_bytes, raise_exception=True)
    assert str(exception.value).endswith('expected signature which is: bytes: (5), bytes: (1), bytes: (7), bytes: (1), bytes: (3)')

    # lets make sure they validate when given the correct bytes
    assert revenge_splitter.validate_checksum(revenge_bytes)
    assert attack_splitter.validate_checksum(attack_bytes)


def test_checksum_collision():
    # thx dnunez

    four_variables_splitter = ChecksumVerifyingSplitter(
        VariableLengthBytestring,
        VariableLengthBytestring,
        VariableLengthBytestring,
        VariableLengthBytestring,
    )

    hd_movie_splitter = ChecksumVerifyingSplitter(
        (bytes, 1987475062)
    )

    assert four_variables_splitter.generate_checksum() != hd_movie_splitter.generate_checksum()

def test_nested_metadata_mixin_splitters():
    # thx dnunez

    boring_normal_splitter = ChecksumVerifyingSplitter(
        1,
        2,
        3,
        4,
    )

    assert len(boring_normal_splitter) == 1 + 2 + 3 + 4

    byte_output_1 = boring_normal_splitter.render(
        b'1' + b'22' + b'333' + b'4444'
    )

    assert len(byte_output_1) == len(boring_normal_splitter) + boring_normal_splitter.HEADER_LENGTH

    byte_output_a = boring_normal_splitter.render(
        b'a' + b'bb' + b'ccc' + b'dddd'
    )

    # now lets nest two of that splitter in some other splitter
    nested_splitter = ChecksumVerifyingSplitter(boring_normal_splitter, (bytes, 1), boring_normal_splitter)

    # splitter lengths should only reflect the length of their payloads
    assert len(nested_splitter) == len(boring_normal_splitter) + len(boring_normal_splitter) + 1

    nested_bytes = nested_splitter.render(byte_output_1 + b'x' + byte_output_a)

    # the rendering of the outer splitter + the two inner splitters results in a total of 3 checksum headers,
    # being applied, which are 4 bytes each; total length should be 21 + 4 * 3
    assert len(nested_bytes) == len(boring_normal_splitter) + len(boring_normal_splitter) + 1 + ChecksumVerifyingSplitter.HEADER_LENGTH * 3

    # calling our nested_splitter should result in the original data coming back out.
    nested_data = nested_splitter(nested_bytes)
    assert nested_data == [[b'1', b'22', b'333', b'4444'], b'x', [b'a', b'bb', b'ccc', b'dddd']]

    # i guess this... just doesn't work
    with pytest.raises(ValueError):
        nested_splitter(nested_bytes, single=True)


@pytest.mark.skip()
def test_hash_function_speed():

    hd_movie_splitter = ChecksumVerifyingSplitter(
        (bytes, 1987475062)
    )

    for i in range(2000 * 5000):
        hd_movie_splitter.generate_checksum()

