import msgpack
import pytest

from bytestring_splitter import BytestringSplitter,  HeaderMetaDataMixinBase, VersioningMixin


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
