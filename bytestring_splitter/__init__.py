from collections import namedtuple
import zlib
from bytestring_splitter.__about__ import __author__, __summary__, __title__, __version__

__all__ = ["__title__", "__summary__", "__version__", "__author__", ]

from contextlib import suppress

VARIABLE_HEADER_LENGTH = 4


class BytestringSplittingError(Exception):
    """
    Raised when the bytes don't go in the constructor.
    """


class PartiallySplitBytes:
    """
    Represents a bytestring which has been split but not instantiated as processed objects yet.
    """

    def __init__(self, processed_objects):
        self.processed_objects = processed_objects

    def finish(self):
        assert False


class PartiallyKwargifiedBytes(PartiallySplitBytes):
    """
    Like PartiallySplitBytes, but finishing will also instantiate the final containing object, passing
    each processed message as a kwarg.
    """
    _receiver = None
    _additional_kwargs = None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._finished_values = {}

    def set_receiver(self, receiver):
        self._receiver = receiver

    def set_additional_kwargs(self, additional_kwargs):
        self._additional_kwargs = additional_kwargs

    def set_original_bytes_repr(self, bytes_representation):
        self._original_bytes = bytes_representation

    def finish(self):

        for message_name, (bytes_for_message, message_class, kwargs) in self.processed_objects.items():
            self._finished_values[message_name] = produce_value(message_class,
                                                                message_name,
                                                                bytes_for_message,
                                                                kwargs)
        return self._receiver(**self._finished_values, **self._additional_kwargs)

    def __getattr__(self, message_name):
        # First we'll try to see if this message_name already has a finished value:
        try:
            return self._finished_values[message_name]
        except KeyError:  # suppress might be good here, but it appears to have a performance penalty, and this is a performance-concerned function.
            pass

        try:
            bytes_for_message, message_class, kwargs = self.processed_objects[message_name]
            self._finished_values[message_name] = produce_value(message_class,
                                                                message_name,
                                                                bytes_for_message,
                                                                kwargs)
            produced_value = produce_value(message_class, message_name, bytes_for_message, kwargs)
            del self.processed_objects[message_name]  # We don't do this as a pop() in case produce_value raises.
            return produced_value
        except KeyError:
            raise AttributeError(
                f"{self.__class__} doesn't have a {message_name}, and it's not a partially split object either; those are {list(self.processed_objects.keys())}")

    def __bytes__(self):
        return self._original_bytes


def produce_value(message_class, message_name, bytes_for_this_object, kwargs):
    try:
        constructor = getattr(message_class, "from_bytes")
    except AttributeError:
        constructor = message_class

    try:
        message = constructor(bytes_for_this_object, **kwargs)
    except Exception as e:
        if message_name:
            error_message = "While constructing {}: ".format(message_name)
        else:
            error_message = ""
        error_message += "Unable to create a {} from {}, got: \n {}: {}".format(message_class,
                                                                                bytes_for_this_object, e, e.args)
        raise BytestringSplittingError(error_message)

    message_is_variable_length = isinstance(message, VariableLengthBytestring) or issubclass(message.__class__,
                                                                                             VariableLengthBytestring)
    if message_is_variable_length:
        value = message.message_as_bytes
    else:
        value = message
    return value


class BytestringSplitter:
    Message = namedtuple("Message", ("name", "message_class", "length", "kwargs"))
    processed_objects_container = list
    partial_receiver = PartiallySplitBytes

    def __init__(self, *message_parameters):
        """
        :param message_parameters:  A collection of parameters specifying how to parse a bytestring into discrete messages.
        """
        if not message_parameters:
            raise ValueError(
                "Can't make a BytestringSplitter unless you specify what to split!")

        self.message_parameters = message_parameters
        self.message_types = []

        self.is_variable_length = False
        self._length = None

        self._populate_message_types()

        # A quick sanity check here to make sure the message_types don't have a common formatting issue.
        # See, you're allowed to pass a simple class and int to make a splitter - that's more-or-less syntactic sugar.
        # You're not allowed to do things like BytestringSplitter((bytes, 3), 17) because what does that even mean?
        with suppress(IndexError):
            first_message_is_not_int_or_tuple = type(message_parameters[0]) not in (int, tuple)
            second_message_is_int = isinstance(message_parameters[1], int)

            if first_message_is_not_int_or_tuple and second_message_is_int:
                raise TypeError(
                    """You can't specify the length of the message as a direct argument to the constructor.
                    Instead, pass it as the second argument in a tuple (with the class as the first argument)""")

    def __call__(self, splittable: bytes,
                 return_remainder=False,
                 msgpack_remainder=False,
                 partial=False,
                 single=False):
        """
        :param splittable: the bytes to be split
        :param return_remainder: Whether to return any bytes left after splitting.
        :param msgpack_remainder: Whether to msgpack those bytes.
        :param partial: Whether to actually instantiate messages as their respective classes, or return the bytes with their classes as tuples.
        :param single: If this is True, assume that these bytes are a single object (rather than a collection) and return that list
            or raise an error if there is a remainder.
        :return: Either a collection of objects of the types specified in message_types or, if single, a single object.
        """
        processed_objects, remainder = self.actually_split(splittable, return_remainder, msgpack_remainder, partial,
                                                           single)
        processed_objects = self.deal_with_remainder(processed_objects, remainder, msgpack_remainder=msgpack_remainder,
                                                     return_remainder=return_remainder)

        if partial:
            return self.partial_receiver(processed_objects)
        return processed_objects

    def deal_with_remainder(self, processed_objects, remainder, msgpack_remainder=False, return_remainder=False):
        if msgpack_remainder:
            try:
                import msgpack
            except ImportError:
                raise RuntimeError("You need to install msgpack to use msgpack_remainder.")
            # TODO: Again, not very agnostic re: collection type here.
            processed_objects.append(msgpack.loads(remainder))
        elif return_remainder:
            processed_objects.append(remainder)
        return processed_objects

    def actually_split(self, splittable, return_remainder, msgpack_remainder, partial, single):
        if not self.is_variable_length:
            if not (return_remainder or msgpack_remainder) and len(self) != len(splittable):
                message = "Wrong number of bytes to constitute message types {} - need {}, got {} Did you mean to return the remainder?"
                raise BytestringSplittingError(message.format(self.nice_message_types(), len(self), len(splittable)))
            if len(self) is not -1 and len(self) > len(splittable):
                message = "Not enough bytes to constitute message types {} - need {}, got {}"
                raise BytestringSplittingError(message.format(self.nice_message_types(), len(self), len(splittable)))
        cursor = 0
        processed_objects = self.processed_objects_container()

        for message_type in self.message_types:
            message_name, message_class, message_length, kwargs = message_type

            if message_length is VariableLengthBytestring:
                # If this message is of variable length, let's get the length
                # and advance the cursor past the bytes which represent the length.
                message_length_as_bytes = splittable[cursor:cursor + VARIABLE_HEADER_LENGTH]
                message_length = int.from_bytes(message_length_as_bytes, "big")
                cursor += VARIABLE_HEADER_LENGTH

            if message_length > len(splittable):
                error_message = "Can't split a message with more bytes than the original splittable.  {} claimed a length of {}"
                if message_name:
                    error_message = error_message.format(message_name, message_length)
                else:
                    error_message = error_message.format(message_class, message_length)
                raise BytestringSplittingError(error_message)

            expected_end_of_object_bytes = cursor + message_length
            bytes_for_this_object = splittable[cursor:expected_end_of_object_bytes]

            if partial:
                try:  # TODO: Make this more agnostic toward the collection type.
                    processed_objects[message_name] = bytes_for_this_object, message_class, kwargs
                except TypeError:
                    processed_objects.append((bytes_for_this_object, message_class))
                cursor = expected_end_of_object_bytes
                continue

            # FINISHING
            value = produce_value(message_class, message_name, bytes_for_this_object, kwargs)
            ####################

            cursor = expected_end_of_object_bytes

            if single:
                _remainder = len(splittable[cursor:])
                if _remainder:
                    raise ValueError(
                        f"The bytes don't represent a single {message_class}; there are {_remainder} too many.")  # TODO
                return value, False
            else:
                try:  # TODO: Make this more agnostic toward the collection type.
                    processed_objects[message_name] = value
                except TypeError:
                    processed_objects.append(value)

        remainder = splittable[cursor:]

        return processed_objects, remainder

    def __len__(self):
        return self._length

    def expected_bytes_length(self):
        return len(self)

    def _populate_message_types(self):
        """
        Examine the message types meta passed during __init__.

        Set self._length as the sum of all fixed-length messsages.

        Set self.is_variable_length in the event that we find
        any variable-length message types.

        Parse message meta to get proper classes and lengths for all messages.
        """

        total_length = 0
        for message_type in self.message_parameters:
            message_name, message_class, message_length, kwargs = self._parse_message_meta(message_type)
            if message_length == VariableLengthBytestring:
                self.is_variable_length = True
            else:
                total_length += message_length
            if isinstance(message_class, BytestringSplitter):
                # If the message class is itself a splitter, we obviously only want a single from it.
                # (To use a collection instead, just add another message or use repeat().)
                kwargs['single'] = True
            self.message_types.append((message_name, message_class, message_length, kwargs))

        self._length = total_length

    @staticmethod
    def _parse_message_meta(message_type):
        """
        Takes the message type and determines the class of the message, the length
        (or that it's variable-length), and the kwargs to pass to its constructor.

        :param message_type: Either a class or a tuple of (class, length, kwargs)
        """
        try:
            message_class = message_type[0]
        except TypeError:
            message_class = message_type

        seeker = 1

        try:
            # If a message length has been passed manually, it will be the second item.
            # It might be the class object VariableLengthBytestring, which we use
            # as a flag that this is a variable-length message.
            message_length = message_type[seeker]
            if message_length == VariableLengthBytestring or int(message_type[seeker]):
                seeker += 1
            else:
                raise TypeError("Can't use this as a length.")  # This will move us into the except block below.
        except TypeError:
            try:
                # If this can be casted as an int, we assume that it's the intended length, in bytes.
                message_length = int(message_class)
                message_class = bytes
            except (ValueError, TypeError):
                # If not, we expect it to be a method on the first item.
                message_length = message_class.expected_bytes_length()
        except AttributeError:
            raise TypeError("""No way to know the expected length.
                Either pass it as the second member of a tuple or
                set _EXPECTED_LENGTH on the class you're passing.""")

        try:
            kwargs = message_type[seeker]
        except (IndexError, TypeError):
            kwargs = {}

        # Sanity check to make sure that this can be used to cast the message.
        if not hasattr(message_class, "__call__"):
            raise ValueError("{} can't be a message_class.".format(message_class))

        return BytestringSplitter.Message(None, message_class, message_length, kwargs)

    def __add__(self, splitter):
        return self.__class__(*self.message_parameters + splitter.message_parameters)

    def __mul__(self, times_to_add):
        if not isinstance(times_to_add, int):
            raise TypeError("You only multiply a BytestringSplitter by an int.")

        new_splitter = self
        for i in range(1, times_to_add):
            new_splitter += self

        return new_splitter

    def nice_message_types(self):
        return str().join("{}:{}, ".format(t[1].__name__, t[2]) for t in self.message_types)[:-2]

    def repeat(self, splittable, as_set=False):
        """
        Continue to split the splittable until we get to the end.

        If as_set, return values as a set rather than a list.
        """
        remainder = True
        if as_set:
            messages = set()
            collector = messages.add
        else:
            messages = []
            collector = messages.append
        while remainder:
            *message, remainder = self(splittable, return_remainder=True)
            if len(message) == 1:
                message = message[0]
            collector(message)
            splittable = remainder
        return messages


class BytestringKwargifier(BytestringSplitter):
    processed_objects_container = dict
    partial_receiver = PartiallyKwargifiedBytes

    def __init__(self, _receiver=None, _partial_receiver=None, _additional_kwargs=None, **parameter_pairs):
        self.receiver = _receiver
        if _partial_receiver is not None:
            self.partial_receiver = _partial_receiver
        self._additional_kwargs = _additional_kwargs or {}
        super().__init__(*parameter_pairs.items())

    def __call__(self, splittable, receiver=None, partial=False, return_remainder=False, *args, **kwargs):
        receiver = receiver or self.receiver

        if receiver is None:
            raise TypeError(
                "Can't kwargify without a receiver.  You can either pass one when calling or pass one when init'ing.")

        container = []

        while True:
            if return_remainder:
                result, remainder = BytestringSplitter.__call__(self, splittable, partial=partial,
                                                               return_remainder=return_remainder, *args, **kwargs)
            else:
                result = BytestringSplitter.__call__(self, splittable, partial=partial,
                                                               return_remainder=return_remainder, *args, **kwargs)
                remainder = None

            if partial:
                result.set_receiver(receiver)
                result.set_additional_kwargs(self._additional_kwargs)
                result.set_original_bytes_repr(splittable)
                container.append(result)
            else:
                container.append(receiver(**result, **self._additional_kwargs))
            if not remainder:
                break
            else:
                splittable = remainder

        if len(container) == 1:
            return container[0]
        else:
            return container

    @staticmethod
    def _parse_message_meta(message_item):
        message_name, message_type = message_item
        _, message_class, message_length, kwargs = BytestringSplitter._parse_message_meta(message_type)
        return BytestringSplitter.Message(message_name, message_class, message_length, kwargs)

    def deal_with_remainder(self, processed_objects, remainder, msgpack_remainder=False, return_remainder=False):
        if remainder:
            _processed_objects = [processed_objects]
            if msgpack_remainder:
                try:
                    import msgpack
                except ImportError:
                    raise RuntimeError("You need to install msgpack to use msgpack_remainder.")
                # TODO: Again, not very agnostic re: collection type here.
                _processed_objects.append(msgpack.loads(remainder))
            elif return_remainder:
                _processed_objects.append(remainder)
            else:
                raise BytestringSplittingError("Kwargifier sees a remainder, but return_remainder is False.")
            return _processed_objects
        elif return_remainder:
            return processed_objects, False
        else:
            return processed_objects

    def repeat(self, splittable, as_set=False):
        """
        Continue to split the splittable until we get to the end.

        If as_set, return values as a set rather than a list.
        """
        if as_set:
            raise BytestringSplittingError("Don't try to kwargify with as_set - we're not going to check if your objects are hashable.")
        messages = self(splittable, return_remainder=True)
        return messages


class HeaderMetaDataMixinBase:
    """
    A baseclass for mixins that work by serializing metadata about a bytestring by adding bytes to the
    start of said bytestring and deserialize that data at other times by removing those same bytes.
    """

    def __call__(self, splittable, *args, **kwargs):
        setattr(self, f'input_{self.METADATA_TAG}', kwargs.pop(self.METADATA_TAG, None))
        splittable = self.strip_metadata(splittable)
        splitter = super().__call__(splittable, *args, **kwargs)
        return splitter

    @classmethod
    def _get_ordered_mixins(cls, reversed=False):
        """
        returns mixins inheriting from HeaderMetaDataMixinBase in MRO order
        for the purpose of removing or adding bytes in the correct order

        This allows for metadata to be added and removed from the bytestring in the
        same order that the mixins are declared in the class definition.
        """

        mixins = [
            subclass for subclass in cls.__mro__ if
            issubclass(subclass, HeaderMetaDataMixinBase)
            and not issubclass(subclass, BytestringSplitter)
            and subclass is not HeaderMetaDataMixinBase
            and subclass is not cls
            ]

        # if an implementer creates a splitter by directly subclassing this baseclass and BytestringSplitter
        if not mixins and issubclass(cls, HeaderMetaDataMixinBase):
            mixins.append(cls)

        if reversed:
            return mixins[::-1]
        return mixins

    @classmethod
    def get_metadata(cls, some_bytes, **kwargs):
        """
        return a dictionary of the metadata from the beginning of the supplied bytestring
        where the keys are the HEADER_TAGs of any mixins in the MRO chain
        """

        data = {}
        for subclass in cls._get_ordered_mixins():
            data.update(subclass._get_metadata(some_bytes, **kwargs))
            some_bytes = subclass._strip_metadata(some_bytes)
        return data

    def render(self, some_bytes, **kwargs):
        """
        A shortcut which allows a BytestringSplitter instance to attempt to
        autogenerate all needed input for bytestring serialization if possible
        """
        for subclass in self._get_ordered_mixins():
            if not kwargs.get(subclass.METADATA_TAG):
                if hasattr(subclass, f'generate_{subclass.METADATA_TAG}'):
                    kwargs[subclass.METADATA_TAG] = getattr(subclass, f'generate_{subclass.METADATA_TAG}')(self, some_bytes)
        return self.assign_metadata(some_bytes, **kwargs)

    @classmethod
    def assign_metadata(cls, some_bytes, **kwargs):
        """
        prepends the metadata bytes to the supplied bytestring for all mixins in the chain
        """

        for subclass in cls._get_ordered_mixins(reversed=True):

            # if a splitter has class attributes that override
            # a mixin's TAG, we should pass them in in the kwargs here
            if hasattr(cls, subclass.METADATA_TAG) and not kwargs.get(subclass.METADATA_TAG):
                kwargs[subclass.METADATA_TAG] = getattr(cls, subclass.METADATA_TAG)

            some_bytes = subclass._assign_metadata(some_bytes, **kwargs)

        return some_bytes
        return cls._assign_metadata(some_bytes, **kwargs)

    @classmethod
    def strip_metadata(cls, some_bytes):
        """
        Slightly dirty... in a chain of mixins, this doesn't guarantee to remove it's _own_
        exact bytes of metadata, it only promises to remove the correct _number_ of bytes, which
        in conjunction with all its siblings, will result in full metadata strippage
        """
        for subcls in cls._get_ordered_mixins():
            some_bytes = subcls._strip_metadata(some_bytes)
        return some_bytes

    @classmethod
    def _assign_metadata(cls, some_bytes, **kwargs):
        """
        called by the baseclass for all subclasses for them to add their own metadata
        """
        data = kwargs.get(cls.METADATA_TAG, None) or\
            getattr(cls, f'_input_{cls.METADATA_TAG}', None) or\
            getattr(cls, cls.METADATA_TAG, None)

        if not data:
            raise ValueError(f"could not determine {cls.METADATA_TAG} to assign to output bytes and none was supplied")
        return cls._prepend_metadata(cls._serialize_metadata(data), some_bytes)

    @classmethod
    def _prepend_metadata(cls, data, some_bytes):
        return data + some_bytes

    @classmethod
    def _strip_metadata(self, some_bytes):
        return some_bytes[self.HEADER_LENGTH:]

    @classmethod
    def _get_metadata(cls, some_bytes, data=None):
        # gets the metadata off the top of the bytestring for this mixin
        data = data or {}
        data_bytes = some_bytes[:cls.HEADER_LENGTH]
        data[cls.METADATA_TAG] = cls._deserialize_metadata(data_bytes)

        return data

    def get_header_bytes(self, some_bytes):
        return self._get_metadata(some_bytes)[self.METADATA_TAG]

    @classmethod
    def _deserialize_metadata(cls, data_bytes):
        """
        will often be overridden to transform metadata as needed
        for a given type of metadata

        see VersioningMixin below
        """

        return data_bytes

    @classmethod
    def _serialize_metadata(cls, data_bytes):
        """
        will often be overridden to transform metadata as needed
        for a given type of metadata

        see VersioningMixin below
        """
        return data_bytes


class VersioningMixin(HeaderMetaDataMixinBase):

    HEADER_LENGTH = 2
    METADATA_TAG = 'version'

    @classmethod
    def _deserialize_metadata(cls, data_bytes):
        return int.from_bytes(data_bytes, 'big')

    @classmethod
    def _serialize_metadata(cls, value):
        return value.to_bytes(cls.HEADER_LENGTH, "big")

    @classmethod
    def assign_version(cls, some_bytes, version):
        #  a convenience method specific to VersioningMixin
        return cls.assign_metadata(some_bytes, version=version)


class StructureChecksumMixin(HeaderMetaDataMixinBase):

    HEADER_LENGTH = 4
    METADATA_TAG = 'checksum'
    HASH_FUNCTION = zlib.crc32

    class InvalidBytestringException(BaseException):
        pass

    def generate_checksum(self, *args, **kwargs):

        hash_input = b''.join([
            b'\xFF\xFF\xFF\xFF' if message_length is VariableLengthBytestring
            else message_length.to_bytes(VARIABLE_HEADER_LENGTH, "big")
            for message_name, message_class, message_length, kwargs in self.message_types
        ])
        return self.HASH_FUNCTION(hash_input).to_bytes(self.HEADER_LENGTH, "big")


    def validate_checksum(self, some_bytes, raise_exception=False):
        result = self.generate_checksum() == self.get_header_bytes(some_bytes)
        if result is False and raise_exception:
            expected = ', '.join([f'{message_class.__name__}: ({message_length})' for message_name, message_class, message_length, kwargs in self.message_types])
            raise StructureChecksumMixin.InvalidBytestringException(f"The contents of this bytestring could not be validated to match the expected signature which is: {expected}")
        return result


class VersionedBytestringSplitter(VersioningMixin, BytestringSplitter):
    pass

    def repeat(self, splittable, as_set=False):
        """
        Continue to split the splittable until we get to the end.

        If as_set, return values as a set rather than a list.
        """
        remainder = True
        if as_set:
            messages = set()
            collector = messages.add
        else:
            messages = []
            collector = messages.append
        while remainder:
            *message, remainder = self(splittable, return_remainder=True)
            if len(message) == 1:
                message = message[0]
            collector(message)
            splittable = remainder
        return messages


class VersionedBytestringKwargifier(VersionedBytestringSplitter, BytestringKwargifier):
    """
    A BytestringKwargifier which is versioned.
    """

    def __init__(self, *args, **kwargs):
        self._input_version = kwargs.pop('version')
        super().__init__(*args, **kwargs)


class VariableLengthBytestring:

    def __init__(self, message):
        if isinstance(message, int):
            raise TypeError("Don't pass an int here.  It won't do what you think it will do.")

        try:
            self.message_as_bytes = bytes(message)
        except TypeError:
            raise TypeError("You need to pass something that can be cast to bytes, not {}.".format(type(message)))

        self.message_length = len(self.message_as_bytes)

        try:
            self.message_length_as_bytes = self.message_length.to_bytes(VARIABLE_HEADER_LENGTH, "big")
        except OverflowError:
            raise ValueError("Your message is too long.  The max length is {} bytes; yours is {}".format(
                256 ** VARIABLE_HEADER_LENGTH - 1,
                self.message_length))

    def __bytes__(self):
        return self.message_length_as_bytes + self.message_as_bytes

    def __add__(self, other):
        return bytes(self) + other

    def __radd__(self, other):
        return other + bytes(self)

    def __eq__(self, other):
        return self.message_as_bytes == bytes(other)

    @classmethod
    def expected_bytes_length(cls):
        return cls

    @classmethod
    def bundle(cls, collection):
        """
        Casts each item in collection to bytes, makes a VariableLengthBytestring from each.
        Then, casts the result to bytes and makes a VariableLengthBytestring from it.

        Useful for semantically packing collections of individually useful objects without needing boilerplate all over the place.
        """
        vbytes = (VariableLengthBytestring(i) for i in collection)
        concatenated_bytes = bytes().join(bytes(d) for d in vbytes)
        vbytes_joined = VariableLengthBytestring(concatenated_bytes)
        return vbytes_joined

    @staticmethod
    def dispense(bytestring):
        """
        Takes a bytestring representation of a VariableLengthBytestring, confirms that it is the correct length,
        and returns the original bytes.
        """
        message_length_as_bytes = bytestring[:VARIABLE_HEADER_LENGTH]
        message_length = int.from_bytes(message_length_as_bytes, "big")
        message = bytestring[VARIABLE_HEADER_LENGTH:]
        if not message_length == len(message):
            raise BytestringSplittingError(
                "This does not appear to be a VariableLengthBytestring, or is not the correct length.")

        try:
            items = BytestringSplitter(VariableLengthBytestring).repeat(message)
        except BytestringSplittingError:
            items = (message,)

        return items
