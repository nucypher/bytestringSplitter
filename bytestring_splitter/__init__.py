from collections import namedtuple

from bytestring_splitter.__about__ import __author__, __summary__, __title__, __version__

__all__ = ["__title__", "__summary__", "__version__", "__author__", ]

from contextlib import suppress

VARIABLE_HEADER_LENGTH = 4


class BytestringSplittingError(Exception):
    """
    Raised when the bytes don't go in the constructor.
    """


class BytestringSplitter(object):
    Message = namedtuple("Message", ("name", "message_class", "length", "kwargs"))
    processed_objects_container = list

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

    def __call__(self, splittable, return_remainder=False, msgpack_remainder=False):

        if not self.is_variable_length:
            if not (return_remainder or msgpack_remainder) and len(self) != len(splittable):
                raise BytestringSplittingError(
                    """Wrong number of bytes to constitute message types {} - 
                    need {}, got {} \n Did you mean to return the remainder?""".format(
                        self.message_types, len(self), len(splittable)))
            if len(self) is not -1 and len(self) > len(splittable):
                raise BytestringSplittingError(
                    """Not enough bytes to constitute
                    message types {} - need {}, got {}""".format(self.message_types,
                                                                 len(self),
                                                                 len(splittable)))
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
                error_message += "Unable to create a {} from {}, got: \n {}: {}".format(message_class, bytes_for_this_object, e, e.args)
                raise BytestringSplittingError(error_message)

            message_is_variable_length = isinstance(message, VariableLengthBytestring) or issubclass(message.__class__, VariableLengthBytestring)
            if message_is_variable_length:
                value = message.message_as_bytes
            else:
                value = message

            try:
                processed_objects[message_name] = value
            except TypeError:
                processed_objects.append(value)
            cursor = expected_end_of_object_bytes

        remainder = splittable[cursor:]

        if msgpack_remainder:
            try:
                import msgpack
            except ImportError:
                raise RuntimeError("You need to install msgpack to use msgpack_remainder.")
            processed_objects.append(msgpack.loads(remainder))
        elif return_remainder:
            processed_objects.append(remainder)

        return processed_objects

    def __len__(self):
        return self._length

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

        try:
            # If a message length has been passed manually, it will be the second item.
            # It might be the class object VariableLengthBytestring, which we use
            # as a flag that this is a variable-length message.
            message_length = message_type[1]
        except TypeError:
            try:
                # If this can be casted as an int, we assume that it's the intended length, in bytes.
                message_length = int(message_class)
                message_class = bytes
            except (ValueError, TypeError):
                # If not, we expect it to be an attribute on the first item.
                message_length = message_class.expected_bytes_length()
        except AttributeError:
            raise TypeError("""No way to know the expected length.  
                Either pass it as the second member of a tuple or 
                set _EXPECTED_LENGTH on the class you're passing.""")

        try:
            kwargs = message_type[2]
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

    def __init__(self, receiver=None, **kwargs):
        self.receiver = receiver
        BytestringSplitter.__init__(self, *kwargs.items())

    def __call__(self, splittable, receiver=None):
        receiver = receiver or self.receiver

        if receiver is None:
            raise TypeError(
                "Can't fabricate without a receiver.  You can either pass one when calling or pass one when init'ing.")

        results = BytestringSplitter.__call__(self, splittable, return_remainder=False, msgpack_remainder=False)
        return receiver(**results)

    @staticmethod
    def _parse_message_meta(message_item):
        message_name, message_type = message_item
        _, message_class, message_length, kwargs = BytestringSplitter._parse_message_meta(message_type)
        return BytestringSplitter.Message(message_name, message_class, message_length, kwargs)


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
            raise BytestringSplittingError("This does not appear to be a VariableLengthBytestring, or is not the correct length.")

        try:
            items = BytestringSplitter(VariableLengthBytestring).repeat(message)
        except BytestringSplittingError:
            items = (message,)

        return items
