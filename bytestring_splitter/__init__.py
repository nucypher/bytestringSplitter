from contextlib import suppress

VARIABLE_HEADER_LENGTH = 4


class BytestringSplitter(object):

    def __init__(self, *message_types_meta):
        """
        :param message_types:  A collection of types of messages to parse.
        """
        self._message_types_meta = message_types_meta

        self.is_variable_length = None
        self._length = None

        self.message_types = []
        self._populate_message_types()

        if not message_types_meta:
            raise ValueError(
                "Can't make a BytestringSplitter unless you specify what to split!")

        # A quick sanity check here to make sure the message_types don't have a common formatting issue.from
        # See, you're allowed to pass a simple class and int to make a splitter - that's more-or-less syntactic sugar.
        # You're not allowed to do things like BytestringSplitter((bytes, 3), 17) because what does that even mean?
        with suppress(IndexError):
            first_message_is_not_int_or_tuple = type(message_types_meta[0]) not in (int, tuple)
            second_message_is_int = isinstance(message_types_meta[1], int)

            if first_message_is_not_int_or_tuple and second_message_is_int:
                raise TypeError("You can't specify the length of the message as a direct argument to the constructor.  Instead, pass it as the second argument in a tuple (with the class as the first argument)")

    def __call__(self, splittable, return_remainder=False, msgpack_remainder=False):

        if not self.is_variable_length:
            if not any((return_remainder, msgpack_remainder)) and len(self) != len(splittable):
                raise ValueError(
                    """"Wrong number of bytes to constitute message types {} - 
                    need {}, got {} \n Did you mean to return the remainder?""".format(
                        self.message_types, len(self), len(splittable)))
            if len(self) is not -1 and len(self) > len(splittable):
                raise ValueError(
                    """Not enough bytes to constitute
                    message types {} - need {}, got {}""".format(self.message_types,
                                                                 len(self),
                                                                 len(splittable)))
        cursor = 0
        message_objects = []

        for message_type in self.message_types:
            message_class, message_length, kwargs = self._parse_message_meta(message_type)

            if message_length is VariableLengthBytestring:
                # If this message is of variable length, let's get the length
                # and advance the cursor past the bytes which represent the length.
                message_length_as_bytes = splittable[cursor:cursor+VARIABLE_HEADER_LENGTH]
                message_length = int.from_bytes(message_length_as_bytes, "big")
                cursor += VARIABLE_HEADER_LENGTH

            expected_end_of_object_bytes = cursor + message_length
            bytes_for_this_object = splittable[cursor:expected_end_of_object_bytes]
            try:
                message = message_class.from_bytes(bytes_for_this_object, **kwargs)
            except AttributeError:
                message = message_class(bytes_for_this_object, **kwargs)

            message_objects.append(message)
            cursor = expected_end_of_object_bytes

        remainder = splittable[cursor:]

        if msgpack_remainder:
            try:
                import msgpack
            except ImportError:
                raise RuntimeError("You need to install msgpack to use msgpack_remainder.")
            message_objects.append(msgpack.loads(remainder))
        elif return_remainder:
            message_objects.append(remainder)

        return message_objects

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
        for message_type in self._message_types_meta:
            message_class, message_length, kwargs = self._parse_message_meta(message_type)
            if message_length == VariableLengthBytestring:
                self.is_variable_length = True
            else:
                total_length += message_length
            self.message_types.append(self._parse_message_meta(message_type))

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
            if isinstance(message_class, int):
                # If this is an int, we assume that it's the intended length, in bytes.
                message_length = message_class
                message_class = bytes
            else:
                # If not, we expect it to be an attribute on the first item.
                message_length = message_class.expected_bytes_length()
        except AttributeError:
            raise TypeError("No way to know the expected length.  Either pass it as the second member of a tuple or set _EXPECTED_LENGTH on the class you're passing.")

        try:
            kwargs = message_type[2]
        except (IndexError, TypeError):
            kwargs = {}

        return message_class, message_length, kwargs

    def __add__(self, splitter):
        return self.__class__(*self.message_types + splitter.message_types)

    def __mul__(self, times_to_add):
        if not isinstance(times_to_add, int):
            raise TypeError("You only multiply a BytestringSplitter by an int.")

        new_splitter = self
        for i in range(1, times_to_add):
            new_splitter += self

        return new_splitter

    def repeat(self, splittable):
        """
        Continue to split the splittable until we get to the end.
        """
        remainder = True
        messages = []
        while remainder:
            *message, remainder = self(splittable, return_remainder=True)
            if len(message) == 1:
                message = message[0]
            messages.append(message)
            splittable = remainder
        return messages


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
                2 ** VARIABLE_HEADER_LENGTH -1,
                self.message_length))

    def __bytes__(self):
        return self.message_length_as_bytes + self.message_as_bytes

    def __add__(self, other):
        return bytes(self) + other

    def __radd__(self, other):
        return other + bytes(self)

    def __eq__(self, other):
        return self.message_as_bytes == bytes(other)
