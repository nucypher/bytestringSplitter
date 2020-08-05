import pytest

from bytestring_splitter import BytestringSplitter, VariableLengthBytestring, BytestringKwargifier, \
    BytestringSplittingError, VersionedBytestringSplitter, VersionedBytestringKwargifier, HeaderMetaDataMixinBase, \
    VersioningMixin


class DeliciousCoffee():
    def __init__(self, blend, milk_type, size):
        self.blend = blend
        self.milk_type = milk_type
        self.size = size

    def sip(self):
        return "Mmmm"


coffee_splitter = BytestringKwargifier(
    DeliciousCoffee,
    blend=VariableLengthBytestring,
    milk_type=(bytes, 13),
    size=(int, 2, {"byteorder": "big"})
)


def test_kwargified_coffee():
    coffee_as_bytes = VariableLengthBytestring(b"Equal Exchange Mind, Body, and Soul") + b"local_oatmilk" + int(54453).to_bytes(2, byteorder="big")

    cup_of_coffee = coffee_splitter(coffee_as_bytes)
    assert cup_of_coffee.blend == b"Equal Exchange Mind, Body, and Soul"
    assert cup_of_coffee.milk_type == b"local_oatmilk"
    assert cup_of_coffee.size == 54453


def test_kwargified_several_cups():
    first_cup = VariableLengthBytestring(b"Equal Exchange Mind, Body, and Soul") + b"local_oatmilk" + int(
        54453).to_bytes(2, byteorder="big")
    second_cup = VariableLengthBytestring(b"Sandino Roasters Blend") + b"half_and_half" + int(16).to_bytes(2,
                                                                                                           byteorder="big")
    cups_as_bytes = first_cup + second_cup
    two_cups = coffee_splitter.repeat(cups_as_bytes)

    assert two_cups[0].blend == b"Equal Exchange Mind, Body, and Soul"
    assert two_cups[1].blend == b"Sandino Roasters Blend"


def test_partial_instantiation():
    coffee_as_bytes = VariableLengthBytestring(b"Sandino Roasters Blend") + b"half_and_half" + int(16).to_bytes(2, byteorder="big")

    brewing_coffee = coffee_splitter(coffee_as_bytes, partial=True)

    with pytest.raises(AttributeError):
        brewing_coffee.sip()

    cup_of_coffee = brewing_coffee.finish()
    assert cup_of_coffee.sip() == "Mmmm"


def test_just_in_time_attribute_resolution():
    coffee_as_bytes = VariableLengthBytestring(b"Democracy Coffee") + b"half_and_half" + int(16).to_bytes(2, byteorder="big")

    brewing_coffee = coffee_splitter(coffee_as_bytes, partial=True)
    assert brewing_coffee._finished_values == {}

    blend = brewing_coffee.blend
    assert blend == b"Democracy Coffee"

    assert brewing_coffee._finished_values == {'blend': b'Democracy Coffee'}

    # Still can't sip, though.
    with pytest.raises(AttributeError):
        brewing_coffee.sip()

    # Again.  This time, we'll get the cached value (though the experience to the user is the same).
    blend = brewing_coffee.blend
    assert blend == b"Democracy Coffee"

    cup_of_coffee = brewing_coffee.finish()
    assert cup_of_coffee.sip() == "Mmmm"


class CaffeinatedBeverage:

    def __bytes__(self):
        mybytes = b''
        for arg in self.args:
            mybytes += getattr(self, arg)
        return BeverageFactory.add_version(self, mybytes)


class OldFashionedCoffee(CaffeinatedBeverage, DeliciousCoffee):
    version = 1
    args = ['blend', 'milk_type', 'size']
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.blend = VariableLengthBytestring(self.blend)


class EnergyDrink(CaffeinatedBeverage):
    version = 2
    args = ['name', 'warning_label', 'active_ingredient', 'size']

    def __init__(self, name, warning_label, active_ingredient, size):
        self.name = VariableLengthBytestring(name)
        self.warning_label = VariableLengthBytestring(warning_label)
        self.active_ingredient = active_ingredient
        self.size = size


class BeverageFactory:
    splitters = [
        VersionedBytestringKwargifier(
            OldFashionedCoffee,
            blend=VariableLengthBytestring,
            milk_type=(bytes, 13),
            size=(int, 2, {"byteorder": "big"}),
            version=1
        ),
        VersionedBytestringKwargifier(
            EnergyDrink,
            name=VariableLengthBytestring,
            warning_label=VariableLengthBytestring,
            active_ingredient=(bytes, 11),
            size=(int, 2, {"byteorder": "big"}),
            version=2
        )
    ]

    @staticmethod
    def from_bytes(some_bytes):
        version = VersionedBytestringSplitter.get_metadata(some_bytes)['version']
        return BeverageFactory.splitters[version - 1](some_bytes)

    @staticmethod
    def add_version(instance, instance_bytes):
        return BeverageFactory.splitters[instance.version - 1].assign_version(instance_bytes, instance.version)


def test_instantiate_from_versionedbytes():

    unknown_beverage_1_bytes = int(1).to_bytes(2, byteorder="big") + VariableLengthBytestring(b"Equal Exchange Mind, Body, and Soul") + b"local_oatmilk" + int(54453).to_bytes(2, byteorder="big")
    unknown_beverage_2_bytes = int(2).to_bytes(2, byteorder="big") + VariableLengthBytestring(b"MegaBlaster") + VariableLengthBytestring(b"Avoid consumption after 6pm") + b"blastamine5" + int(54453).to_bytes(2, byteorder="big")

    bev1 = BeverageFactory.from_bytes(unknown_beverage_1_bytes)
    bev2 = BeverageFactory.from_bytes(unknown_beverage_2_bytes)
    assert isinstance(bev1, OldFashionedCoffee)
    assert bev1.milk_type == b"local_oatmilk"
    assert isinstance(bev2, EnergyDrink)
    assert bev2.name == b'MegaBlaster'


def test_versioned_instances_to_bytes():

    coffee = OldFashionedCoffee(b"I'm better without milk", b'local_oatmilk', int(1).to_bytes(2, byteorder="big"))
    energy = EnergyDrink(b"Megablaster", b"why you drinking this stuff?", b"FD&CYellow5", int(1).to_bytes(2, byteorder="big"))

    assert bytes(coffee).startswith(b'\x00\x01')
    assert bytes(energy).startswith(b'\x00\x02')

    # one more round trip just to me certain
    assert isinstance(BeverageFactory.from_bytes(bytes(coffee)), OldFashionedCoffee)
    assert isinstance(BeverageFactory.from_bytes(bytes(energy)), EnergyDrink)
