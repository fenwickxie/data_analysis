import pytest
from data_analysis.dispatcher import DataDispatcher

def test_zero_padding():
    dispatcher = DataDispatcher()
    dispatcher.set_padding_strategy('zero')
    seq = [1, 2]
    padded = dispatcher.get_module_input.__func__.__closure__[0].cell_contents['pad_or_interp'](dispatcher, seq, 5)
    assert padded == [0, 0, 1, 2]

def test_linear_padding():
    dispatcher = DataDispatcher()
    dispatcher.set_padding_strategy('linear')
    seq = [1, 3]
    padded = dispatcher.get_module_input.__func__.__closure__[0].cell_contents['pad_or_interp'](dispatcher, seq, 4)
    assert all(isinstance(x, float) for x in padded)

def test_forward_padding():
    dispatcher = DataDispatcher()
    dispatcher.set_padding_strategy('forward')
    seq = [5]
    padded = dispatcher.get_module_input.__func__.__closure__[0].cell_contents['pad_or_interp'](dispatcher, seq, 3)
    assert padded == [5, 5, 5]

def test_missing_padding():
    dispatcher = DataDispatcher()
    dispatcher.set_padding_strategy('missing')
    seq = []
    padded = dispatcher.get_module_input.__func__.__closure__[0].cell_contents['pad_or_interp'](dispatcher, seq, 2)
    assert padded == [None, None]
