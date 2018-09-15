from fdhandle.stocks import get_comcode_map, get_stockcode_map, \
    get_innercode_map

_comcode_map = None
_stockcode_map = None
_innercode_map = None
_orderbookid_map = None


def comecode_map():
    global _comcode_map
    if not _comcode_map:
        _comcode_map = get_comcode_map()
    return _comcode_map


def stockcode_map():
    global _stockcode_map
    if not _stockcode_map:
        _stockcode_map = get_stockcode_map()
    return _stockcode_map


def innercode_map():
    global _innercode_map
    if not _innercode_map:
        _innercode_map = get_innercode_map()
    return _innercode_map


def orderbookid_map():
    global _orderbookid_map
    if not _orderbookid_map:
        _orderbookid_map = {}
        innercodes_map = innercode_map()
        stockcodes_map = stockcode_map()
        for inner_code, stockcode in innercodes_map.items():
            _orderbookid_map[stockcodes_map[stockcode]] = inner_code
    return _orderbookid_map
