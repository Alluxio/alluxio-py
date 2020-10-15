import json
import random


def assert_string_subclass(obj, name):
    json = obj.json()
    assert json == name
    new_obj = type(obj).from_json(json)
    assert type(new_obj) == type(obj)
    assert new_obj.name == name


def assert_json_encode(obj, json_obj):
    d = vars(obj)
    for key in d:
        assert d[key] == json_obj[key], "{key}::{incorrect} != {correct}".format(key=key, incorrect=repr(d[key]),
                                                                                 correct=repr(json_obj[key]))
    assert d == json_obj


def assert_json_decode(obj, json_dict):
    decoded = type(obj).from_json(json.dumps(json_dict))
    assert type(decoded) == type(obj)
    d = vars(obj)
    json_obj = vars(decoded)
    for key in d:
        assert d[key] == json_obj[key], "{key}::{incorrect} != {correct}".format(key=key, incorrect=repr(d[key]),
                                                                                 correct=repr(json_obj[key]))
    assert vars(decoded) == vars(obj)


def random_bool():
    return random.randint(0, 1) == 0


def random_str():
    result, length = "", random.randint(1, 128)
    for _ in range(length):
        result += chr(random.randint(65, 122))
    return result


def random_int():
    return random.randint(1, 10000)
