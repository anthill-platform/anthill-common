
import ujson
import inspect
import re
from datetime import datetime


class ValidationError(Exception):
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return self.message


_str_name_pattern = re.compile("^([A-Za-z0-9_.-]+)+$")
_str_tags_pattern = re.compile("^([A-Za-z0-9_.,-]+)+$")


def validate(**fields):
    def wrapper1(method):
        def wrapper2(*args, **kwargs):

            _args, _varargs, _varkw, _defaults_values, i1, i2, i3 = inspect.getfullargspec(method)

            # this nasty trick makes a dict from tail of _args with _defaults_values as values
            kwarg_defaults = {
                key: value
                for key, value in zip(_args[-len(_defaults_values):], _defaults_values)
            } if _defaults_values is not None else None

            # this generator will return tuples (name, value) of *args
            def _list_args():
                for argument_value in args:
                    argument_name = _args.pop(0)
                    yield (argument_name, argument_value)

            # this generator will return tuples (name, value) of **kwargs with their default values, if omitted
            def _list_kwargs():
                for argument_name, argument_value in kwargs.items():
                    if kwarg_defaults is None:
                        yield (argument_name, argument_value, False)
                    elif argument_name in kwarg_defaults:
                        default_value = kwarg_defaults[argument_name]
                        yield (argument_name, argument_value, argument_value == default_value)
                    else:
                        yield (argument_name, argument_value, False)

            def validate_arg(field_name, field):
                validator_name = fields.get(field_name)
                if not validator_name:
                    return field
                if inspect.isclass(validator_name):
                    if isinstance(field, validator_name):
                        return field
                    else:
                        raise ValidationError("{0} is not a '{1}'".format(field_name, validator_name.__name__))
                validator = VALIDATORS.get(validator_name)
                if not validator:
                    raise ValidationError("No such validator {0}".format(validator_name))
                return validator(field_name, field)

            def validate_kwarg(field_name, value):
                validator_name = fields.get(field_name)
                if not validator_name:
                    return value
                if inspect.isclass(validator_name):
                    if isinstance(value, validator_name):
                        return value
                    else:
                        raise ValidationError("{0} is not a '{1}'".format(field_name, validator_name.__name__))
                validator = VALIDATORS.get(validator_name)
                if not validator:
                    raise ValidationError("No such validator {0}".format(validator_name))
                return validator(field_name, value)

            return method(*[
                validate_arg(field_name, field)
                for field_name, field in _list_args()
            ], **{
                field_name: field if _default else validate_kwarg(field_name, field)
                for field_name, field, _default in _list_kwargs()
            })

        return wrapper2
    return wrapper1


def validate_value(value, validator_name):
    if not validator_name:
        return value
    if inspect.isclass(validator_name):
        if isinstance(value, validator_name):
            return value
        else:
            raise ValidationError("Value is not a '{0}'".format(validator_name.__name__))
    validator = VALIDATORS.get(validator_name)
    if not validator:
        raise ValidationError("No such validator {0}".format(validator_name))
    return validator('value', value)


def _json(field_name, field):
    try:
        ujson.dumps(field)
    except TypeError:
        raise ValidationError("Field {0} is not a valid JSON object".format(field_name))
    return field


def _load_json(field_name, field):
    try:
        return ujson.loads(field)
    except (TypeError, ValueError):
        raise ValidationError("Field {0} is not a valid JSON object".format(field_name))


def _load_json_dict(field_name, field):
    try:
        field = ujson.loads(field)
    except (TypeError, ValueError):
        raise ValidationError("Field {0} is not a valid JSON object".format(field_name))

    if not isinstance(field, dict):
        raise ValidationError("Field {0} is not a valid JSON object".format(field_name))

    return field


def _load_json_dict_of_ints(field_name, field):
    try:
        field = ujson.loads(field)
    except (TypeError, ValueError):
        raise ValidationError("Field {0} is not a valid JSON object".format(field_name))

    if not isinstance(field, dict):
        raise ValidationError("Field {0} is not a valid JSON object".format(field_name))

    return {
        _str(name, name): _int(field_name + "." + name, value)
        for name, value in field.items()
    }


def _load_json_dict_of_primitives(field_name, field):
    try:
        field = ujson.loads(field)
    except (TypeError, ValueError):
        raise ValidationError("Field {0} is not a valid JSON object".format(field_name))

    if not isinstance(field, dict):
        raise ValidationError("Field {0} is not a valid JSON object".format(field_name))

    return {
        _str(name, name): _primitive(field_name + "." + name, value)
        for name, value in field.items()
    }


def _json_dict(field_name, field):
    if not isinstance(field, dict):
        raise ValidationError("Field {0} is not a valid JSON object".format(field_name))

    try:
        ujson.dumps(field)
    except TypeError:
        raise ValidationError("Field {0} is not a valid JSON object".format(field_name))

    return field


def _json_list(field_name, field):
    if not isinstance(field, list):
        raise ValidationError("Field {0} is not a valid JSON list".format(field_name))

    try:
        ujson.dumps(field)
    except TypeError:
        raise ValidationError("Field {0} is not a valid JSON list".format(field_name))

    return field


def _json_dict_of_ints(field_name, field):
    try:
        ujson.dumps(field)
    except (TypeError, ValueError):
        raise ValidationError("Field {0} is not a valid JSON object".format(field_name))

    if not isinstance(field, dict):
        raise ValidationError("Field {0} is not a valid JSON object".format(field_name))

    return {
        _str(name, name): _int(field_name + "." + name, value)
        for name, value in field.items()
    }


def _json_dict_of_strings(field_name, field):
    try:
        ujson.dumps(field)
    except (TypeError, ValueError):
        raise ValidationError("Field {0} is not a valid JSON object".format(field_name))

    if not isinstance(field, dict):
        raise ValidationError("Field {0} is not a valid JSON object".format(field_name))

    return {
        _str(name, name): _str(field_name + "." + name, value)
        for name, value in field.items()
    }


def _json_dict_of_primitives(field_name, field):
    try:
        ujson.dumps(field)
    except (TypeError, ValueError):
        raise ValidationError("Field {0} is not a valid JSON object".format(field_name))

    if not isinstance(field, dict):
        raise ValidationError("Field {0} is not a valid JSON object".format(field_name))

    return {
        _str(name, name): _primitive(field_name + "." + name, value)
        for name, value in field.items()
    }


def _json_dict_of_dicts(field_name, field):
    try:
        ujson.dumps(field)
    except (TypeError, ValueError):
        raise ValidationError("Field {0} is not a valid JSON object".format(field_name))

    if not isinstance(field, dict):
        raise ValidationError("Field {0} is not a valid JSON object".format(field_name))

    def _check(name, value):
        if not isinstance(value, dict):
            raise ValidationError("Field {0}.{1} is not a valid JSON object".format(field_name, name))

        return value

    return {
        _str(name, name): _check(name, value)
        for name, value in field.items()
    }


def _json_list_of_strings(field_name, field):
    try:
        ujson.dumps(field)
    except (TypeError, ValueError):
        raise ValidationError("Field {0} is not a valid JSON object".format(field_name))

    if not isinstance(field, list):
        raise ValidationError("Field {0} is not a valid JSON list".format(field_name))

    return [
        _str(field_name, child)
        for child in field
    ]


def _json_list_of_str_name(field_name, field):
    try:
        ujson.dumps(field)
    except (TypeError, ValueError):
        raise ValidationError("Field {0} is not a valid JSON object".format(field_name))

    if not isinstance(field, list):
        raise ValidationError("Field {0} is not a valid JSON list".format(field_name))

    return [
        _str_name(field_name, child)
        for child in field
    ]


def _json_list_of_ints(field_name, field):
    try:
        ujson.dumps(field)
    except (TypeError, ValueError):
        raise ValidationError("Field {0} is not a valid JSON object".format(field_name))

    if not isinstance(field, list):
        raise ValidationError("Field {0} is not a valid JSON list".format(field_name))

    return [
        _int(field_name, child)
        for child in field
    ]


def _int(field_name, field):
    try:
        return int(field)
    except (TypeError, ValueError):
        raise ValidationError("Field {0} is not a valid number".format(field_name))


def _float(field_name, field):
    try:
        return float(field)
    except (TypeError, ValueError):
        raise ValidationError("Field {0} is not a valid floating number".format(field_name))


def _bool(field_name, field):

    if isinstance(field, bool):
        return field

    if isinstance(field, str):
        return field == "true"

    try:
        return bool(field)
    except (TypeError, ValueError):
        raise ValidationError("Field {0} is not a valid bool".format(field_name))


def _str(field_name, field):
    if not isinstance(field, str):
        raise ValidationError("Field {0} is not a valid string".format(field_name))
    return field


def _primitive(field_name, field):
    try:
        return int(field)
    except (TypeError, ValueError):
        if isinstance(field, str):
            if _str_name_pattern.match(field):
                return field
        raise ValidationError("Field {0} is not a valid int or str_name".format(field_name))


def _bytes(field_name, field):
    if not isinstance(field, bytes):
        raise ValidationError("Field {0} is not bytes".format(field_name))
    return field


def _str_name(field_name, field):
    if not isinstance(field, str):
        raise ValidationError("Field {0} is not a valid string".format(field_name))

    if not _str_name_pattern.match(field):
        raise ValidationError("Field {0} is not a valid name. "
                              "Only A-Z, a-z, 0-9, '_' and '-' is allowed.".format(field_name))

    return field


def _str_tags(field_name, field):
    if not isinstance(field, str):
        raise ValidationError("Field {0} is not a valid string".format(field_name))

    if not _str_tags_pattern.match(field):
        raise ValidationError("Field {0} is not a valid name. "
                              "Only A-Z, a-z, 0-9, '_' and '-' is allowed.".format(field_name))

    return field


def _str_datetime(field_name, field):
    try:
        datetime.strptime(field, '%Y-%m-%d %H:%M:%S')
    except ValueError:
        raise ValidationError("Field {0} is not a valid date".format(field_name))
    return field


def _datetime(field_name, field):
    if isinstance(field, str):
        return datetime.strptime(field, '%Y-%m-%d %H:%M:%S')

    if not isinstance(field, datetime):
        raise ValidationError("Field {0} is not a valid date".format(field_name))
    return field


VALIDATORS = {
    "json": _json,
    "json_dict": _json_dict,
    "json_list": _json_list,
    "json_dict_of_ints": _json_dict_of_ints,
    "json_dict_of_strings": _json_dict_of_strings,
    "json_dict_of_primitives": _json_dict_of_primitives,
    "json_dict_of_dicts": _json_dict_of_dicts,
    "json_list_of_strings": _json_list_of_strings,
    "json_list_of_str_name": _json_list_of_str_name,
    "json_list_of_ints": _json_list_of_ints,
    "int": _int,
    "float": _float,
    "str": _str,
    "bytes": _bytes,
    "string": _str,
    "str_name": _str_name,
    "str_tags": _str_tags,
    "str_datetime": _str_datetime,
    "datetime": _datetime,
    "bool": _bool,
    "load_json": _load_json,
    "load_json_dict": _load_json_dict,
    "load_json_dict_of_ints": _load_json_dict_of_ints,
    "load_json_dict_of_primitives": _load_json_dict_of_primitives
}
