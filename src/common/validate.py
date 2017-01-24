
import ujson
import inspect
import re


class ValidationError(Exception):
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return self.message


_str_name_pattern = re.compile("^([A-Za-z0-9_.-]+)+$")


def validate(**fields):
    def wrapper1(method):
        def wrapper2(*args, **kwargs):
            args_spec = inspect.getargspec(method)

            # validate *args first
            def validate_arg(t):
                field_name, field = t
                validator_name = fields.get(field_name)
                if not validator_name:
                    return field
                validator = VALIDATORS.get(validator_name)
                if not validator:
                    raise ValidationError("No such validator {0}".format(validator_name))
                return validator(field_name, field)

            # then validate **kwargs
            def validate_kwarg(field_name, value):
                validator_name = fields.get(field_name)
                if not validator_name:
                    return value
                validator = VALIDATORS.get(validator_name)
                if not validator:
                    raise ValidationError("No such validator {0}".format(validator_name))
                return validator(field_name, value)

            return method(*map(validate_arg, zip(args_spec.args, args)), **{
                field_name: validate_kwarg(field_name, field)
                for field_name, field in kwargs.iteritems()
            })

        return wrapper2
    return wrapper1


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
        for name, value in field.iteritems()
    }


def _json_dict(field_name, field):
    if not isinstance(field, dict):
        raise ValidationError("Field {0} is not a valid JSON object".format(field_name))

    try:
        ujson.dumps(field)
    except TypeError:
        raise ValidationError("Field {0} is not a valid JSON object".format(field_name))

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
        for name, value in field.iteritems()
    }


def _int(field_name, field):
    try:
        return int(field)
    except (TypeError, ValueError):
        raise ValidationError("Field {0} is not a valid number".format(field_name))


def _int_or_none(field_name, field):
    if field is None:
        return None

    try:
        return int(field)
    except (TypeError, ValueError):
        raise ValidationError("Field {0} is not a valid number".format(field_name))


def _str(field_name, field):
    if not isinstance(field, (str, unicode)):
        raise ValidationError("Field {0} is not a valid string".format(field_name))
    return field


def _str_or_none(field_name, field):
    if field is None:
        return None

    if not isinstance(field, (str, unicode)):
        raise ValidationError("Field {0} is not a valid string".format(field_name))
    return field


def _str_name(field_name, field):
    if not isinstance(field, (str, unicode)):
        raise ValidationError("Field {0} is not a valid string".format(field_name))

    if not _str_name_pattern.match(field):
        raise ValidationError("Field {0} is not a valid name. "
                              "Only A-Z, a-z, 0-9, '_' and '-' is allowed.".format(field_name))

    return field


VALIDATORS = {
    "json": _json,
    "json_dict": _json_dict,
    "json_dict_of_ints": _json_dict_of_ints,
    "int": _int,
    "int_or_none": _int_or_none,
    "str": _str,
    "str_or_none": _str_or_none,
    "string": _str,
    "str_name": _str_name,
    "load_json": _load_json,
    "load_json_dict": _load_json_dict,
    "load_json_dict_of_ints": _load_json_dict_of_ints
}
