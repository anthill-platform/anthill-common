
from tornado.gen import coroutine, Return
from abc import ABCMeta, abstractmethod


class FuncError(Exception):
    def __init__(self, message):
        self.message = message


class Functions(object):

    @staticmethod
    def apply_func(func, object_value, condition, value):
        try:
            f = Functions.FUNCTIONS[func]
        except KeyError:
            raise FuncError("no_such_func")
        else:
            return f.__func__(object_value, condition, value)

    @staticmethod
    def func_decrement(object_value, condition, value):
        if (not isinstance(object_value, (int, float))) or (not isinstance(value, (int, float))):
            raise FuncError("Not a number")
        new_value = (object_value or 0) - value
        if new_value <= 0:
            raise FuncError("not_enough")
        return new_value

    @staticmethod
    def func_equal(object_value, condition, value):
        if object_value != condition:
            raise FuncError("not_equal")
        return value

    @staticmethod
    def func_exists(object_value, condition, value):
        if object_value is None:
            raise FuncError("not_exists")
        return object_value

    @staticmethod
    def func_greater_equal_than(object_value, condition, value):
        if (not isinstance(object_value, (int, float))) or (not isinstance(value, (int, float))):
            raise FuncError("Not a number")
        if (object_value or 0) < condition:
            raise FuncError("smaller")
        return value

    @staticmethod
    def func_greater_than(object_value, condition, value):
        if (not isinstance(object_value, (int, float))) or (not isinstance(value, (int, float))):
            raise FuncError("Not a number")
        if (object_value or 0) <= condition:
            raise FuncError("smaller_or_equal")
        return value

    @staticmethod
    def func_increment(object_value, condition, value):
        if (not isinstance(object_value, (int, float))) or (not isinstance(value, (int, float))):
            raise FuncError("Not a number")
        return (object_value or 0) + value

    @staticmethod
    def func_not_equal(object_value, condition, value):
        if object_value == condition:
            raise FuncError("equal")
        return value

    @staticmethod
    def func_smaller_equal_than(object_value, condition, value):
        if (not isinstance(object_value, (int, float))) or (not isinstance(value, (int, float))):
            raise FuncError("Not a number")
        if (object_value or 0) > condition:
            raise FuncError("greater")
        return value

    @staticmethod
    def func_smaller_than(object_value, condition, value):
        if (not isinstance(object_value, (int, float))) or (not isinstance(value, (int, float))):
            raise FuncError("Not a number")
        if (object_value or 0) >= condition:
            raise FuncError("greater_or_equal")
        return value

    FUNCTIONS = {
        "++": func_increment,
        "--": func_decrement,
        "!=": func_not_equal,
        "==": func_equal,
        "increment": func_increment,
        "decrement": func_decrement,
        "exists": func_exists,
        ">=": func_greater_equal_than,
        "<=": func_smaller_equal_than,
        ">": func_greater_than,
        "<": func_smaller_than
    }


class NoDataError(Exception):
    pass


class Profile(object):
    __metaclass__ = ABCMeta

    @staticmethod
    def __check_value__(field, object_value, value):
        if isinstance(value, dict):
            if ("@func" in value) and ("@value" in value):
                func_name = value["@func"]
                func_condition = value.get("@cond")
                func_value = Profile.__check_value__(field, object_value, value["@value"])
                try:
                    new_value = Functions.apply_func(func_name, object_value, func_condition, func_value)
                except FuncError as e:
                    raise ProfileError("Failed to update field '{0}': {1}".format(field, e.message))
                else:
                    return new_value

        return value

    @staticmethod
    def __get_field__(item, path):
        try:
            key = path.pop(0)

            if not path:
                return item[key]
            else:
                return Profile.__get_field__(item[key], path)
        except KeyError:
            return None

    @staticmethod
    def __merge_profiles__(old_root, new_data, path, merge=True):
        merged = (old_root or {}).copy()
        Profile.__set_profile_fields__(merged, path, new_data, merge=merge)
        return merged

    @staticmethod
    def __set_profile_field__(item, field, value, merge=True):
        object_value = item[field] if field in item else None

        value = Profile.__check_value__(field, object_value, value)

        if merge:
            # in case both items are objects, merge them
            if isinstance(value, dict) and isinstance(object_value, dict):

                for item_key, item_value in value.iteritems():
                    Profile.__set_profile_field__(object_value, item_key, item_value, merge=merge)
                return

        # if a field's value is None, delete such field
        if value is None:
            try:
                item.pop(field)
            except KeyError:
                pass
        else:
            item[field] = value

    @staticmethod
    def __set_profile_fields__(profile, path, fields, merge=True):
        if isinstance(path, list):
            for key in path:
                if key not in profile:
                    profile[key] = {}
                profile = profile[key]

        for key, value in fields.iteritems():
            Profile.__set_profile_field__(profile, key, value, merge=merge)

    @abstractmethod
    @coroutine
    def get(self):
        pass

    @coroutine
    def get_data(self, path):

        yield self.init()

        try:
            data = yield self.get()
        finally:
            yield self.release()

        if data is None:
            raise Return(None)

        if path:
            result = self.__get_field__(data, list(path))
            raise Return(result)
        else:
            raise Return(data)

    @coroutine
    def init(self):
        pass

    @abstractmethod
    @coroutine
    def insert(self, data):
        pass

    @coroutine
    def release(self):
        pass

    @coroutine
    def set_data(self, fields, path, merge=True):
        if not isinstance(fields, dict):
            raise ProfileError("Expected fields to be a dict.")

        yield self.init()

        try:
            data = yield self.get()
        except NoDataError:
            updated = Profile.__merge_profiles__({}, fields, path=path, merge=merge)
            yield self.insert(updated)
        else:
            updated = Profile.__merge_profiles__(data, fields, path=path, merge=merge)
            yield self.update(updated)
        finally:
            yield self.release()

        if path:
            raise Return(Profile.__get_field__(updated, list(path)))
        else:
            raise Return(updated)

    @abstractmethod
    @coroutine
    def update(self, data):
        pass


class DatabaseProfile(Profile):
    __metaclass__ = ABCMeta

    def __init__(self, db):
        super(Profile, self).__init__()
        self.db = db
        self.conn = None

    @coroutine
    def init(self):
        self.conn = yield self.db.acquire(auto_commit=False)

    @coroutine
    def release(self):
        yield self.conn.commit()
        self.conn.close()


class ProfileError(Exception):
    def __init__(self, message):
        self.message = message
