
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
        """
        Function that decrements Profile's value by '@value' field. For example, this object:
        
        { "a": 10 } after applying the function { "@func": "--", "@value": 5 } to it will be updated to be: { "a": 5 }
        """
        if object_value is not None:
            if (not isinstance(object_value, (int, float))) or (not isinstance(value, (int, float))):
                raise FuncError("Not a number")
        new_value = (object_value or 0) - value
        if new_value <= 0:
            raise FuncError("not_enough")
        return new_value

    @staticmethod
    def func_equal(object_value, condition, value):
        """
        Function that ensures the field is equal to '@value' field. if not, update will fail with error:
        
        { "a": 10 } after applying the function { "@func": "==", "@value": 9 } to it will fail with FuncError, but
            after applying the function { "@func": "==", "@value": 10 } nothing will happen.
            
        This function along with other is useful to make certain operation only if certain requirement is met.
        """
        if object_value != condition:
            raise FuncError("not_equal")
        return value

    @staticmethod
    def func_exists(object_value, condition, value):
        """
        Function that ensures the field exists. if not, update will fail with error:
        
        { "a": 10 }
        
        This update:
        
        { 
            "b": { "@func": "exists" }
        }
        
        will fail with FuncError, but this one:
        
        { 
            "a": { "@func": "exists" }
        }
        
        wont.
            
        This function along with other is useful to make certain operation only if certain requirement is met.
        """
        if object_value is None:
            raise FuncError("not_exists")
        return object_value

    @staticmethod
    def func_greater_equal_than(object_value, condition, value):
        """
        Function that ensures the field is >= to '@value' field. if not, update will fail with error:
        
        { "a": 8 } after applying the function { "@func": ">=", "@value": 9 } to it will fail with FuncError, but
            after applying the function { "@func": ">=", "@value": 7 } nothing will happen.
            
        This function along with other is useful to make certain operation only if certain requirement is met.
        """
        if object_value is not None:
            if (not isinstance(object_value, (int, float))) or (not isinstance(value, (int, float))):
                raise FuncError("Not a number")
        if (object_value or 0) < condition:
            raise FuncError("smaller")
        return value

    @staticmethod
    def func_greater_than(object_value, condition, value):
        if object_value is not None:
            if (not isinstance(object_value, (int, float))) or (not isinstance(value, (int, float))):
                raise FuncError("Not a number")
        if (object_value or 0) <= condition:
            raise FuncError("smaller_or_equal")
        return value

    @staticmethod
    def func_increment(object_value, condition, value):
        """
        Function that increments Profile's value by '@value' field. For example, this object:
        
        { "a": 10 } after applying the function { "@func": "++", "@value": 5 } will be updated to be: { "a": 15 }
        """
        if object_value is not None:
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
        if object_value is not None:
            if (not isinstance(object_value, (int, float))) or (not isinstance(value, (int, float))):
                raise FuncError("Not a number")
        if (object_value or 0) > condition:
            raise FuncError("greater")
        return value

    @staticmethod
    def func_smaller_than(object_value, condition, value):
        if object_value is not None:
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

    """
    A class that represents abstract Profile.
    
    Profile is a JSON object that can be tied to various entities (such as users, groups, or even leaderboard records)
        and then be used in context of that entities as their profile (for example, a User Profile would be a Profile
        object of the certain user).
        
    The associated IDs should be assigned to the Profile object during creation of that Profile object:
            
    class SomeProfile(Profile):
        def __init__(self, db, gamespace_id, some_id):
            super(Profile, self).__init__(db)
            self.gamespace_id = gamespace_id
            self.some_id = some_id
            
    In order of all this to work, at least several methods must be implemented: Profile.get, Profile.insert 
        and Profile.update. See the documentation for the methods to understand their usage. After implementing such 
        methods, set_data and get_data may be used for actual profile actions.
        
    Other than that, various 'functions' are supported during update. A 'function' is a special JSON object that passed
        instead of actual value to the certain field. Once such object is detected, a 'function' will be applied to it.
    
    {
        "@func": <a function name>,
        "@cond": <optional condition parameter>
        "@value": <a value that will be applied to the function>
    }
        
    For example, say we have this object:
    
    { "b": 3 }
    
    and we apply such update to it:
    
    { "b": {
        "@func": "++",
        "@value": 7
    }}
    
    The function will be detected, and the original value (3) will be @func'd (incremented) by @value (7):
    
    { "b": 10 }
    
    This makes a lot of sense in concurrent environment (for example, if two clients are applying increment at the 
        same time to the same field, the resulting value would be a sum of those increments).
    
    Functions can be even nested. For example, if we apply a such update to previous object:
    
    {
        "b": {
            "@func": "<",
            "@cond": 50
            "@value": {
                "@func": "++",
                "@value": 1
            },
        }
    }
    
    Then the field 'b' will be incremented by 1 (with concurrency support) but only if 'b' is smaller than 50, thus
        guaranteeing the total amount cannot be greater than 50 concurrently.
    
    See FUNCTIONS dict for complete list of supported functions.
        
    """

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
        """
        Called when certain Profile object is requested. This method should return (well, raise Return()) 
            a complete JSON object that represents the Profile.  If the requested object is not found, 
            NoDataError should be raised.
        
        :returns a complete JSON object that represents the Profile
        :raises NoDataError if no Profile could be found
        """

        pass

    @abstractmethod
    @coroutine
    def insert(self, data):
        """
        Called when certain Profile object is being created. 
        
        :param data A JSON object that should be associated to the Profile object
        :raises ProfileError if the creation is not supported
        """

        pass

    @abstractmethod
    @coroutine
    def update(self, data):
        """
        Called when certain Profile object is being changed (updated).
        :param data: A JSON object that should be used to update the Profile object 
        """

        pass

    @coroutine
    def init(self):
        """
        Called upon initialization of the Profile instance.
        """

        pass

    @coroutine
    def release(self):
        """
        Called once the Profile instance should be released.
        """
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

    @staticmethod
    def merge_data(old_root, new_data, path, merge=True):
        return Profile.__merge_profiles__(old_root, new_data, path=path, merge=merge)


class DatabaseProfile(Profile):

    """
    A yet abstract implementation of Profile object that uses Database as storage that allows concurrent requests
        to be made.
    
    Typical usage:
    
        @coroutine
        def get(self):
            profile = yield self.conn.get(
                '''
                    SELECT `profile_object`
                    FROM `table`
                    WHERE ...
                    FOR UPDATE;
                ''', ...)
    
            if profile:
                raise Return(profile["payload"])
    
            raise common.profile.NoDataError()
    
        @coroutine
        def insert(self, data):
        
            yield self.conn.insert(
                '''
                    INSERT INTO `table`
                    (..., `profile_object`)
                    VALUES (..., %s);
                ''', ..., data)
    
        @coroutine
        def update(self, data):
            yield self.conn.execute(
                '''
                    UPDATE `table`
                    SET `profile_object`=%s
                    WHERE ...;
                ''', ujson.dumps(data), ...)
    
    """

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
