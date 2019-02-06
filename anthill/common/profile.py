
from abc import ABCMeta, abstractmethod


class FuncError(Exception):
    def __init__(self, message):
        self.message = message


class Functions(object):

    @staticmethod
    def check_value(field_name, object_value, value):
        if isinstance(value, dict):
            func_name = value.get("@func", None)
            if func_name:
                return Functions.apply_func(str(func_name), field_name, object_value, value)
        return True, value

    @staticmethod
    def apply_func(func_name, field_name, object_value, arguments):
        try:
            f = Functions.FUNCTIONS[func_name]
        except KeyError:
            raise ProfileError("No such function: " + str(func_name))
        else:
            try:
                do_apply, new_value = f.__func__(field_name, object_value, arguments)
            except FuncError as e:
                raise ProfileError("Failed to update field '{0}': {1}".format(field_name, e.message))
            else:
                return do_apply, new_value

    @staticmethod
    def func_decrement(field_name, object_value, arguments, **ignored):
        """
        Function that decrements Profile's value by '@value' field.

        Arguments:
            @value: how much to decrement

        For example, this object:

        { "a": 10 } after applying the function { "@func": "--", "@value": 5 } to it will be updated to be: { "a": 5 }
        """

        value = arguments.get("@value", None)
        if not value:
            raise FuncError("@value is not defined")
        if not isinstance(value, (int, float)):
            raise FuncError("@value is not a number")

        if object_value is not None:
            if (not isinstance(object_value, (int, float))) or (not isinstance(value, (int, float))):
                raise FuncError("Not a number")
        new_value = (object_value or 0) - value

        return True, new_value

    @staticmethod
    def func_decrement_zero(field_name, object_value, arguments, **ignored):
        """
        Function that decrements Profile's value by '@value' field, only if new value is >= 0.

        Arguments:
            @value: how much to decrement

        For example, this object:

        { "a": 10 } after applying the function { "@func": "--", "@value": 5 } to it will be updated to be: { "a": 5 }
        """

        value = arguments.get("@value", None)
        if not value:
            raise FuncError("@value is not defined")
        if not isinstance(value, (int, float)):
            raise FuncError("@value is not a number")

        if object_value is not None:
            if (not isinstance(object_value, (int, float))) or (not isinstance(value, (int, float))):
                raise FuncError("Not a number")
        new_value = (object_value or 0) - value
        if new_value < 0:
            raise FuncError("not_enough")

        return True, new_value

    @staticmethod
    def func_equal(field_name, object_value, arguments, **ignored):
        """
        Function that ensures the field is equal to '@value' field.

        Arguments:
            @cond: An object to compare the value to
            @value: (Optional) a value to use instead of object's value (see below), useful for nesting
            @then: (Optional) a value to use, if the condition is equal to object's value. If condition is equal,
                and @then is not defined, nothing happens (object's value remains the same)
            @else: (Optional) a value to use, if the condition is not equal to object's value. If the condition is not
                equal, and @else is not defined, FuncError 'not_equal' is raised.

        if not, update will fail with error:
        
        { "a": 10 } after applying the function { "@func": "==", "@cond": 9 } to it will fail with FuncError, but
            after applying the function { "@func": "==", "@cond": 10 } nothing will happen.
            
        This function along with other is useful to make certain operation only if certain requirement is met.
        """

        condition = arguments.get("@cond", None)
        if not condition:
            raise FuncError("@cond is not defined")

        value = arguments.get("@value", None)
        if value:
            ignored, value = Functions.check_value(field_name, object_value, value)
        else:
            value = object_value

        if value != condition:
            else_ = arguments.get("@else", None)
            if else_:
                return Functions.check_value(field_name, value, else_)
            raise FuncError("not_equal")

        then_ = arguments.get("@then", None)
        if then_:
            return Functions.check_value(field_name, value, then_)

        return False, None

    @staticmethod
    def func_exists(field_name, object_value, arguments, **ignored):
        """
        Function that ensures the field exists.

        Arguments:
            @then: (Optional) a value to use, if the object exists. If object exists,
                and @then is not defined, nothing happens (object remains present)
            @else: (Optional) a value to use, if the object does not exist. If object does not exist,
                and @else is not defined, FuncError 'not_exists' is raised.

        Example value:
        {"a": 10}
        
        Example updates
        {"b": { "@func": "exists" }} -> FuncError 'not_exists'
        {"a": { "@func": "exists" }} -> Nothing happens
        {"a": { "@func": "exists", "@then": 5 }} -> {"a": 5}
        {"b": { "@func": "exists", "@else": 5 }} -> {"a": 10, "b": 5}
        {"a": { "@func": "exists", "@else": 5 }} -> {"a": 10}

        """
        if object_value is None:
            else_ = arguments.get("@else", None)
            if else_:
                return Functions.check_value(field_name, object_value, else_)
            raise FuncError("not_exists")

        then_ = arguments.get("@then", None)
        if then_:
            return Functions.check_value(field_name, object_value, then_)

        return False, None

    @staticmethod
    def func_array_append(field_name, object_value, arguments, **ignored):
        """
        Function that appends an object into the end of the array (list)

        Arguments:
            @value: Valid JSON object to add
            @limit: (Optional) A maximum size of the array. If the limit is reached, 'limit_exceeded' is raised.
            @shift: (Optional) If true, and limit is reached, the first element will be deleted to free space

        Example value:
        {"a": ["test1", "test2", "test3"]}

        Example updates
        {"a": { "@func": "append", "@value": 5 }} -> {"a": ["test1", "test2", "test3", 5]}
        {"a": { "@func": "append", "@value": 5, "@limit": 3 }} -> FuncError 'limit_exceeded'
        {"a": { "@func": "append", "@value": 5, "@limit": 3, "@shift": true }} -> {"a": ["test2", "test3", 5]}

        """

        value = arguments.get("@value", None)
        if not value:
            raise FuncError("@value is not defined")

        if object_value:
            if not isinstance(object_value, list):
                raise FuncError("Object is not a list")
        else:
            object_value = []

        object_value.append(value)

        limit = arguments.get("@limit", None)
        if limit:
            if not isinstance(limit, (int, float)):
                raise FuncError("@limit is not a number")
            if len(object_value) > limit:
                if arguments.get("@shift", False):
                    object_value = object_value[-limit:]
                else:
                    raise FuncError("limit_exceeded")

        return True, object_value

    @staticmethod
    def func_not_exists(field_name, object_value, arguments, **ignored):
        """
        Function that ensures the field does not exist.

        Arguments:
            @then: (Optional) a value to use, if the object does not exist. If object does not exist,
                and @then is not defined, nothing happens (object remains not present)
            @else: (Optional) a value to use, if the object does exist. If object does exist,
                and @else is not defined, FuncError 'exists' is raised.

        Example value:
        {"a": 10}

        Example updates
        {"b": { "@func": "exists" }} -> FuncError 'not_exists'
        {"a": { "@func": "exists" }} -> Nothing happens
        {"a": { "@func": "exists", "@then": 5 }} -> {"a": 5}
        {"b": { "@func": "exists", "@else": 5 }} -> {"a": 10, "b": 5}
        {"a": { "@func": "exists", "@else": 5 }} -> {"a": 10}

        """
        if object_value is not None:
            else_ = arguments.get("@else", None)
            if else_:
                return Functions.check_value(field_name, object_value, else_)
            raise FuncError("exists")

        then_ = arguments.get("@then", None)
        if then_:
            return Functions.check_value(field_name, object_value, then_)

        return False, None

    @staticmethod
    def func_num_child_where(field_name, object_value, arguments):
        """
        Function that return number of child objects (that assumes that the object in question is a dict
            or a list of dicts) that pass with come criteria.

        Arguments:
            @test: Conditional function to test child object's fields upon.
            @field: Field name of child objects to test
            ... others are passed to Conditional function

        For example, say you have this object:
        {"members": {"a": {"stats": 20}, "b": {"stats": 10}, "c": {"stats": 5}, "d": {"stats": 200}}}

        And you would to check how much of members do have "stats" more that 15:

        {
            "members": {"@func": "num_child_where", "@test": ">", "@field": "stats", "@value": 15}
        }

        This is practically useful if you just need to make sure that there's more that 2 of such:

        {
            "members": {
                "@value": {
                    "@func": "num_child_where",
                    "@test": ">",
                    "@field": "stats",
                    "@value": 15
                },
                "@func": ">",
                "@cond": 2
            }
        }

        """

        func_name = arguments.get("@test")
        field_name = arguments.get("@field")
        if func_name is None:
            raise FuncError("@cond is not defined")
        if field_name is None:
            raise FuncError("@field is not defined")

        f = Functions.FUNCTIONS.get(func_name)
        if f is None:
            raise FuncError("No such function: " + str(func_name))

        def check(child):
            if isinstance(child, dict):
                child_value = child.get(field_name, None)
                try:
                    # return values are ignored
                    f.__func__(field_name, child_value, arguments)
                except FuncError:
                    return False
                else:
                    return True
            return False

        if isinstance(object_value, dict):
            return True, sum(1 for item in object_value.values() if check(item))
        elif isinstance(object_value, list):
            return True, sum(1 for item in object_value if check(item))
        else:
            raise FuncError("Object is neither dict or list")

    @staticmethod
    def func_greater_equal_than(field_name, object_value, arguments):
        """
        Function that ensures the field is >= to '@value' field.

        Arguments:
            @cond: An object to compare the value to
            @value: (Optional) a value to use instead of object's value (see below), useful for nesting
            @then: (Optional) a value to use, if the condition is >= to object's value. If condition is >=,
                and @then is not defined, nothing happens (object's value remains the same)
            @else: (Optional) a value to use, if the condition is < to object's value. If the condition is <,
                and @else is not defined, FuncError 'smaller' is raised.

        if not, update will fail with error:
        
        { "a": 8 } after applying the function { "@func": ">=", "@value": 9 } to it will fail with FuncError, but
            after applying the function { "@func": ">=", "@value": 7 } nothing will happen.
            
        This function along with other is useful to make certain operation only if certain requirement is met.
        """

        condition = arguments.get("@cond", None)
        if not condition:
            raise FuncError("@cond is not defined")

        value = arguments.get("@value", None)
        if value:
            ignored, value = Functions.check_value(field_name, object_value, value)
        else:
            value = object_value

        if (value is not None) and (not isinstance(value, (int, float))):
            raise FuncError("value is not a number")

        if (value or 0) < condition:
            else_ = arguments.get("@else", None)
            if else_:
                return Functions.check_value(field_name, value, else_)
            raise FuncError("smaller")

        then_ = arguments.get("@then", None)
        if then_:
            return Functions.check_value(field_name, value, then_)

        return False, None

    @staticmethod
    def func_greater_than(field_name, object_value, arguments):
        """
        Function that ensures the field is > to '@value' field.

        Arguments:
            @cond: An object to compare the value to
            @value: (Optional) a value to use instead of object's value (see below), useful for nesting
            @then: (Optional) a value to use, if the condition is > to object's value. If condition is >,
                and @then is not defined, nothing happens (object's value remains the same)
            @else: (Optional) a value to use, if the condition is <= to object's value. If the condition is <=,
                and @else is not defined, FuncError 'smaller_or_equal' is raised.

        if not, update will fail with error:

        { "a": 8 } after applying the function { "@func": ">", "@value": 9 } to it will fail with FuncError, but
            after applying the function { "@func": ">", "@value": 7 } nothing will happen.

        This function along with other is useful to make certain operation only if certain requirement is met.
        """

        condition = arguments.get("@cond", None)
        if not condition:
            raise FuncError("@cond is not defined")

        value = arguments.get("@value", None)
        if value:
            ignored, value = Functions.check_value(field_name, object_value, value)
        else:
            value = object_value

        if (value is not None) and (not isinstance(value, (int, float))):
            raise FuncError("value is not a number")

        if (value or 0) <= condition:
            else_ = arguments.get("@else", None)
            if else_:
                return Functions.check_value(field_name, value, else_)
            raise FuncError("smaller_or_equal")

        then_ = arguments.get("@then", None)
        if then_:
            return Functions.check_value(field_name, value, then_)

        return False, None

    @staticmethod
    def func_increment(field_name, object_value, arguments):
        """
        Function that increments Profile's value by '@value' field.

        Arguments:
            @value: how much to decrement

        For example, this object:
        
        { "a": 10 } after applying the function { "@func": "++", "@value": 5 } will be updated to be: { "a": 15 }
        """

        value = arguments.get("@value", None)
        if not value:
            raise FuncError("@value is not defined")
        if not isinstance(value, (int, float)):
            raise FuncError("@value is not a number")

        if object_value is not None:
            if (not isinstance(object_value, (int, float))) or (not isinstance(value, (int, float))):
                raise FuncError("Not a number")

        return True, (object_value or 0) + value

    @staticmethod
    def func_not_equal(field_name, object_value, arguments):
        """
        Function that ensures the field is not equal to '@value' field.

        Arguments:
            @cond: An object to compare the value to
            @value: (Optional) a value to use instead of object's value (see below), useful for nesting
            @then: (Optional) a value to use, if the condition is not equal to object's value. If condition is not
                equal, and @then is not defined, nothing happens (object's value remains the same)
            @else: (Optional) a value to use, if the condition is equal to object's value. If the condition is equal,
                and @else is not defined, FuncError 'equal' is raised.

        if not, update will fail with error:

        { "a": 10 } after applying the function { "@func": "!=", "@cond": 10 } to it will fail with FuncError, but
            after applying the function { "@func": "!=", "@cond": 9 } nothing will happen.

        This function along with other is useful to make certain operation only if certain requirement is met.
        """

        condition = arguments.get("@cond", None)
        if not condition:
            raise FuncError("@cond is not defined")

        value = arguments.get("@value", None)
        if value:
            ignored, value = Functions.check_value(field_name, object_value, value)
        else:
            value = object_value

        if value == condition:
            else_ = arguments.get("@else", None)
            if else_:
                return Functions.check_value(field_name, value, else_)
            raise FuncError("equal")

        then_ = arguments.get("@then", None)
        if then_:
            return Functions.check_value(field_name, value, then_)

        return False, None

    @staticmethod
    def func_smaller_equal_than(field_name, object_value, arguments):
        """
        Function that ensures the field is <= to '@value' field.

        Arguments:
            @cond: An object to compare the value to
            @value: (Optional) a value to use instead of object's value (see below), useful for nesting
            @then: (Optional) a value to use, if the condition is <= to object's value. If condition is <=,
                and @then is not defined, nothing happens (object's value remains the same)
            @else: (Optional) a value to use, if the condition is > to object's value. If the condition is >,
                and @else is not defined, FuncError 'greater' is raised.

        if not, update will fail with error:

        { "a": 8 } after applying the function { "@func": "<=", "@value": 7 } to it will fail with FuncError, but
            after applying the function { "@func": "<=", "@value": 9 } nothing will happen.

        This function along with other is useful to make certain operation only if certain requirement is met.
        """

        condition = arguments.get("@cond", None)
        if not condition:
            raise FuncError("@cond is not defined")

        value = arguments.get("@value", None)
        if value:
            ignored, value = Functions.check_value(field_name, object_value, value)
        else:
            value = object_value

        if (value is not None) and (not isinstance(value, (int, float))):
                raise FuncError("value is not a number")

        if (value or 0) > condition:
            else_ = arguments.get("@else", None)
            if else_:
                return Functions.check_value(field_name, value, else_)
            raise FuncError("greater")

        then_ = arguments.get("@then", None)
        if then_:
            return Functions.check_value(field_name, value, then_)

        return False, None

    @staticmethod
    def func_smaller_than(field_name, object_value, arguments):
        """
        Function that ensures the field is < to '@value' field.

        Arguments:
            @cond: An object to compare the value to
            @value: (Optional) a value to use instead of object's value (see below), useful for nesting
            @then: (Optional) a value to use, if the condition is < to object's value. If condition is <,
                and @then is not defined, nothing happens (object's value remains the same)
            @else: (Optional) a value to use, if the condition is >= to object's value. If the condition is >=,
                and @else is not defined, FuncError 'greater_or_equal' is raised.

        if not, update will fail with error:

        { "a": 8 } after applying the function { "@func": "<", "@value": 7 } to it will fail with FuncError, but
            after applying the function { "@func": "<", "@value": 9 } nothing will happen.

        This function along with other is useful to make certain operation only if certain requirement is met.
        """

        condition = arguments.get("@cond", None)
        if not condition:
            raise FuncError("@cond is not defined")

        value = arguments.get("@value", None)
        if value:
            ignored, value = Functions.check_value(field_name, object_value, value)
        else:
            value = object_value

        if (value is not None) and (not isinstance(value, (int, float))):
            raise FuncError("value is not a number")

        if (value or 0) >= condition:
            else_ = arguments.get("@else", None)
            if else_:
                return Functions.check_value(field_name, value, else_)
            raise FuncError("greater_or_equal")

        then_ = arguments.get("@then", None)
        if then_:
            return Functions.check_value(field_name, value, then_)

        return False, None

    FUNCTIONS = {
        "++": func_increment,
        "--": func_decrement,
        "--/0": func_decrement_zero,
        "!=": func_not_equal,
        "==": func_equal,
        "increment": func_increment,
        "decrement": func_decrement,
        "decrement_greater_zero": func_decrement_zero,
        "exists": func_exists,
        "not_exists": func_not_exists,
        "array_append": func_array_append,
        ">=": func_greater_equal_than,
        "<=": func_smaller_equal_than,
        ">": func_greater_than,
        "<": func_smaller_than,
        "num_child_where": func_num_child_where
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

    Also, the whole profile pattern may be used simply by using static method Profile.merge_data
    
    See FUNCTIONS dict for complete list of supported functions.
        
    """

    __metaclass__ = ABCMeta

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

        do_apply, value = Functions.check_value(field, object_value, value)

        if not do_apply:
            return

        if merge:
            # in case both items are objects, merge them
            if isinstance(value, dict):
                if object_value is None:
                    object_value = {}
                    item[field] = object_value
                    for item_key, item_value in value.items():
                        Profile.__set_profile_field__(object_value, item_key, item_value, merge=merge)
                elif isinstance(object_value, dict):
                    for item_key, item_value in value.items():
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

        for key, value in fields.items():
            Profile.__set_profile_field__(profile, key, value, merge=merge)

    @abstractmethod
    async def get(self):
        """
        Called when certain Profile object is requested. This method should return (well, return )
            a complete JSON object that represents the Profile.  If the requested object is not found, 
            NoDataError should be raised.
        
        :returns a complete JSON object that represents the Profile
        :raises NoDataError if no Profile could be found
        """

        pass

    @abstractmethod
    async def insert(self, data):
        """
        Called when certain Profile object is being created. 
        
        :param data A JSON object that should be associated to the Profile object
        :raises ProfileError if the creation is not supported
        """

        pass

    @abstractmethod
    async def update(self, data):
        """
        Called when certain Profile object is being changed (updated).
        :param data: A JSON object that should be used to update the Profile object 
        """

        pass

    async def init(self):
        """
        Called upon initialization of the Profile instance.
        """

        pass

    async def release(self):
        """
        Called once the Profile instance should be released.
        """
        pass

    async def get_data(self, path):
        if path is not None and not isinstance(path, list):
            path = list(path)

        await self.init()

        try:
            data = await self.get()
        finally:
            await self.release()

        if data is None:
            return None

        if path:
            result = self.__get_field__(data, path)
            return result
        else:
            return data

    async def set_data(self, fields, path, merge=True):
        if path is not None and not isinstance(path, list):
            path = list(path)

        if not isinstance(fields, dict):
            raise ProfileError("Expected fields to be a dict.")

        await self.init()

        try:
            data = await self.get()
        except NoDataError:
            updated = Profile.__merge_profiles__({}, fields, path=path, merge=merge)
            await self.insert(updated)
        else:
            updated = Profile.__merge_profiles__(data, fields, path=path, merge=merge)
            await self.update(updated)
        finally:
            await self.release()

        if path:
            return Profile.__get_field__(updated, path)
        else:
            return updated

    @staticmethod
    def merge_data(old_root, new_data, path, merge=True):
        return Profile.__merge_profiles__(old_root, new_data, path=path, merge=merge)


class DatabaseProfile(Profile):

    """
    A yet abstract implementation of Profile object that uses Database as storage that allows concurrent requests
        to be made.
    
    Typical usage:
    
        async def get(self):
            profile = await self.conn.get(
                '''
                    SELECT `profile_object`
                    FROM `table`
                    WHERE ...
                    FOR UPDATE;
                ''', ...)
    
            if profile:
                return profile["payload"]
    
            raise common.profile.NoDataError()
    
        async def insert(self, data):
        
            await self.conn.insert(
                '''
                    INSERT INTO `table`
                    (..., `profile_object`)
                    VALUES (..., %s);
                ''', ..., data)
    
        async def update(self, data):
            await self.conn.execute(
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

    async def init(self):
        self.conn = self.db.acquire(auto_commit=False)
        await self.conn.init()

    async def release(self):
        await self.conn.commit()
        self.conn.close()


class PredefinedProfile(Profile):
    """
    Profile object with value object already supplied.
    Updating or inserting is nor supported.
    """
    def __init__(self, value):
        self.value = value

    async def get(self):
        return self.value

    async def update(self, data):
        pass

    async def insert(self, data):
        pass


class ProfileError(Exception):
    def __init__(self, message):
        self.message = message
