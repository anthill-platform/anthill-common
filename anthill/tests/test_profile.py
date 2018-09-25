from tornado.testing import AsyncTestCase, gen_test

from anthill.common.profile import Profile, ProfileError, PredefinedProfile


class TestProfile(AsyncTestCase):
    async def check_profile_success(self, input_value, update_value, check_value, path=None, merge=True):
        profile = PredefinedProfile(input_value)
        result = await profile.set_data(update_value, path=path, merge=merge)
        self.assertEqual(result, check_value)

    async def check_profile_error(self, input_value, update_value, raises_reason, path=None, merge=True):
        profile = PredefinedProfile(input_value)
        try:
            await profile.set_data(update_value, path=path, merge=merge)
        except ProfileError as e:
            self.assertIn(
                raises_reason, e.message,
                "Should raise ProfileError with '{0}', raised '{1}' instead.".format(
                    raises_reason,
                    e.message
                ))
        else:
            raise Exception("Should raise ProfileError with '{0}', not raised".format(raises_reason))

    @gen_test
    async def test_increment(self):
        await self.check_profile_success(
            {"a": 5},
            {"a": {"@func": "++", "@value": 4}},
            {"a": 9})
        await self.check_profile_error(
            {"a": 5},
            {"a": {"@func": "++", "@value": "haha"}},
            "@value is not a number")

    @gen_test
    async def test_decrement(self):
        await self.check_profile_success(
            {"a": 5},
            {"a": {"@func": "--", "@value": 4}},
            {"a": 1})
        await self.check_profile_error(
            {"a": 5},
            {"a": {"@func": "--", "@value": "haha"}},
            "@value is not a number")

    @gen_test
    async def test_decrement_zero(self):
        await self.check_profile_success(
            {"a": 5},
            {"a": {"@func": "--/0", "@value": 4}},
            {"a": 1})
        await self.check_profile_success(
            {"a": 5},
            {"a": {"@func": "--/0", "@value": 5}},
            {"a": 0})
        await self.check_profile_error(
            {"a": 5},
            {"a": {"@func": "--/0", "@value": "haha"}},
            "@value is not a number")
        await self.check_profile_error(
            {"a": 5},
            {"a": {"@func": "--/0", "@value": 6}},
            "not_enough")

    @gen_test
    async def test_equal(self):
        await self.check_profile_success(
            {"a": 5},
            {"a": {"@func": "==", "@cond": 5, "@then": 10}},
            {"a": 10})
        await self.check_profile_success(
            {"a": 5},
            {"a": {"@func": "==", "@cond": 5}},
            {"a": 5})
        await self.check_profile_success(
            {"a": 5},
            {"a": {"@func": "==", "@cond": 20, "@else": 10}},
            {"a": 10})
        await self.check_profile_success(
            {"a": 5},
            {"a": {"@func": "==", "@cond": 20, "@else": {"@func": "++", "@value": 1}}},
            {"a": 6})
        await self.check_profile_error(
            {"a": 5},
            {"a": {"@func": "==", "@cond": 6}},
            "not_equal")

    @gen_test
    async def test_more_or_equal(self):
        await self.check_profile_success(
            {"a": 10},
            {"a": {"@func": ">=", "@cond": 5, "@then": 2}},
            {"a": 2})
        await self.check_profile_success(
            {"a": 5},
            {"a": {"@func": ">=", "@cond": 1}},
            {"a": 5})
        await self.check_profile_success(
            {"a": 5},
            {"a": {"@func": ">=", "@cond": 20, "@else": 10}},
            {"a": 10})
        await self.check_profile_success(
            {"a": 5},
            {"a": {"@func": ">=", "@cond": 20, "@else": {"@func": "++", "@value": 1}}},
            {"a": 6})
        await self.check_profile_success(
            {"a": 5},
            {"a": {"@func": ">=", "@cond": 5}},
            {"a": 5})
        await self.check_profile_error(
            {"a": 5},
            {"a": {"@func": ">=", "@cond": 6}},
            "smaller")

    @gen_test
    async def test_more(self):
        await self.check_profile_success(
            {"a": 10},
            {"a": {"@func": ">", "@cond": 5, "@then": 2}},
            {"a": 2})
        await self.check_profile_success(
            {"a": 5},
            {"a": {"@func": ">", "@cond": 1}},
            {"a": 5})
        await self.check_profile_success(
            {"a": 5},
            {"a": {"@func": ">", "@cond": 20, "@else": 10}},
            {"a": 10})
        await self.check_profile_success(
            {"a": 5},
            {"a": {"@func": ">", "@cond": 20, "@else": {"@func": "++", "@value": 1}}},
            {"a": 6})
        await self.check_profile_error(
            {"a": 5},
            {"a": {"@func": ">", "@cond": 5}},
            "smaller_or_equal")

    @gen_test
    async def test_less(self):
        await self.check_profile_success(
            {"a": 5},
            {"a": {"@func": "<", "@cond": 10, "@then": 2}},
            {"a": 2})
        await self.check_profile_success(
            {"a": 1},
            {"a": {"@func": "<", "@cond": 5}},
            {"a": 1})
        await self.check_profile_success(
            {"a": 20},
            {"a": {"@func": "<", "@cond": 5, "@else": 10}},
            {"a": 10})
        await self.check_profile_success(
            {"a": 20},
            {"a": {"@func": "<", "@cond": 5, "@else": {"@func": "++", "@value": 1}}},
            {"a": 21})
        await self.check_profile_error(
            {"a": 5},
            {"a": {"@func": "<", "@cond": 5}},
            "greater_or_equal")

    @gen_test
    async def test_less_or_equal(self):
        await self.check_profile_success(
            {"a": 5},
            {"a": {"@func": "<=", "@cond": 10, "@then": 2}},
            {"a": 2})
        await self.check_profile_success(
            {"a": 1},
            {"a": {"@func": "<=", "@cond": 5}},
            {"a": 1})
        await self.check_profile_success(
            {"a": 20},
            {"a": {"@func": "<=", "@cond": 5, "@else": 10}},
            {"a": 10})
        await self.check_profile_success(
            {"a": 20},
            {"a": {"@func": "<=", "@cond": 5, "@else": {"@func": "++", "@value": 1}}},
            {"a": 21})
        await self.check_profile_success(
            {"a": 4},
            {"a": {"@func": "<=", "@cond": 4}},
            {"a": 4})
        await self.check_profile_error(
            {"a": 5},
            {"a": {"@func": "<=", "@cond": 4}},
            "greater")

    @gen_test
    async def test_exists(self):
        await self.check_profile_success(
            {"a": 5},
            {"a": {"@func": "exists"}},
            {"a": 5})
        await self.check_profile_error(
            {"a": 5},
            {"b": {"@func": "exists"}},
            "not_exists")
        await self.check_profile_success(
            {"a": 5},
            {"a": {"@func": "exists", "@then": 10}},
            {"a": 10})
        await self.check_profile_success(
            {"a": 5},
            {"b": {"@func": "exists", "@else": 10}},
            {"a": 5, "b": 10})

    @gen_test
    async def test_num_child_where(self):
        await self.check_profile_success(
            {"members": [{"stats": 10}, {"stats": 15}, {"stats": 5}, {"stats": 20}]},
            {"members": {"@func": "num_child_where", "@test": ">", "@field": "stats", "@cond": 10}},
            {"members": 2})
        await self.check_profile_success(
            {"members": {"a": {"stats": 10}, "b": {"stats": 15}, "c": {"stats": 5}, "d": {"stats": 20}}},
            {"members": {"@func": "num_child_where", "@test": ">", "@field": "stats", "@cond": 10}},
            {"members": 2})
        await self.check_profile_success(
            {"members": {"a": {"stats": 10}, "b": {"stats": 15}, "c": {"stats": 5}, "d": {"stats": 20}}},
            {"members": {"@func": ">", "@cond": 1, "@value":
                {"@func": "num_child_where", "@test": ">", "@field": "stats", "@cond": 10}}},
            {"members": {"a": {"stats": 10}, "b": {"stats": 15}, "c": {"stats": 5}, "d": {"stats": 20}}})
        await self.check_profile_error(
            {"members": {"a": {"stats": 10}, "b": {"stats": 15}, "c": {"stats": 5}, "d": {"stats": 20}}},
            {"members": {"@func": ">", "@cond": 3, "@value":
                {"@func": "num_child_where", "@test": ">", "@field": "stats", "@cond": 10}}},
            "smaller_or_equal")

    @gen_test
    async def test_array_append(self):
        await self.check_profile_success(
            {},
            {"a": {"@func": "array_append", "@value": 5}},
            {"a": [5]})
        await self.check_profile_success(
            {"a": ["test1", "test2", "test3"]},
            {"a": {"@func": "array_append", "@value": 5}},
            {"a": ["test1", "test2", "test3", 5]})
        await self.check_profile_error(
            {"a": ["test1", "test2", "test3"]},
            {"a": {"@func": "array_append", "@value": 5, "@limit": 3}},
            "limit_exceeded")
        await self.check_profile_success(
            {"a": ["test1", "test2", "test3"]},
            {"a": {"@func": "array_append", "@value": 5, "@limit": 3, "@shift": True}},
            {"a": ["test2", "test3", 5]})

    @gen_test
    async def test_object(self):
        await self.check_profile_success(
            {"root": {
                "a": 5
            }},
            {"root": {
                "a": {"@func": "++", "@value": 4}
            }},
            {"root": {
                "a": 9
            }})

    @gen_test
    async def test_object_empty(self):
        await self.check_profile_success(
            {},
            {"root": {
                "a": {"@func": "++", "@value": 4}
            }},
            {"root": {
                "a": 4
            }})
