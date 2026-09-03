import asyncio
import unittest

from app.controllers import device_controller as dc
from app.controllers import message_controller as mc
from app.controllers import plc_controller as pc
from app.controllers.callbacks_mqtt_controller import CallbacksMQTTContrller
from app.main.main import app


class _Result:
    def scalar_one_or_none(self):
        return None

    def scalars(self):
        return self

    def all(self):
        return []


class _CaptureSession:
    def __init__(self):
        self.statements = []
        self.added = []

    async def execute(self, statement):
        self.statements.append(statement)
        return _Result()

    def add(self, value):
        self.added.append(value)

    async def flush(self):
        return None


class TenantQueryTests(unittest.IsolatedAsyncioTestCase):
    async def test_new_device_is_bound_to_creator(self):
        db = _CaptureSession()

        device = await dc.register_device(
            db,
            user_id=17,
            device_id="device-a",
            name="Device A",
            description=None,
            topics=["tenant/a"],
            api_key_hash="hash",
        )

        self.assertEqual(device.user_id, 17)

    async def test_device_list_filters_owner(self):
        db = _CaptureSession()

        await dc.list_device(db, user_id=17)

        statement = db.statements[0]
        self.assertIn("devices.user_id", str(statement))
        self.assertIn(17, statement.compile().params.values())

    async def test_plc_lookup_filters_owner_through_device(self):
        db = _CaptureSession()

        await pc.search_plc(db, plc_id=9, user_id=17)

        statement = db.statements[0]
        sql = str(statement)
        self.assertIn("JOIN devices", sql)
        self.assertIn("devices.user_id", sql)
        self.assertIn(17, statement.compile().params.values())

    async def test_message_list_filters_owner_through_device(self):
        db = _CaptureSession()

        await mc.list_message(db, user_id=17)

        statement = db.statements[0]
        sql = str(statement)
        self.assertIn("JOIN devices", sql)
        self.assertIn("devices.user_id", sql)
        self.assertIn(17, statement.compile().params.values())


class WebSocketIsolationTests(unittest.IsolatedAsyncioTestCase):
    async def test_broadcast_reaches_only_device_owner(self):
        controller = CallbacksMQTTContrller()
        controller._loop = asyncio.get_running_loop()
        owner_queue = asyncio.Queue()
        other_queue = asyncio.Queue()

        controller.register_ws_queue(owner_queue, {"device-a"})
        controller.register_ws_queue(other_queue, {"device-b"})
        controller.broadcast_message(
            device_id="device-a",
            topic="tenant/a",
            payload="42",
            qos=1,
        )
        await asyncio.sleep(0)

        self.assertEqual((await owner_queue.get())["device_id"], "device-a")
        self.assertTrue(other_queue.empty())


class RouteAuthenticationTests(unittest.TestCase):
    def test_all_non_public_api_operations_require_bearer_auth(self):
        schema = app.openapi()
        public_operations = {("/api/v1/users", "post")}
        missing_auth = []

        for path, path_item in schema["paths"].items():
            if not path.startswith("/api/v1"):
                continue
            for method, operation in path_item.items():
                if method not in {
                    "get", "post", "put", "patch", "delete", "options", "head"
                }:
                    continue
                if (path, method) in public_operations:
                    continue
                if not operation.get("security"):
                    missing_auth.append(f"{method.upper()} {path}")

        self.assertEqual(missing_auth, [])


if __name__ == "__main__":
    unittest.main()
