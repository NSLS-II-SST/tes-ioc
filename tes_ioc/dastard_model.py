import asyncio
import zmq
import zmq.asyncio

import json
import itertools


class DastardError(Exception):
    pass


class AsyncDastardModel:

    def __init__(self, host, port, config = {}):
        # Dastard ZMQ Sub
        self.context = zmq.asyncio.Context()
        self.socket = zmq.asyncio.socket(zmq.SUB)
        self.host = host
        self.baseport = port
        self.subport = port + 1
        self.address = f"tcp://{self.host}:{self.subport}"
        self.socket.connect(self.address)
        self.socket.setsockopt_string(zmq.SUBSCRIBE, "")
        self.messages_seen = collections.Counter()
        self.cache = {}

        # Dastard command
        self.config = config
        self._id_iter = itertools.count()
        self._off_filename = ""
        self.write_lock = asyncio.Lock()


    async def update_messages(self):
        while True:
            try:
                topic, contents = await self.get_message()
                
                if topic is None:
                    break
                self.cache[topic] = contents
            except Exception as e:
                print(f"Error updating messages: {e}")
                break        
        
    async def get_message(self):
        msg = await self.socket.recv_multipart()
        try:
            topic, contents_str = msg
        except TypeError:
            raise Exception(f"msg: `{msg}` should have two parts, but does not")
        topic = topic.decode()
        contents = json.loads(contents_str.decode())
        self.messages_seen[topic] += 1
        return topic, contents

    def get_message_with_topic(self, target_topic: str):
        return self.cache.get(target_topic)

    def reset(self):
        print("Reset - draining messages")
        self.update_messages()
        print("Reset - messages drained")
        self.messages_seen = collections.Counter()
        self.cache = {}

    def _message(self, method_name, params):
        if not isinstance(params, list):
            params = [params]
        d = {"id": next(self._id_iter), "params": params, "method": method_name}
        return d

    async def _sendrcv(self, msg):
        async with self.write_lock:
            reader, writer = await asyncio.open_connection(self.host, self.baseport)
            writer.write(msg.encode())
            await writer.drain()

            response = await reader.read(4096)
            writer.close()
            await writer.wait_closed()

    async def send(self, method_name: str, params, verbose=True):
        # Needs a timeout and error!
        msg = self._message(method_name, params)
        print(f"Dastard Client: calling {method_name}")
        response = await self._sendrcv(json.dumps(msg))
        if response == b"":
            print("Got b'', trying to re-send to Dastard")
            response = await self._sendrcv(json.dumps(msg))

        if response == b"":
            raise DastardError("no communication from Dastard")
        
        response = json.loads(response.decode())
        if verbose:
            print(f"Dastard Client: response: {response}")
        else:
            print(f"Dastard Client: got response for {method_name}")
        if not response["id"] == msg["id"]:
            raise DastardError("response id does not match message id")
        err = response.get("error", None)
        if err is not None:
            raise DastardError(f"""Dastard responded with error: {err}""")
        
        return response["result"]

    async def 
