import asyncio
import zmq
import zmq.asyncio
import collections
import json
import itertools
import time


class DastardError(Exception):
    pass


class AsyncDastardModel:

    def __init__(self, host, port, config = {}):
        # Dastard ZMQ Sub
        self.context = zmq.asyncio.Context()
        self.socket = self.context.socket(zmq.SUB)
        self.host = host
        self.baseport = port
        self.subport = port + 1
        self.address = f"tcp://{self.host}:{self.subport}"
        self.socket.connect(self.address)
        self.socket.setsockopt_string(zmq.SUBSCRIBE, "")
        self.messages_seen = collections.Counter()
        self.cache = {}
        self.channel_names = []

        # Dastard command
        self.config = config
        self._id_iter = itertools.count()
        self._off_filename = ""
        self.write_lock = asyncio.Lock()


    async def update_messages(self):
        while True:
            try:
                topic, contents = asyncio.create_task(self.get_message())
                
                if topic is None:
                    break
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
        if topic is not None:
            self.cache[topic] = contents
        self.messages_seen[topic] += 1
        return topic, contents

    async def wait_for_message_with_topic(self, target_topic: str, message_count: int, timeout=10.0):
        start_time = time.time()
        while time.time() - start_time < timeout:
            if self.cache.get(target_topic) is not None:
                if self.messages_seen[target_topic] >= message_count:
                    return self.cache[target_topic]
            await asyncio.sleep(0.2)
        raise TimeoutError(f"Timeout waiting for message with topic {target_topic}")

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
        return response

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

    async def request_status(self):
        response = await self.send("SourceControl.SendAllStatus", "dummy")
        return response

    async def start_file(self, ljh22=None, off=None, path=None):
        params = {"Request": "Start", "WriteLJH3": False}
        params.update(self.config.get("writeControl", {}))

        if ljh22 is not None:
            params["WriteLJH22"] = ljh22
        if off is not None:
            params["WriteOFF"] = off
        if path is not None:
            params["Path"] = path

        writing_count = self.messages_seen.get("WRITING", 0)
        print("DEBUG: WriteControl params: ", params)
        response = await self.send("SourceControl.WriteControl", params)

        try:
            contents = await self.wait_for_message_with_topic("WRITING", writing_count + 1)
        except TimeoutError:
            raise DastardError("Timeout waiting for WRITING message")
        if not contents["Active"]:
            raise DastardError(
                f'Response from Dastard RPC should have contents["Active"]=True, but it does not\ncontents:\n{contents}'
            )
        self.off_filename = contents["FilenamePattern"] % ("chan1", "off")
        return self.off_filename

    async def stop_writing(self):
        params = {"Request": "Stop"}
        response = await self.send("SourceControl.WriteControl", params)
        return response

    async def set_experiment_state(self, state):
        params = {"Label": state, "WaitForError": True}
        response = await self.send("SourceControl.SetExperimentStateLabel", params)
        return response

    async def set_triggers(self, full_trigger_state):
        response = await self.send("SourceControl.ConfigureTriggers", full_trigger_state)
        return response

    async def configure_record_lengths(self, npre=None, nsamp=None):
        params = self.config.get("pulseLengths", {})
        if nsamp is not None:
            params["Nsamp"] = nsamp
        if npre is not None:
            params["Npre"] = npre
        response = await self.send("SourceControl.ConfigurePulseLengths", params)
        return response

    async def set_pulse_trigger_all_chans(self, threshold=None, n_monotone=None):
        config = {"ChannelIndices": self.get_channel_indices()}
        config.update(self.config.get("pulseTrigger", {}))
        if threshold is not None:
            config["EdgeMultiLevel"] = threshold
        if n_monotone is not None:
            config["EdgeMultiVerifyNMonotone"] = n_monotone
        response = await self.set_triggers(config)

        #if self.config.get("use_bahama", False):
        #    self.configure_bahama_pulses()
        return response

    async def set_noise_trigger_all_chans(self):
        config = {
            "ChannelIndices": self.get_channel_indices(),
        }
        config.update(self.config.get("noiseTrigger", {}))
        response = await self.set_triggers(config)

        #if self.config.get("use_bahama", False):
        #    self.configure_bahama_noise()
        return response

    async def zero_all_triggers(self):
        config = {
            "ChannelIndices": self.get_channel_indices(),
        }
        response = await self.set_triggers(config)
        return response

    def get_n_channels(self):
        return len(self.get_name_to_number_index())

    def get_channel_indices(self):
        return list(self.get_name_to_number_index().values())

    # dastard channelNames go from chan1 to chanN and err1 to errN
    # we need to map from channelName to channelIndex (0-2N-1)
    def get_name_to_number_index(self):
        nameNumberToIndex = {}
        for i, name in enumerate(self.channel_names):
            if not name.startswith("chan"):
                continue
            nameNumber = int(name[4:])
            nameNumberToIndex[nameNumber] = i
            # for now since we only use this with lancero sources, error for non-odd index
            # if i % 2 != 1:
            #     raise Exception(
            #         "all fb channelIndices on a lancero source are odd, we shouldn't load projectors for even channelIndices")
        return nameNumberToIndex