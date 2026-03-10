import asyncio
import os
from caproto.server import (
    PVGroup,
    ioc_arg_parser,
    pvproperty,
    run,
)
from .dastard_model import AsyncDastardModel
from caproto import ChannelType
import numpy as np
import tomllib

class TESIOC(PVGroup):
    """
    An IOC to run a TES detector through Dastard
    """
    alive = pvproperty(value=False, dtype=bool, name="CONNECTED")
    running = pvproperty(value=False, dtype=bool, name="RUNNING")
    writing = pvproperty(value=False, dtype=bool, name="WRITING")
    source = pvproperty(value="None", dtype=ChannelType.STRING, name="SOURCE")
    filename = pvproperty(value="", dtype=ChannelType.STRING, name="FILENAME")
    write_ljh = pvproperty(value=False, dtype=bool, name="WRITE_LJH")
    write_off = pvproperty(value=False, dtype=bool, name="WRITE_OFF")
    projectors = pvproperty(value=False, dtype=bool, name="PROJECTORS")
    state = pvproperty(value="", dtype=ChannelType.STRING, name="STATE")
    n_channels = pvproperty(value=0, dtype=int, name="NCHAN")
    n_disabled = pvproperty(value=0, dtype=int, name="NDISABLED")
    trigger_rate = pvproperty(value=0, dtype=int, name="TRIGGERRATE")

    n_samples = pvproperty(value=0, dtype=int, name="NSAMPLES")
    n_presamples = pvproperty(value=0, dtype=int, name="NPRESAMPLES")

    file_start = pvproperty(value=0, dtype=int, name="FILE_START")
    file_end = pvproperty(value=0, dtype=int, name="FILE_END")
    scan_start = pvproperty(value=0, dtype=int, name="SCAN_START")
    scan_end = pvproperty(value=0, dtype=int, name="SCAN_END")
    acquire = pvproperty(value=0, dtype=float, name="ACQUIRE")

    set_pulse_trigger = pvproperty(value=0, dtype=int, name="SET_PULSE_TRIGGER")
    set_noise_trigger = pvproperty(value=0, dtype=int, name="SET_NOISE_TRIGGER")

    def __init__(self, *args, config={},**kwargs):
        super().__init__(*args, **kwargs)
        host = config.get("host", "dastard")
        port = config.get("port", 5500)
        self.dastard_model = AsyncDastardModel(host=host, port=port, config=config)
        self._off_filename = ""
        self._channel_names = []

    async def _handle_message(self, topic, contents):
        """
        Handle ZMQ messages from Dastard. Mirrors handle_message in
        tes_scan_server.dastard_client.DastardClient.
        """
        if topic not in ["TRIGGERRATE", "EXTERNALTRIGGER", "ALIVE"]:
            print(f"DEBUG: Topic: {topic}, Contents: {contents}")
        if topic == "ALIVE":
            running = contents.get("Running", False)
            if self.running.value != running:
                await self.running.write(running)

        elif topic == "STATUS":
            print("DEBUG: Got Status Message")
            if self.running.value:
                print("DEBUG: Running, updating statuses")
                source = contents.get("SourceName", "None")
                nsamples = contents.get("Nsamples", 0)
                npresamp = contents.get("Npresamp", 0)
                nchannels = contents.get("Nchannels", 0)
                projectors = contents.get("ChannelsWithProjectors", [])
                has_projectors = bool(projectors)

                tasks = []
                if self.source.value != source:
                    tasks.append(self.source.write(source))
                
                if self.projectors.value != has_projectors:
                    tasks.append(self.projectors.write(has_projectors))
                if self.n_samples.value != nsamples:
                    tasks.append(self.n_samples.write(nsamples))
                if self.n_presamples.value != npresamp:
                    tasks.append(self.n_presamples.write(npresamp))
                if self.n_channels.value != nchannels:
                    tasks.append(self.n_channels.write(nchannels))
                await asyncio.gather(*tasks)

        elif topic == "CHANNELNAMES":
            if self.dastard_model.channel_names != contents:
                self.dastard_model.channel_names = contents

        elif topic == "WRITING":
            writing = contents.get("Active", False)
            if self.writing.value != writing:
                await self.writing.write(writing)
            if writing:
                filename = contents["FilenamePattern"] % ("chan1", "off")
                if self._off_filename != filename:
                    self._off_filename = filename
                    await self.filename.write(filename)
            elif self._off_filename != "":
                self._off_filename = ""
                await self.filename.write("")

        elif topic == "TRIGGER":
            trigger_types = [
                "AutoTrigger",
                "LevelTrigger",
                "EdgeTrigger",
                "EdgeMulti",
                "EdgeMultiNoise",
            ]
            n_chans = len(self._channel_names)
            for item in contents:
                if not any(item.get(t, False) for t in trigger_types):
                    if len(item["ChannelIndices"]) < n_chans:
                        await self.n_disabled.write(len(item["ChannelIndices"]))
        elif topic == "TRIGGERRATE":
            counts = np.sum(contents.get("CountsSeen", [0]))
            duration = contents.get("Duration", 0) / 1e9
            await self.trigger_rate.write(int(counts/duration))

    @alive.startup
    async def alive(self, instance, async_lib):
        try:
            topic, contents = await asyncio.wait_for(
                self.dastard_model.get_message(),
                timeout=10.0,
            )
            await self._handle_message(topic, contents)
            alive = True
        except asyncio.TimeoutError:
            alive = False
        except Exception:
            alive = False
        await instance.write(alive)

        if alive:
            await self.dastard_model.request_status()

        while True:
            try:
                topic, contents = await asyncio.wait_for(
                    self.dastard_model.get_message(),
                    timeout=10.0,
                )
                await self._handle_message(topic, contents)
                alive = True
            except asyncio.TimeoutError:
                alive = False
            except Exception:
                alive = False
            await instance.write(alive)
    
    @set_pulse_trigger.putter
    async def set_pulse_trigger(self, instance, value):
        await instance.write(1, verify_value=False)
        await self.dastard_model.configure_record_lengths()
        await self.dastard_model.zero_all_triggers()
        await self.dastard_model.set_pulse_trigger_all_chans()
        return 0

    @set_noise_trigger.putter
    async def set_noise_trigger(self, instance, value):
        await instance.write(1, verify_value=False)
        await self.dastard_model.configure_record_lengths()
        await self.dastard_model.zero_all_triggers()
        await self.dastard_model.set_noise_trigger_all_chans()
        return 0

    @write_ljh.startup
    async def write_ljh(self, instance, async_lib):
        write_ljh = self.dastard_model.config.get("writeControl", {}).get("WriteLJH22", False)
        await self.write_ljh.write(write_ljh)

    @write_off.startup
    async def write_off(self, instance, async_lib):
        write_off = self.dastard_model.config.get("writeControl", {}).get("WriteOFF", False)
        await self.write_off.write(write_off)

    @file_start.putter
    async def file_start(self, instance, value):
        await instance.write(1, verify_value=False)
        try:
            write_ljh = self.write_ljh.value == "On"
            write_off = self.write_off.value == "On"
            filename = await self.dastard_model.start_file(ljh22=write_ljh, off=write_off)
            await self.filename.write(filename)
            return 0
        except Exception as e:
            await self.filename.write("")
            return 0

    @file_end.putter
    async def file_end(self, instance, value):
        await instance.write(1, verify_value=False)
        await self.dastard_model.stop_writing()
        return 0

def main():
    ioc_options, run_options = ioc_arg_parser(
        default_prefix="XF:07ID-ES{{UCAL}}:",
        desc="TES IOC",
        supported_async_libs=("asyncio",),
    )

    config_path = os.path.join(os.path.dirname(__file__), "config.toml")
    with open(config_path, "rb") as f:
        config = tomllib.load(f)
    config = config.get("dastard", {})
    ioc = TESIOC(config=config, **ioc_options)
    run(ioc.pvdb, **run_options)    
    
if __name__ == "__main__":
    main()
