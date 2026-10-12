from hatchling.metadata.plugin.interface import MetadataHookInterface


class CustomMetadataHook(MetadataHookInterface):
    def update(self, metadata):
        metadata["optional-dependencies"] = {
            "mqtt": ["paho-mqtt~=2.1"],
            "secrets": [f"flechtctl=={metadata['version']}"],
        }
