import grpc
import requests
import threading
import io
import avro.schema
import avro.io
import time
import certifi
import json
import os
import importlib.util
import sys
import logging

from bs4 import BeautifulSoup

# import the stubs dynamically
local_dir = os.path.dirname(__file__)

pb2_spec = importlib.util.spec_from_file_location(
    "pubsub_api_pb2", f"{local_dir}/pubsub_api_pb2.py"
)
pb2 = importlib.util.module_from_spec(pb2_spec)
sys.modules["pubsub_api_pb2"] = pb2
pb2_spec.loader.exec_module(pb2)

pb2_grpc_spec = importlib.util.spec_from_file_location(
    "pubsub_api_pb2_grpc", f"{local_dir}/pubsub_api_pb2_grpc.py"
)
pb2_grpc = importlib.util.module_from_spec(pb2_grpc_spec)
sys.modules["pubsub_api_pb2_grpc"] = pb2_grpc
pb2_grpc_spec.loader.exec_module(pb2_grpc)


class PubSubAPI:
    def __init__(self, logger=None):
        self.sf_username = os.getenv("SALESFORCE_USER", "env-var-empty")
        self.sf_password = os.getenv("SALESFORCE_PASSWORD", "env-var-empty")
        self.sf_security_token = os.getenv("SALESFORCE_SECURITY_TOKEN", "env-var-empty")
        self.sf_domain_url = os.getenv("SALESFORCE_DOMAIN_URL")
        self.sf_soap_path = os.getenv("SALESFORCE_SOAP_PATH", "/services/Soap/u/59.0/")
        self.sf_channel_host_port = os.getenv(
            "SALESFORCE_CHANNEL_HOST_PORT", "api.pubsub.salesforce.com:7443"
        )
        self.sf_topic = os.getenv("SALESFORCE_TOPIC", "env-var-empty")
        self.sf_replay_preset = os.getenv("SALESFORCE_REPLAY_PRESET", "EARLIEST")
        self.sf_replay_id = os.getenv("SALESFORCE_REPLAY_ID", "1")

        if self.sf_replay_preset == "EARLIEST":
            self.sf_replay_preset = pb2.ReplayPreset.EARLIEST
            self.sf_replay_id = 1
        elif self.sf_replay_preset == "LATEST":
            self.sf_replay_preset = pb2.ReplayPreset.LATEST
            self.sf_replay_id = 1
        elif self.sf_replay_preset == "CUSTOM":
            self.sf_replay_preset = pb2.ReplayPreset.CUSTOM
        else:
            # invalid sf_replay_preset value (did not match any enum)
            self.sf_replay_preset = pb2.ReplayPreset.EARLIEST
            self.sf_replay_id = 1

        # if a logger was not passed in, then set one up for logging
        self.logger = logger
        if self.logger is None:
            self.logger = logging.getLogger("PubSubAPI")
            self.logger.setLevel(logging.DEBUG)
            stdoutHandler = logging.StreamHandler(stream=sys.stdout)
            fmt = logging.Formatter(
                "%(name)s | %(asctime)s | %(levelname)s | %(message)s"
            )
            stdoutHandler.setFormatter(fmt)
            stdoutHandler.setLevel(logging.DEBUG)
            self.logger.addHandler(stdoutHandler)

        self.semaphore = threading.Semaphore(1)
        self.latest_replay_id = None
        self.auth_meta_data = None

    def getAuthMetadata(self):
        if self.auth_meta_data is None:
            return self.login()
        return self.auth_meta_data

    def login(self):
        url = f"""{self.sf_domain_url}{self.sf_soap_path}"""
        xml = f"""<soapenv:Envelope xmlns:soapenv='http://schemas.xmlsoap.org/soap/envelope/'
        xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance'
        xmlns:urn='urn:partner.soap.sforce.com'><soapenv:Body>
        <urn:login>
        <urn:username><![CDATA[{self.sf_username}]]></urn:username>
        <urn:password><![CDATA[{self.sf_password}{self.sf_security_token}]]></urn:password>
        </urn:login></soapenv:Body></soapenv:Envelope>"""
        headers = {"content-type": "text/xml", "SOAPAction": "login"}

        self.logger.info("Logging into Salesforce")
        res = requests.post(url, data=xml, headers=headers)

        xml_object = BeautifulSoup(res.content, "xml")
        session_id_node = xml_object.find("sessionId")
        if session_id_node is None:
            self.logger.error("session_id is None")
            self.logger.error(xml_object.prettify())
            raise Exception("No Session ID found in returned XML")
        session_id = session_id_node.text
        tenant_id_node = xml_object.find("organizationId")
        if tenant_id_node is None:
            self.logger.error("tenant_id_node is None")
            self.logger.error(xml_object.prettify())
            raise Exception("No Tenant ID (organizationId) found in returned XML")
        tenant_id = tenant_id_node.text

        self.auth_meta_data = (
            ("accesstoken", session_id),
            ("instanceurl", self.sf_domain_url),
            ("tenantid", tenant_id),
        )

        return self.getAuthMetadata()

    def fetch(self):
        # semaphore = threading.Semaphore(1)

        with open(certifi.where(), "rb") as f:
            creds = grpc.ssl_channel_credentials(f.read())
        with grpc.secure_channel(self.sf_channel_host_port, creds) as channel:
            authmetadata = self.getAuthMetadata()
            stub = pb2_grpc.PubSubStub(channel)

            def fetchReqStream(my_semaphore, sf_topic, sf_replay_reset, sf_replay_id):
                while True:
                    my_semaphore.acquire()
                    yield pb2.FetchRequest(
                        topic_name=sf_topic,
                        replay_preset=sf_replay_reset,
                        num_requested=sf_replay_id,
                    )

            self.logger.debug(
                "Subscribing to Salesforce topic {}".format(self.sf_topic)
            )
            authmetadata = self.getAuthMetadata()
            substream = stub.Subscribe(
                fetchReqStream(
                    self.semaphore,
                    self.sf_topic,
                    self.sf_replay_preset,
                    self.sf_replay_id,
                ),
                metadata=authmetadata,
            )

            for data in substream:
                rpc_id = data.rpc_id
                self.logger.info("fetch: rpc_id = {}".format(rpc_id))
                self.semaphore.release()
                if data.events:
                    # if all requested events are delivered,
                    # release the semaphore so that a new FetchRequest gets sent
                    if data.pending_num_requested == 0:
                        self.semaphore.release()

                    self.logger.debug(
                        "fetch: Number of events received: {}".format(len(data.events))
                    )
                    self.logger.debug(
                        "fetch: Event ID {}".format(data.events[0].event.id)
                    )
                    payloadbytes = data.events[0].event.payload
                    schemaid = data.events[0].event.schema_id
                    schema = stub.GetSchema(
                        pb2.SchemaRequest(schema_id=schemaid), metadata=authmetadata
                    ).schema_json
                    decoded = self.decode(schema, payloadbytes)
                    self.logger.debug(
                        "fetch: Got an event! {}".format(json.dumps(decoded, indent=2))
                    )
                    yield {
                        "latest_replay_id": self.latest_replay_id,
                        "rpc_id": data.rpc_id,
                        "events": data.events,
                        "decoded": decoded,
                    }
                else:
                    decoded = None
                    self.logger.info(
                        "No data but the Salesforce subscription is still active."
                    )
                    time.sleep(1)
                self.latest_replay_id = data.latest_replay_id

    def decode(self, schema, payload):
        schema = avro.schema.parse(schema)
        buf = io.BytesIO(payload)
        decoder = avro.io.BinaryDecoder(buf)
        reader = avro.io.DatumReader(schema)
        ret = reader.read(decoder)
        return ret
