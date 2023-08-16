from dagster import ConfigurableResource
import duckdb
import os
from pydantic import PrivateAttr


class LocalFileStorage(ConfigurableResource):
    dir: str

    def setup_for_execution(self, context) -> None:
        os.makedirs(self.dir, exist_ok=True)

    def write(self, filename, data):
        dir_path = f"{self.dir}/{os.path.dirname(filename)}"
        os.makedirs(dir_path, exist_ok=True)

        with open(f"{self.dir}/{filename}", "wb") as f:
            f.write(data.read())

    def read(self, filename):
        with open(f"{self.dir}/{filename}", "rb") as image_file:
            return image_file.read()


class Database(ConfigurableResource):
    path: str

    def query(self, body: str):
        with duckdb.connect(self.path) as conn:
            return conn.query(body).to_df()


class EmailClient:
    def __init__(self, server_token):
        pass

    def send(self, sender_email, recipient_email, template_id, template, attachments):
        print(sender_email, recipient_email, template_id, template)


class EmailService(ConfigurableResource):
    template_id: int
    sender_email: str
    server_token: str
    _client: EmailClient = PrivateAttr()

    def setup_for_execution(self, context) -> None:
        self._client = EmailClient(server_token=self.server_token)

    def send(self, recipient_email, template, attachments):
        self._client.send(
            sender_email=self.sender_email,
            recipient_email=recipient_email,
            template_id=self.template_id,
            template=template,
            attachments=attachments,
        )
