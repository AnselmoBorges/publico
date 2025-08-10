import requests
from typing import Iterable, Dict, Any, List
from urllib.parse import urljoin

from metadata.ingestion.api.source import Source
from metadata.ingestion.api.common import WorkflowContext
from metadata.generated.schema.entity.data.pipeline import CreatePipelineRequest, Task
from metadata.generated.schema.type.entityReference import EntityReference

ARM = "https://management.azure.com/"


def _get_azure_token(tenant_id: str, client_id: str, client_secret: str) -> str:
    resp = requests.post(
        f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token",
        data={
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
            "scope": "https://management.azure.com/.default",
        },
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


class ADFSource(Source):
    """
    Custom Source para Azure Data Factory.
    Lê pipelines e activities via ARM API e emite CreatePipelineRequest + tasks
    para o sink metadata-rest.
    """

    def __init__(self, ctx: WorkflowContext):
        super().__init__(ctx)
        opts = self.service_connection.connectionOptions.__root__
        self.sub = opts["subscription_id"]
        self.rg = opts["resource_group"]
        self.factory = opts["factory_name"]
        self.tenant = opts["tenant_id"]
        self.cid = opts["client_id"]
        self.csec = opts["client_secret"]
        self.service_name = opts["service_name"]
        self.project = opts.get("project", "default")

        # Token ARM
        self.token = _get_azure_token(self.tenant, self.cid, self.csec)
        self.sess = requests.Session()
        self.sess.headers.update({"Authorization": f"Bearer {self.token}"})
        self.api_version = "2018-06-01"

    def prepare(self):
        return

    def _arm(self, path: str, params: Dict[str, Any] = None) -> Dict[str, Any]:
        url = urljoin(ARM, path)
        p = {"api-version": self.api_version}
        if params:
            p.update(params)
        r = self.sess.get(url, params=p, timeout=60)
        r.raise_for_status()
        return r.json()

    def _list_pipelines(self) -> List[Dict[str, Any]]:
        path = (
            f"/subscriptions/{self.sub}/resourceGroups/{self.rg}"
            f"/providers/Microsoft.DataFactory/factories/{self.factory}/pipelines"
        )
        data = self._arm(path)
        return data.get("value", [])

    def _build_tasks(self, activities: List[Dict[str, Any]]) -> List[Task]:
        tasks: List[Task] = []
        for act in activities or []:
            name = act.get("name")
            act_type = act.get("type")
            deps = []
            if act_type == "Copy":
                tp = act.get("typeProperties", {})
                sink = (tp.get("sink", {}) or {}).get("type")
                if sink:
                    deps.append(sink)
            tasks.append(
                Task(
                    name=name or "activity",
                    taskType=act_type or "Activity",
                    downstreamTasks=deps or None,
                )
            )
        return tasks

    def next_record(self) -> Iterable[CreatePipelineRequest]:
        for p in self._list_pipelines():
            name = p.get("name")
            props = p.get("properties", {})
            acts = props.get("activities", [])
            tasks = self._build_tasks(acts)

            yield CreatePipelineRequest(
                name=name,
                displayName=name,
                description=props.get("description"),
                service=EntityReference(id=None, type="pipelineService", name=self.service_name),
                tasks=tasks,
                sourceUrl=p.get("id") or "",
            )

    def close(self):
        self.sess.close()
