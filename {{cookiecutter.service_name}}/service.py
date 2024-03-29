# see https://zoo-project.github.io/workshops/2014/first_service.html#f1
import pathlib
import zoo
import yaml
import os
import json
import importlib
import base64
from zoo_calrissian_runner import ExecutionHandler, ZooCalrissianRunner

class CalrissianRunnerExecutionHandler(ExecutionHandler):
    def get_pod_env_vars(self):
        # sets two env vars in the pod launched by Calrissian
        return {"A": "1", "B": "1"}

    def get_pod_node_selector(self):
        return None

    def get_secrets(self):
        username = os.getenv("CR_USERNAME", None)
        password = os.getenv("CR_TOKEN", None)
        registry = os.getenv("CR_ENDPOINT", None)

        auth = base64.b64encode(f"{username}:{password}".encode("utf-8")).decode(
            "utf-8"
        )

        return {
            "auths": {
                registry: {
                    "username": username,
                    "auth": auth,
                },
            }
        }

    def get_additional_parameters(self):
        return {
            "ADES_STAGEOUT_AWS_SERVICEURL": os.getenv("AWS_SERVICE_URL", None),
            "ADES_STAGEOUT_AWS_REGION": os.getenv("AWS_REGION", None),
            "ADES_STAGEOUT_AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID", None),
            "ADES_STAGEOUT_AWS_SECRET_ACCESS_KEY": os.getenv(
                "AWS_SECRET_ACCESS_KEY", None
            ),
            "ADES_STAGEIN_AWS_SERVICEURL": os.getenv("AWS_SERVICE_URL", None),
            "ADES_STAGEIN_AWS_REGION": os.getenv("AWS_REGION", None),
            "ADES_STAGEIN_AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID", None),
            "ADES_STAGEIN_AWS_SECRET_ACCESS_KEY": os.getenv(
                "AWS_SECRET_ACCESS_KEY", None
            ),
            "ADES_STAGEOUT_OUTPUT": os.getenv("ADES_STAGEOUT_OUTPUT", None)
        }

    def handle_outputs(self, log, output, usage_report, tool_logs):
        
        os.makedirs(
            os.path.join(self.conf["main"]["tmpPath"], self.job_id),
            mode=0o777,
            exist_ok=True,
        )
        with open(os.path.join(self.conf["main"]["tmpPath"], self.job_id, "job.log"), "w") as f:
            f.writelines(log)

        with open(
            os.path.join(self.conf["main"]["tmpPath"], self.job_id, "output.json"), "w"
        ) as output_file:
            json.dump(output, output_file, indent=4)

        with open(
            os.path.join(self.conf["main"]["tmpPath"], self.job_id, "usage-report.json"),
            "w",
        ) as usage_report_file:
            json.dump(usage_report, usage_report_file, indent=4)

        aggregated_outputs = {}
        aggregated_outputs = {
            "usage_report": usage_report,
            "outputs": output,
            "log": os.path.join(self.job_id, "job.log"),
        }

        with open(
            os.path.join(self.conf["main"]["tmpPath"], self.job_id, "report.json"), "w"
        ) as report_file:
            json.dump(aggregated_outputs, report_file, indent=4)

        # self.conf["service_logs"] = [
        #     {
        #         "url": f"https://someurl.com/{os.path.basename(tool_log)}",
        #         "title": f"Tool log {os.path.basename(tool_log)}",
        #         "rel": "related",
        #     }
        #     for tool_log in tool_logs
        # ]


def {{cookiecutter.workflow_id |replace("-", "_")  }}(conf, inputs, outputs):

    with open(
        os.path.join(
            pathlib.Path(os.path.realpath(__file__)).parent.absolute(),
            "app-package.cwl",
        ),
        "r",
    ) as stream:
        cwl = yaml.safe_load(stream)

    runner = ZooCalrissianRunner(
        cwl=cwl,
        conf=conf,
        inputs=inputs,
        outputs=outputs,
        execution_handler=CalrissianRunnerExecutionHandler(conf=conf),
    )
    runner._namespace_name=f"{conf['lenv']['Identifier']}-{conf['lenv']['usid']}"
    exit_status = runner.execute()

    if exit_status == zoo.SERVICE_SUCCEEDED:
        # TODO remove hardcoded key StacCatalogUri which is defined in the main.cwl
        # Remove the "stac" output from runner.outputs.outputs["stac"]["value"] in previous phases
        out = {"StacCatalogUri": runner.outputs.outputs["stac"]["value"]["StacCatalogUri"] }
        json_out_string= json.dumps(out, indent=4)
        outputs["stac"]["value"]=json_out_string
        return zoo.SERVICE_SUCCEEDED

    else:
        conf["lenv"]["message"] = zoo._("Execution failed")
        return zoo.SERVICE_FAILED
