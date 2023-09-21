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
        try:
            with open('/assets/pod_env_vars.yaml', 'r') as file:
                additional_params = yaml.safe_load(file)
            return additional_params
        # if file does not exist
        except FileNotFoundError:
            return {}
        # if file is empty
        except yaml.YAMLError:
            return {}
        # if file is not yaml
        except yaml.scanner.ScannerError:
            return {}
        except:
            return {}   

    def get_pod_node_selector(self):
        try:
            with open('/assets/pod_nodeselectors.yaml', 'r') as file:
                additional_params = yaml.safe_load(file)
            return additional_params
        # if file does not exist
        except FileNotFoundError:
            return {}
        # if file is empty
        except yaml.YAMLError:
            return {}
        # if file is not yaml
        except yaml.scanner.ScannerError:
            return {}
        except:
            return {}  

    def get_secrets(self):
        try:
            with open('/assets/pod_imagePullSecrets.yaml', 'r') as file:
                additional_params = yaml.safe_load(file)
            return additional_params
        # if file does not exist
        except FileNotFoundError:
            return {}
        # if file is empty
        except yaml.YAMLError:
            return {}
        # if file is not yaml
        except yaml.scanner.ScannerError:
            return {}
        except:
            return {}        

    def get_additional_parameters(self):

        try:
            with open('/assets/additional_inputs.yaml', 'r') as file:
                additional_params = yaml.safe_load(file)
            return additional_params
        # if file does not exist
        except FileNotFoundError:
            return {}
        # if file is empty
        except yaml.YAMLError:
            return {}
        # if file is not yaml
        except yaml.scanner.ScannerError:
            return {}
        except:
            return {}


    def handle_outputs(self, log, output, usage_report, tool_logs):
        
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

    # we are changing the working directory to store the outputs
    # in a directory dedicated to this execution
    working_dir=os.path.join(conf["main"]["tmpPath"], runner._namespace_name)
    os.makedirs(
            working_dir,
            mode=0o777,
            exist_ok=True,
    )
    os.chdir(working_dir)

    exit_status = runner.execute()

    if exit_status == zoo.SERVICE_SUCCEEDED:
        out = {"StacCatalogUri": runner.outputs.outputs["stac"]["value"]["StacCatalogUri"] }
        json_out_string= json.dumps(out, indent=4)
        outputs["stac"]["value"]=json_out_string
        return zoo.SERVICE_SUCCEEDED

    else:
        conf["lenv"]["message"] = zoo._("Execution failed")
        return zoo.SERVICE_FAILED
