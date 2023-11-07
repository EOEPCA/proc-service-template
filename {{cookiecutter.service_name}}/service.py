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

    def local_get_file(self,fileName):
        """
        Read and load a yaml file

        :param fileName the yaml file to load
        """
        try:
            with open(fileName, 'r') as file:
                data = yaml.safe_load(file)
            return data
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

    def get_pod_env_vars(self):
        
        return self.conf.get("pod_env_vars", {})

    def get_pod_node_selector(self):
        
        return self.conf.get("pod_node_selector", {})
    
    def get_secrets(self):
        
        return self.local_get_file('/assets/pod_imagePullSecrets.yaml')

    def get_additional_parameters(self):
        
        return self.conf.get("additional_parameters", {})

    def handle_outputs(self, log, output, usage_report, tool_logs):
        """
        Handle the output files of the execution.

        :param log: The application log file of the execution.
        :param output: The output file of the execution.
        :param usage_report: The performance metrics file for the application during execution.
        :param tool_logs: A list of paths to individual workflow step logs from the execution.

        """
        # link element to add to the statusInfo
        servicesLogs=[
            {
                "url": f"{self.conf['main']['tmpUrl']}/{self.conf['lenv']['Identifier']}-{self.conf['lenv']['usid']}/{os.path.basename(tool_log)}",
                "title": f"Tool log {os.path.basename(tool_log)}",
                "rel": "related",
            }
            for tool_log in tool_logs
        ]
        for i in range(len(servicesLogs)):
            okeys=["url","title","rel"]
            keys=["url","title","rel"]
            if i>0:
                for j in range(len(keys)):
                    keys[j]=keys[j]+"_"+str(i)
            if "service_logs" not in self.conf:
                self.conf["service_logs"]={}
            for j in range(len(keys)):
                self.conf["service_logs"][keys[j]]=servicesLogs[i][okeys[j]]
        self.conf["service_logs"]["length"]=str(len(servicesLogs))


def {{cookiecutter.workflow_id |replace("-", "_")  }}(conf, inputs, outputs):

    execution_handler = CalrissianRunnerExecutionHandler(conf=conf)
    print(execution_handler.get_additional_parameters())
    
    print(f"conf {conf.keys()}")

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
        execution_handler=execution_handler,
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
