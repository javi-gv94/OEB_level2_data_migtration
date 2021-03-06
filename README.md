# OEB_workflows_data_migration (BETA version)
## Description
Application used by community managers to migrate results of a benchmarking workflow from [Virtual Research Environment](https://openebench.bsc.es/vre) to [OpenEBench](https://openebench.bsc.es) database. It takes the minimal datasets from the 'consolidated results' from the workflow, adds the rest of metadata to validate against the [Benchmarking Data Model](https://github.com/inab/benchmarking-data-model), and the required OEB keys, builds the neccesary TestActions, and finally pushes them to OpenEBench temporary database.

## Prerequisites for moving workflow results from VRE to OEB
In order to use the migration tool, some requirements need to be fulfilled:
* The benchmarking event, challenges, metrics, and input/reference datasets that the results refer to should already be registered in OpenEBench and have official OEB identifiers.
* IDs of challenges and metrics used in the workflow should be annotated in the correspondent OEB objects (in the *_metadata:level_2* field) so that the can be mapped to the registered OEB elements.
* The tool that computed the input file' predictions should also be registered in OpenEBench.
* The 'consolidated results' file should come from a pipeline that follows the OpenEBench Benchmarking Workflows Standards.
(If any of these requirements is not satisfied, a form should be provided so that the manager or developer can 'inaugurate' the required object in OEB)
* **NOTE:** this tool just moves VRE datasets to OEB database, it does NOT update the reference aggregation data that VRE workflows use. In order to move official OEB aggregation datasets to a VRE workflow, copy them manually to the corresponding reference directory *(/gpfs/VRE/public/aggreggation/<workflow_name>)* 

## Parameters

parser.add_argument("-i", "--consolidated_oeb_data", help="json file with the aggregation of datasets coming from a OEB VRE workflow \
                                                                    (data_type:consolidated_benchmark_dataset)", required=True)
    parser.add_argument("-v", "--data_visibility", help="visibility of the datasets associated to the participant's run, according \
                                                                    to the benchmarking data model - 'enum': [ 'public', 'community', 'challenge', 'participant' ]", required=True)
    parser.add_argument("-be", "--benchmarking_event_id", help="benchmarking event id that corresponds to the executed workflow \
                                                                    - should be an official OEB id stored in the DB", required=True)
    parser.add_argument("-f", "--participant_file", help="location of the file that was uploaded by the participant \
                                                                    - should be a FS path or a remote DOI", required=True)
    parser.add_argument("-com", "--community_id", help=" official id of the community that corresponds to the execution \
                                                                    - should already be registered in OEB", required=True)
    parser.add_argument("-t", "--tool_id", help=" official id of the tool that made the predictions which were used as input \
                                                    , if tool is not registered in OEB, should provide the access to a form to register it", required=True)
    parser.add_argument("-vers", "--data_version", help=" version for annotating metadata of all datasets coming from this execution \
                                                    not required - default: unknown", required=False, default="unknown")  

## Usage

Edit the [run.sh](./run.sh) file with your own parameters. E.g.:
.py3env/bin/python push_data_to_oeb.py -i config.json -db ./db_config.yaml -tk <YOUR-KEYCLOACK-TOKEN>
