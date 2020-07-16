import logging
import sys
import os
from datetime import datetime, timezone
import json
from process.benchmarking_dataset import benchmarking_dataset


class assessment():

    def __init__(self):

        logging.basicConfig(level=logging.INFO)

    def build_assessment_datasets(self, response, assessment_datasets, data_visibility, participant_data, community_id, tool_id, version, contacts):

        logging.info(
            "\n\t==================================\n\t3. Processing assessment datasets\n\t==================================\n")

        valid_assessment_datasets = []
        for dataset in assessment_datasets:

            sys.stdout.write('Building object "' +
                             str(dataset["_id"]) + '"...\n')
            # initialize new dataset object
            valid_data = {
                "_id": dataset["_id"],
                "type": "assessment"
            }

            # add dataset visibility
            valid_data["visibility"] = data_visibility

            # add name and description, if workflow did not provide them
            if "name" not in dataset:
                valid_data["name"] = "Assessment of metric '" + dataset["metrics"]["metric_id"] + \
                    "' in " + dataset["participant_id"] + " participant"
            else:
                valid_data["name"] = dataset["name"]
            if "description" not in dataset:
                valid_data["description"] = "Assessment dataset of applying metric '" + \
                    dataset["metrics"]["metric_id"] + "' in " + \
                    dataset["participant_id"] + " participant"
            else:
                valid_data["description"] = dataset["description"]

            # replace the datasets challenge identifiers with the official OEB ids, which should already be defined in the database.

            data = response["data"]["getChallenges"]

            oeb_challenges = {}
            for challenge in data:
                oeb_challenges[challenge["_metadata"]
                               ["level_2:challenge_id"]] = challenge["_id"]

            # replace dataset related challenges with oeb challenge ids
            execution_challenges = []
            try:
                execution_challenges.append(
                    oeb_challenges[dataset["challenge_id"]])
            except:
                logging.info("No challenges associated to " +
                             dataset["challenge_id"] + " in OEB. Please contact OpenEBench support for information about how to open a new challenge")
                logging.info(
                    dataset["_id"] + " not processed, skipping to next assessment element...")
                continue
                # sys.exit()

            valid_data["challenge_ids"] = execution_challenges

            # select metrics_reference datasets used used in the challenges
            rel_oeb_datasets = set()
            for challenge in [item for item in data if item["_id"] in valid_data["challenge_ids"]]:
                for ref_data in challenge["datasets"]:
                    rel_oeb_datasets.add(ref_data["_id"])

            # add the participant data that corresponds to this assessment
            rel_oeb_datasets.add(participant_data["_id"])

            # add data registration dates
            valid_data["dates"] = {
                "creation": str(datetime.now(timezone.utc).replace(microsecond=0).isoformat()),
                "modification": str(datetime.now(timezone.utc).replace(microsecond=0).isoformat())
            }

            # add assessment metrics values, as inline data
            metric_value = dataset["metrics"]["value"]
            error_value = dataset["metrics"]["stderr"]

            valid_data["datalink"] = {"inline_data": {
                "value": metric_value, "error": error_value}}

            # add Benchmarking Data Model Schema Location
            valid_data["_schema"] = "https://www.elixir-europe.org/excelerate/WP2/json-schemas/1.0/Dataset"

            # add OEB id for the community
            valid_data["community_ids"] = [community_id]

            # add dataset dependencies: metric id, tool and reference datasets
            list_oeb_datasets = []
            for dataset_id in rel_oeb_datasets:
                list_oeb_datasets.append({
                    "dataset_id": dataset_id
                })

            for metric in response["data"]["getMetrics"]:

                if metric["_metadata"] != None and metric["_metadata"]["level_2:metric_id"] == dataset["metrics"]["metric_id"]:
                    metric_id = metric["_id"]

            try:
                metric_id
            except:
                logging.fatal("No metric associated to " + dataset["metrics"]["metric_id"] +
                              " in OEB. Please contact OpenEBench support for information about how to register your own metrics")
                sys.exit()

            valid_data["depends_on"] = {
                "tool_id": tool_id,
                "rel_dataset_ids": list_oeb_datasets,
                "metrics_id": metric_id
            }

            # add data version
            valid_data["version"] = str(version)

            # add dataset contacts ids
            valid_data["dataset_contact_ids"] = [contact["_id"]
                                                 for contact in response["data"]["getContacts"] if contact["email"][0] in contacts]

            sys.stdout.write('Processed "' + str(dataset["_id"]) + '"...\n')

            valid_assessment_datasets.append(valid_data)

        return valid_assessment_datasets

    def build_metrics_events(self, response, assessment_datasets, tool_id, contacts):

        logging.info(
            "\n\t==================================\n\t4. Generating Metrics Events\n\t==================================\n")

        # initialize the array of test events
        metrics_events = []

        # an  new event object will be created for each of the previously generated assessment datasets
        for dataset in assessment_datasets:

            event_id = rchop(dataset["_id"], "_A") + "_MetricsEvent"
            event = {
                "_id": event_id,
                "_schema": "https://www.elixir-europe.org/excelerate/WP2/json-schemas/1.0/TestAction",
                "action_type": "MetricsEvent",
            }

            sys.stdout.write(
                'Building Event object for assessment "' + str(event["_id"]) + '"...\n')

            # add id of tool for the test event
            event["tool_id"] = tool_id

            # add the oeb official id for the challenge (which is already in the assessment dataset)
            event["challenge_id"] = dataset["challenge_ids"][0]

            # append incoming and outgoing datasets
            involved_data = []

            # include the incomning datasets related to the event
            for data_id in dataset["depends_on"]["rel_dataset_ids"]:
                involved_data.append({
                    "dataset_id": data_id["dataset_id"],
                    "role": "incoming"
                })
            # ad the outgoing assessment data
            involved_data.append({
                "dataset_id": dataset["_id"],
                "role": "outgoing"
            })

            event["involved_datasets"] = involved_data
            # add data registration dates
            event["dates"] = {
                "creation": str(datetime.now(timezone.utc).replace(microsecond=0).isoformat()),
                "reception": str(datetime.now(timezone.utc).replace(microsecond=0).isoformat())
            }

            # add dataset contacts ids
            event["test_contact_ids"] = [contact["_id"] for contact in response["data"]
                                         ["getContacts"] if contact["email"][0] in contacts]

            metrics_events.append(event)

        return metrics_events


def rchop(s, sub):
    return s[:-len(sub)] if s.endswith(sub) else s
