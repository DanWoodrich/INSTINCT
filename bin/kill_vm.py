# based on https://stackoverflow.com/q/52748332/321772
import json
import logging
import requests

# get the token
r = json.loads(
    requests.get("http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token",
                 headers={"Metadata-Flavor": "Google"})
        .text)

token = r["access_token"]

# get instance metadata
# based on https://cloud.google.com/compute/docs/storing-retrieving-metadata
project_id = requests.get("http://metadata.google.internal/computeMetadata/v1/project/project-id",
                          headers={"Metadata-Flavor": "Google"}).text

name = requests.get("http://metadata.google.internal/computeMetadata/v1/instance/name",
                    headers={"Metadata-Flavor": "Google"}).text

zone_long = requests.get("http://metadata.google.internal/computeMetadata/v1/instance/zone",
                         headers={"Metadata-Flavor": "Google"}).text
zone = zone_long.split("/")[-1]

# shut ourselves down
logging.info("Calling API to delete this VM, {zone}/{name}".format(zone=zone, name=name))

requests.delete("https://www.googleapis.com/compute/v1/projects/{project_id}/zones/{zone}/instances/{name}"
                .format(project_id=project_id, zone=zone, name=name),
                headers={"Authorization": "Bearer {token}".format(token=token)})
