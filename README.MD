# Requirements

Virtual environment or conda environment with pyincore installed

# Running a tool locally

./run.sh --analysis pyincore.analyses.buildingdamage.buildingdamage:BuildingDamage --token ~/.incore/.incoretoken --result_name joplin --service_url https://dev.in-core.org --buildings 5fa0b132cc6848728b66948d --hazard_type tornado
--hazard_id 5dfa32bbc0601200080893fb --damage_interval_keys DS_0 DS_1 DS_2 DS_3 --dfr3_mapping_set 5e8e3a21eaa8b80001f04f1c

# Create DataWolf tools on IN-CORE Cluster

There is a script, create_tools_studio.py that will generate workflow tools for each pyincore analysis. There are
four environment variables that allow you to configure tool creation. They are:

```
DATAWOLF_HOST - by default, this is localhost:8888. Change this to where DataWolf is deployed. (e.g. https://tools.in-core.org)

DATAWOLF_USER - this is the DataWolf person ID of the person creating the tools on DataWolf.

KUBE_TOOLS - by default, this is false. You will want to set this to true to create tools for the kubernetes executor

USE_AUTH - by default, this is false. You will want to set this to true for communicating with an authenticated
DataWolf instance.
```

See env-example for some example values. If USE_AUTH is true, then you will need to create an incore token in
inside USER_HOME/.incore. The tool script will use this token to communicate with the authenticated DataWolf instance
when creating tools.

Once the token is created. Activate your pyincore Conda environment. The tool script will use this environment to find
tools to create. Once your environment is activated and the other steps above are completed, run the following command:

```
python create_tools_studio.py
```

The final output should be how many tools were created. By default, the tools will be created using the Class name
prepended with "Studio-Kube-".
