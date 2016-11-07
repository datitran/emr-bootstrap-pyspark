import boto3
import yaml
import time
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)


class EMRLoader(object):
    def __init__(self, aws_access_key, aws_secret_access_key, region_name, cluster_name, instance_count, key_name,
                 log_uri, software_version, script_uri):
        self.instance_count = instance_count
        self.key_name = key_name
        self.cluster_name = cluster_name
        self.aws_access_key = aws_access_key
        self.aws_secret_access_key = aws_secret_access_key
        self.region_name = region_name
        self.log_uri = log_uri
        self.software_version = software_version
        self.script_uri = script_uri

    def emr_client(self):
        client = boto3.client("emr",
                              aws_access_key_id=self.aws_access_key,
                              aws_secret_access_key=self.aws_secret_access_key,
                              region_name=self.region_name)
        return client

    def load_cluster(self):
        response = self.emr_client().run_job_flow(
            Name=self.cluster_name,
            LogUri=self.log_uri,
            ReleaseLabel=self.software_version,
            Instances={
                'MasterInstanceType': 'm3.xlarge',
                'SlaveInstanceType': 'm3.xlarge',
                'InstanceCount': self.instance_count,
                'KeepJobFlowAliveWhenNoSteps': True,
                'TerminationProtected': False,
                'Ec2KeyName': self.key_name
            },
            Applications=[
                {
                    'Name': 'Spark'
                }
            ],
            BootstrapActions=[
                {
                    'Name': 'Install Conda',
                    'ScriptBootstrapAction': {
                        'Path': '{script_uri}bootstrap_actions.sh'.format(script_uri=self.script_uri),
                    }
                },
            ],
            VisibleToAllUsers=True,
            JobFlowRole='EMR_EC2_DefaultRole',
            ServiceRole='EMR_DefaultRole'
        )
        logger.info(response)
        return response

    def add_step(self, job_flow_id, master_dns):
        response = self.emr_client().add_job_flow_steps(
            JobFlowId=job_flow_id,
            Steps=[
                {
                    'Name': 'setup - copy files',
                    'ActionOnFailure': 'CANCEL_AND_WAIT',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': ['aws', 's3', 'cp',
                                 '{script_uri}pyspark_quick_setup.sh'.format(script_uri=self.script_uri),
                                 '/home/hadoop/']
                    }
                },
                {
                    'Name': 'setup pyspark with conda',
                    'ActionOnFailure': 'CANCEL_AND_WAIT',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': ['sudo', 'bash', '/home/hadoop/pyspark_quick_setup.sh', master_dns]
                    }
                }
            ]
        )
        return response


if __name__ == "__main__":
    with open("config.yml", "r") as file:
        config = yaml.load(file)
    config_emr = config.get("emr")

    emr_loader = EMRLoader(
        aws_access_key=config_emr.get("aws_access_key"),
        aws_secret_access_key=config_emr.get("aws_secret_access_key"),
        region_name=config_emr.get("region_name"),
        cluster_name=config_emr.get("cluster_name"),
        instance_count=config_emr.get("instance_count"),
        key_name=config_emr.get("key_name"),
        log_uri=config_emr.get("log_uri"),
        software_version=config_emr.get("software_version"),
        script_uri=config_emr.get("script_uri")
    )

    emr_response = emr_loader.load_cluster()
    emr_client = emr_loader.emr_client()

    while True:
        response = emr_client.describe_cluster(
            ClusterId=emr_response.get("JobFlowId")
        )
        time.sleep(10)
        if response.get("Cluster").get("MasterPublicDnsName") is not None:
            master_dns = response.get("Cluster").get("MasterPublicDnsName")

        if response.get("Cluster").get("Status").get("State") == "WAITING":
            break
        else:
            logger.info(response)

    emr_loader.add_step(emr_response.get("JobFlowId"), master_dns)
