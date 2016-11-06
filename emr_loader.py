import boto3
import yaml


class EMRLoader(object):
    def __init__(self, aws_access_key, aws_secret_access_key, region_name, cluster_name, instance_count, key_name,
                 log_uri):
        self.instance_count = instance_count
        self.key_name = key_name
        self.cluster_name = cluster_name
        self.aws_access_key = aws_access_key
        self.aws_secret_access_key = aws_secret_access_key
        self.region_name = region_name
        self.log_uri = log_uri

    def load_cluster(self):
        client = boto3.client("emr",
                              aws_access_key_id=self.aws_access_key,
                              aws_secret_access_key=self.aws_secret_access_key,
                              region_name=self.region_name)

        response = client.run_job_flow(
            Name=self.cluster_name,
            LogUri=self.log_uri,
            ReleaseLabel='emr-5.0.0',
            Instances={
                'MasterInstanceType': 'm3.xlarge',
                'SlaveInstanceType': 'm3.xlarge',
                'InstanceCount': self.instance_count,
                'KeepJobFlowAliveWhenNoSteps': True,
                'TerminationProtected': False,
                'Ec2KeyName': self.key_name,
            },
            Applications=[
                {
                    'Name': 'Spark'
                }
            ],
            VisibleToAllUsers=True,
            JobFlowRole='EMR_EC2_DefaultRole',
            ServiceRole='EMR_DefaultRole'
        )
        print(response)


if __name__ == "__main__":
    with open("config.yml", "r") as file:
        config = yaml.load(file)
    config_emr = config.get("emr")

    EMRLoader(
        aws_access_key=config_emr.get("aws_access_key"),
        aws_secret_access_key=config_emr.get("aws_secret_access_key"),
        region_name=config_emr.get("region_name"),
        cluster_name=config_emr.get("cluster_name"),
        instance_count=config_emr.get("instance_count"),
        key_name=config_emr.get("key_name"),
        log_uri=config_emr.get("log_uri")
    ).load_cluster()
