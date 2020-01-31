import boto3
import argparse
import sys

akey = "AKIAIE4762N6YX7POGCQ"
skey = "KQkZSC9qxfk/mDdbKy4a5geZBut6o1NScYvvxYQB"
region = "us-west-2"
machines = {"web":"i-042dced5dc1ee2a7a","windows":"i-05eac0ea47f8b0010","api":"i-0ac996cebfa74576f","windows2":"i-01ef8e94","docker":"i-09288e871b87ed9a8"}

parser = argparse.ArgumentParser(description='Process EC2 commands')
parser.add_argument("--cmd", dest="command", help="enter command start|stop")
parser.add_argument("--instance", dest="machine", help="enter machine name web|windows|api|docker")
results = parser.parse_args()


def init_session(r = None):
    if r is None:
        r = region
    client = boto3.client('ec2',aws_access_key_id=akey,aws_secret_access_key=skey)
    return client

if __name__ == "__main__":
    print("running")
    ec2 = init_session(region)
    wmachine = machines.get(results.machine)
    if wmachine is not None:
        #print(machines[results.machine])
        if results.command == "start":
            try:
                ec2.start_instances(InstanceIds=[machines[results.machine]],DryRun=False)
                print("%s instance started" %(results.machine))
            except ClientError as e:
                print(e.response['Error']['Message'])
                sys.exit(404)
        elif results.command == "stop":
            try:
                ec2.stop_instances(InstanceIds=[machines[results.machine]],DryRun=False)
                print("%s instance stopped" % (results.machine))
            except ClientError as e:
                print(e.response['Error']['Message'])
                sys.exit(404)
        else:
            print("command %s not supported" %(results.command))
            sys.exit(0)
    else:
        print("instance not found")
        sys.exit(0)